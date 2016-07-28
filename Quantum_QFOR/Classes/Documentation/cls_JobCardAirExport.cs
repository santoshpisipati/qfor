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
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsJobCardAirExport : CommonFeatures
    {
        #region "Property"

        /// <summary>
        /// The object track n trace
        /// </summary>
        private cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
        /// <summary>
        /// The COM GRP
        /// </summary>
        private int ComGrp;

        /// <summary>
        /// Gets or sets the commodity group.
        /// </summary>
        /// <value>
        /// The commodity group.
        /// </value>
        public int CommodityGroup
        {
            get { return ComGrp; }
            set { ComGrp = value; }
        }

        #endregion "Property"

        #region "GetMainBookingData"

        //Function gets the Booking data for particular BookingPK
        /// <summary>
        /// Gets the main booking data.
        /// </summary>
        /// <param name="bookingPK">The booking pk.</param>
        /// <returns></returns>
        public DataSet GetMainBookingData(string bookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("      DISTINCT bat.booking_air_pk,");
            SQL.Append("      bat.booking_ref_no,");
            SQL.Append("      bat.booking_date,");
            SQL.Append("      cust.customer_id,cust.customer_name, cust.del_address,");
            SQL.Append("      bat.cust_customer_mst_fk,");
            SQL.Append("      col_place.place_name AS \"CollectionPlace\",");
            SQL.Append("      bat.col_place_mst_fk,");
            SQL.Append("      del_place.place_name AS \"DeliveryPlace\",");
            SQL.Append("      bat.del_place_mst_fk,");
            SQL.Append("      POL.port_name as \"POL\",");
            SQL.Append("      bat.port_mst_pol_fk AS \"POLPK\",");
            SQL.Append("      POD.port_name as \"POD\",");
            SQL.Append("      bat.port_mst_pod_fk AS \"PODPK\",");
            SQL.Append("      airline.airline_id,");
            SQL.Append("      airline.airline_name,");
            SQL.Append("      bat.airline_mst_fk,");
            SQL.Append("      bat.flight_no,");
            SQL.Append("      bat.eta_date,");
            SQL.Append("      bat.etd_date,");

            SQL.Append("      (CASE WHEN bat.cust_customer_mst_fk IN ");
            SQL.Append("                           (SELECT");
            SQL.Append("                                   C.CUSTOMER_MST_PK");
            SQL.Append("                            FROM");
            SQL.Append("                                   CUSTOMER_MST_TBL C,");
            SQL.Append("                                   CUSTOMER_CATEGORY_MST_TBL CC,CUSTOMER_CATEGORY_TRN CCT ");
            SQL.Append("                           WHERE");
            SQL.Append("                                   C.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK");
            SQL.Append("                                   AND CC.CUSTOMER_CATEGORY_MST_PK = CCT.CUSTOMER_CATEGORY_MST_FK ");
            SQL.Append("                                   AND UPPER(CC.CUSTOMER_CATEGORY_ID) LIKE 'SHIPPER')");
            SQL.Append("       THEN cust.customer_id ELSE '' END");
            SQL.Append("      ) \"Shipper\",");

            SQL.Append("      (CASE WHEN bat.cust_customer_mst_fk IN ");
            SQL.Append("                           (SELECT");
            SQL.Append("                                   C.CUSTOMER_MST_PK");
            SQL.Append("                            FROM");
            SQL.Append("                                   CUSTOMER_MST_TBL C,");
            SQL.Append("                                   CUSTOMER_CATEGORY_MST_TBL CC,CUSTOMER_CATEGORY_TRN CCT ");
            SQL.Append("                           WHERE");
            SQL.Append("                                   C.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK");
            SQL.Append("                                   AND CC.CUSTOMER_CATEGORY_MST_PK = CCT.CUSTOMER_CATEGORY_MST_FK ");
            SQL.Append("                                   AND UPPER(CC.CUSTOMER_CATEGORY_ID) LIKE 'SHIPPER')");
            SQL.Append("       THEN cust.customer_name ELSE '' END");
            SQL.Append("      ) \"ShipperName\",");

            SQL.Append("      (   CASE WHEN bat.cust_customer_mst_fk IN ");
            SQL.Append("                           (SELECT");
            SQL.Append("                                   C.CUSTOMER_MST_PK");
            SQL.Append("                            FROM");
            SQL.Append("                                   CUSTOMER_MST_TBL C,");
            SQL.Append("                                   CUSTOMER_CATEGORY_MST_TBL CC,CUSTOMER_CATEGORY_TRN CCT ");
            SQL.Append("                            WHERE");
            SQL.Append("                                   C.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK");
            SQL.Append("                                   AND CC.CUSTOMER_CATEGORY_MST_PK = CCT.CUSTOMER_CATEGORY_MST_FK ");
            SQL.Append("                                   AND UPPER(CC.CUSTOMER_CATEGORY_ID) LIKE 'SHIPPER')");
            SQL.Append("      THEN TO_CHAR(cust.customer_mst_pk) ELSE ''END");
            SQL.Append("      ) \"shipper_cust_mst_fk\",");

            SQL.Append("      consignee.customer_id AS \"Consignee\",");
            SQL.Append("      consignee.customer_name AS \"ConsigneeName\",");
            SQL.Append("      bat.cons_customer_mst_fk consignee_cust_mst_fk,");
            SQL.Append("      bat.cb_agent_mst_fk,");
            SQL.Append("      cbagnt.agent_id \"cbAgent\",");
            SQL.Append("      cbagnt.agent_name \"cbAgentName\",");
            SQL.Append("      bat.cl_agent_mst_fk,");
            SQL.Append("      clagnt.agent_id \"clAgent\", ");
            SQL.Append("      clagnt.agent_name \"clAgentName\", ");
            SQL.Append("      bat.pymt_type,");
            SQL.Append("      bat.cargo_move_fk,");
            SQL.Append("      bat.commodity_group_fk,");
            SQL.Append("      comm.commodity_group_desc,");
            SQL.Append("      '' GOODS_DESCRIPTION,");
            SQL.Append("      '' MARKS_NUMBERS");
            SQL.Append("FROM");
            SQL.Append("      booking_air_tbl bat,");
            SQL.Append("      port_mst_tbl POD,");
            SQL.Append("      port_mst_tbl POL,");
            SQL.Append("      customer_mst_tbl cust,");
            SQL.Append("      customer_mst_tbl consignee,");
            SQL.Append("      place_mst_tbl col_place,");
            SQL.Append("      place_mst_tbl del_place,");
            SQL.Append("      airline_mst_tbl airline,");
            SQL.Append("      agent_mst_tbl clagnt,");
            SQL.Append("      agent_mst_tbl cbagnt,");
            SQL.Append("      commodity_group_mst_tbl comm");
            SQL.Append("WHERE");
            SQL.Append("      bat.cust_customer_mst_fk = cust.customer_mst_pk");
            SQL.Append("      AND bat.col_place_mst_fk = col_place.place_pk (+)");
            SQL.Append("      AND bat.del_place_mst_fk = del_place.place_pk (+)");
            SQL.Append("      AND bat.cb_agent_mst_fk = cbagnt.agent_mst_pk(+)");
            SQL.Append("      AND bat.cl_agent_mst_fk = clagnt.agent_mst_pk(+)");
            SQL.Append("      AND bat.port_mst_pol_fk = POL.port_mst_pk");
            SQL.Append("      AND bat.port_mst_pod_fk = POD.port_mst_pk");
            SQL.Append("      AND bat.airline_mst_fk = airline.airline_mst_pk(+)");
            SQL.Append("      AND bat.booking_air_pk =" + bookingPK);
            SQL.Append("      AND bat.cons_customer_mst_fk = consignee.customer_mst_pk(+)");
            SQL.Append("      AND bat.status = 2");
            SQL.Append("      AND bat.commodity_group_fk = comm.commodity_group_pk(+)");

            try
            {
                return objWF.GetDataSet(SQL.ToString());
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

        //gets the container details for particular booking PK.
        /// <summary>
        /// Gets the booking container details.
        /// </summary>
        /// <param name="bookingPK">The booking pk.</param>
        /// <returns></returns>
        public DataSet GetBookingContainerDetails(string bookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("      DISTINCT");
            SQL.Append("      '' pk,");
            SQL.Append("      '' palette_size,");
            SQL.Append("      bat.volume_in_cbm,");
            SQL.Append("      bat.gross_weight,");
            SQL.Append("      bat.chargeable_weight,");
            SQL.Append("      bat.pack_type_mst_fk,");
            SQL.Append("      bat.pack_count,");
            SQL.Append("      btrn.commodity_mst_fk,");
            SQL.Append("      '' LOAD_DATE");
            SQL.Append("FROM");
            SQL.Append("      booking_air_tbl bat,");
            SQL.Append("      booking_trn_air btrn");
            SQL.Append("WHERE");
            SQL.Append("      bat.booking_air_pk = btrn.booking_air_fk");
            SQL.Append("      AND bat.booking_air_pk =" + bookingPK);
            SQL.Append("      AND bat.status = 2");

            try
            {
                return objWF.GetDataSet(SQL.ToString());
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

        //gets the Freight details for particular booking PK.
        /// <summary>
        /// Gets the booking freight details.
        /// </summary>
        /// <param name="bookingPK">The booking pk.</param>
        /// <param name="baseCurrency">The base currency.</param>
        /// <returns></returns>
        public DataSet GetBookingFreightDetails(string bookingPK, Int64 baseCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("     '0' job_trn_air_exp_fd_pk,");
            SQL.Append("     frt.freight_element_id,");
            SQL.Append("     frt.freight_element_name,");
            SQL.Append("     frt.freight_element_mst_pk,");
            SQL.Append("     DECODE(btrn.basis,0,' ',1,'%',2,'Flat rate',3,'Kgs',4,'Unit') basis,");
            SQL.Append("     btrn.quantity,");
            SQL.Append("     DECODE(bfrt.pymt_type,1,'Prepaid',2,'Collect') freight_type,");
            SQL.Append("     nvl(bfrt.tariff_rate,1)*nvl(btrn.quantity,1) freight_amt,");
            SQL.Append("     bfrt.currency_mst_fk,");
            SQL.Append("     ROUND(GET_EX_RATE(bfrt.currency_mst_fk," + baseCurrency + ",SYSDATE),4) AS ROE ,");
            SQL.Append("     'false' \"Delete\", 1 \"Print\"");
            SQL.Append("FROM");
            SQL.Append("     booking_air_tbl bat,");
            SQL.Append("     booking_trn_air btrn,");
            SQL.Append("     booking_trn_air_frt_dtls bfrt,");
            SQL.Append("     freight_element_mst_tbl frt,");
            SQL.Append("     currency_type_mst_tbl curr");
            SQL.Append("WHERE");
            SQL.Append("     bfrt.freight_element_mst_fk = frt.freight_element_mst_pk");
            SQL.Append("     AND bfrt.booking_trn_air_fk = btrn.booking_trn_air_pk");
            SQL.Append("     AND btrn.booking_air_fk = bat.booking_air_pk");
            SQL.Append("     AND bat.booking_air_pk =" + bookingPK);
            SQL.Append("     AND bfrt.currency_mst_fk = curr.currency_mst_pk");
            SQL.Append("     AND bat.status = 2");
            SQL.Append("     ORDER BY frt.freight_element_id");

            try
            {
                return objWF.GetDataSet(SQL.ToString());
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

        #endregion "GetMainBookingData"

        #region "GetBaseCurrency"

        /// <summary>
        /// Gets the base currency.
        /// </summary>
        /// <returns></returns>
        public Int64 GetBaseCurrency()
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append(" SELECT c.currency_mst_fk FROM corporate_mst_tbl c ");

            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(SQL.ToString()));
            }
            catch (OracleException sqlExp)
            {
                return 0;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "GetBaseCurrency"

        #region "Save Function"

        /// <summary>
        /// Saves the specified m_ data set.
        /// </summary>
        /// <param name="M_DataSet">The m_ data set.</param>
        /// <param name="dsContainerData">The ds container data.</param>
        /// <param name="dsTPDetails">The ds tp details.</param>
        /// <param name="dsFreightDetails">The ds freight details.</param>
        /// <param name="dsPurchaseInventory">The ds purchase inventory.</param>
        /// <param name="dsCostDetails">The ds cost details.</param>
        /// <param name="dsPickUpDetails">The ds pick up details.</param>
        /// <param name="dsDropDetails">The ds drop details.</param>
        /// <param name="isEdting">if set to <c>true</c> [is edting].</param>
        /// <param name="ucrNo">The ucr no.</param>
        /// <param name="jobCardRefNumber">The job card reference number.</param>
        /// <param name="userLocation">The user location.</param>
        /// <param name="employeeID">The employee identifier.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsOtherCharges">The ds other charges.</param>
        /// <param name="strBookingRefNo">The string booking reference no.</param>
        /// <param name="strAirlinePK">The string airline pk.</param>
        /// <param name="intIsAirineUpdate">The int is airine update.</param>
        /// <param name="dsMainBookingDetails">The ds main booking details.</param>
        /// <param name="strAirMstPk">The string air MST pk.</param>
        /// <param name="AirwayBillNo">The airway bill no.</param>
        /// <param name="ColPk">The col pk.</param>
        /// <param name="DelPk">The delete pk.</param>
        /// <param name="AddVATOSFLAG">The add vatosflag.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <param name="dsDoc">The ds document.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet, DataSet dsContainerData, DataSet dsTPDetails, DataSet dsFreightDetails, DataSet dsPurchaseInventory, DataSet dsCostDetails, DataSet dsPickUpDetails, DataSet dsDropDetails, bool isEdting, object ucrNo,
        string jobCardRefNumber, string userLocation, string employeeID, long JobCardPK, DataSet dsOtherCharges, string strBookingRefNo, string strAirlinePK, Int16 intIsAirineUpdate, DataSet dsMainBookingDetails, string strAirMstPk,
        string AirwayBillNo, string ColPk = "", string DelPk = "", int AddVATOSFLAG = 0, DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null, DataSet dsDoc = null)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            int intPKVal = 0;
            long intBookingAirPk = Convert.ToInt64(dsMainBookingDetails.Tables[0].Rows[0]["BOOKING_AIR_FK"]);
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            OracleCommand insContainerDetails = new OracleCommand();
            OracleCommand updContainerDetails = new OracleCommand();
            OracleCommand delContainerDetails = new OracleCommand();

            OracleCommand insTPDetails = new OracleCommand();
            OracleCommand updTPDetails = new OracleCommand();
            OracleCommand delTPDetails = new OracleCommand();

            OracleCommand insPickUpDetails = new OracleCommand();
            OracleCommand updPickUpDetails = new OracleCommand();

            OracleCommand insDropDetails = new OracleCommand();
            OracleCommand updDropDetails = new OracleCommand();

            OracleCommand insFreightDetails = new OracleCommand();
            OracleCommand updFreightDetails = new OracleCommand();
            OracleCommand delFreightDetails = new OracleCommand();

            //Dim insPurchaseInvDetails As New OracleClient.OracleCommand
            //Dim updPurchaseInvDetails As New OracleClient.OracleCommand
            //Dim delPurchaseInvDetails As New OracleClient.OracleCommand

            OracleCommand insCostDetails = new OracleCommand();
            //'Added By Koteshwari
            OracleCommand updCostDetails = new OracleCommand();
            OracleCommand delCostDetails = new OracleCommand();

            OracleCommand insIncomeChargeDetails = new OracleCommand();
            OracleCommand updIncomeChargeDetails = new OracleCommand();
            OracleCommand delIncomeChargeDetails = new OracleCommand();

            OracleCommand insExpenseChargeDetails = new OracleCommand();
            OracleCommand updExpenseChargeDetails = new OracleCommand();
            OracleCommand delExpenseChargeDetails = new OracleCommand();

            OracleCommand insOtherChargesDetails = new OracleCommand();
            OracleCommand updOtherChargesDetails = new OracleCommand();
            OracleCommand delOtherChargesDetails = new OracleCommand();
            DataSet dsTrackNTrace = new DataSet();
            //If isEdting = False Then
            //    jobCardRefNumber = GenerateProtocolKey("JOB CARD EXP (AIR)", userLocation, employeeID, DateTime.Now, , , , M_LAST_MODIFIED_BY_FK)
            //End If

            ucrNo = ucrNo + jobCardRefNumber;
            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                dsTrackNTrace = dsContainerData.Copy();

                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_CARD_AIR_EXP_TBL_INS";
                var _with2 = _with1.Parameters;

                insCommand.Parameters.Add("BOOKING_AIR_FK_IN", OracleDbType.Int32, 10, "booking_air_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["BOOKING_AIR_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JOBCARD_REF_NO_IN", jobCardRefNumber).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("UCR_NO_IN", ucrNo).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("JOBCARD_DATE_IN", OracleDbType.Date, 20, "jobcard_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOBCARD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JOB_CARD_STATUS_IN", OracleDbType.Int32, 1, "job_card_status").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOB_CARD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JOB_CARD_CLOSED_ON_IN", OracleDbType.Date, 20, "job_card_closed_on").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOB_CARD_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_XML_STATUS_IN", OracleDbType.Int32, 1, "WIN_XML_STATUS").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_XML_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 200, "remarks").Direction = ParameterDirection.Input;
                insCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2, 25, "flight_no").Direction = ParameterDirection.Input;
                insCommand.Parameters["FLIGHT_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("AIRLINE_SCHEDULE_TRN_FK_IN", OracleDbType.Int32, 10, "AIRLINE_SCHEDULE_TRN_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["AIRLINE_SCHEDULE_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ARRIVAL_DATE_IN", OracleDbType.Date, 20, "arrival_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("DEPARTURE_DATE_IN", OracleDbType.Date, 20, "departure_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["DEPARTURE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SHIPPER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "shipper_cust_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["SHIPPER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CONSIGNEE_CUST_MST_FK_IN", OracleDbType.Int32, 10, "consignee_cust_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CONSIGNEE_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("NOTIFY1_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify1_cust_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["NOTIFY1_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("NOTIFY2_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify2_cust_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["NOTIFY2_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CB_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cb_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CB_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("DP_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "dp_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["DP_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CL_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cl_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CARGO_MOVE_FK_IN", OracleDbType.Int32, 10, "cargo_move_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PYMT_TYPE_IN", OracleDbType.Int32, 1, "pymt_type").Direction = ParameterDirection.Input;
                insCommand.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", OracleDbType.Int32, 10, "shipping_terms_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["SHIPPING_TERMS_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("INSURANCE_AMT_IN", OracleDbType.Int32, 10, "insurance_amt").Direction = ParameterDirection.Input;
                insCommand.Parameters["INSURANCE_AMT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("INSURANCE_CURRENCY_IN", OracleDbType.Int32, 10, "insurance_currency").Direction = ParameterDirection.Input;
                insCommand.Parameters["INSURANCE_CURRENCY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "commodity_group_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TRANSPORTER_DEPOT_FK_IN", OracleDbType.Int32, 10, "depot_pk").Direction = ParameterDirection.Input;
                insCommand.Parameters["TRANSPORTER_DEPOT_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TRANSPORTER_CARRIER_FK_IN", OracleDbType.Int32, 10, "carrier_pk").Direction = ParameterDirection.Input;
                insCommand.Parameters["TRANSPORTER_CARRIER_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("COUNTRY_ORIGIN_FK_IN", OracleDbType.Int32, 10, "country_mst_pk").Direction = ParameterDirection.Input;
                insCommand.Parameters["COUNTRY_ORIGIN_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("DA_NUMBER_IN", OracleDbType.Int32, 10, "da_number").Direction = ParameterDirection.Input;
                insCommand.Parameters["DA_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200, "del_address").Direction = ParameterDirection.Input;
                insCommand.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_PK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

                //Added By Rijesh To Incorporate Cargo Details On March -30 2006
                //*******************
                insCommand.Parameters.Add("GOODS_DESCRIPTION_IN", OracleDbType.Varchar2, 4000, "GOODS_DESCRIPTION").Direction = ParameterDirection.Input;
                insCommand.Parameters["GOODS_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MARKS_NUMBERS_IN", OracleDbType.Varchar2, 2000, "MARKS_NUMBERS").Direction = ParameterDirection.Input;
                insCommand.Parameters["MARKS_NUMBERS_IN"].SourceVersion = DataRowVersion.Current;
                //***********************

                insCommand.Parameters.Add("MASTER_JC_AIR_EXP_FK_IN", OracleDbType.Int32, 10, "MASTER_JC_AIR_EXP_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["MASTER_JC_AIR_EXP_FK_IN"].SourceVersion = DataRowVersion.Current;

                //Code Added By Anil on 18 Aug 09
                insCommand.Parameters.Add("SB_DATE_IN", OracleDbType.Date, 20, "sb_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["SB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SB_NO_IN", OracleDbType.Varchar2, 20, "sb_number").Direction = ParameterDirection.Input;
                insCommand.Parameters["SB_NO_IN"].SourceVersion = DataRowVersion.Current;
                //End By Anil

                insCommand.Parameters.Add("LC_SHIPMENT_IN", OracleDbType.Int32, 1, "LC_SHIPMENT").Direction = ParameterDirection.Input;
                insCommand.Parameters["LC_SHIPMENT_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SHIPPING_INST_DT"].ToString()))
                {
                    insCommand.Parameters.Add("SI_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("SI_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["SHIPPING_INST_DT"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["SI_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["RFS_Date"].ToString()))
                {
                    insCommand.Parameters.Add("RFS_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("RFS_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["RFS_Date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["RFS_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CRQ_Date"].ToString()))
                {
                    insCommand.Parameters.Add("CRQ_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("CRQ_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["CRQ_Date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["CRQ_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SI_IN", OracleDbType.Int32, 1, "SHIPPING_INST_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RFS_IN", OracleDbType.Int32, 1, "RFS").Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CRQ_IN", OracleDbType.Int32, 1, "CRQ").Direction = ParameterDirection.Input;

                //nomination parameters
                insCommand.Parameters.Add("CHK_NOMINATED_IN", OracleDbType.Int32, 1, "CHK_NOMINATED").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_NOMINATED_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CHK_CSR_IN", OracleDbType.Int32, 1, "CHK_CSR").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_CSR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("EXECUTIVE_MST_FK_IN", OracleDbType.Int32, 10, "SALES_EXEC_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["EXECUTIVE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //-----------------------------------------------------------------------------------
                insCommand.Parameters.Add("CC_REQ_IN", OracleDbType.Int32, 1, "cc_req").Direction = ParameterDirection.Input;
                insCommand.Parameters["CC_REQ_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CC_IE_IN", OracleDbType.Int32, 1, "cc_ie").Direction = ParameterDirection.Input;
                insCommand.Parameters["CC_IE_IN"].SourceVersion = DataRowVersion.Current;

                //Added by Raghavendra
                insCommand.Parameters.Add("PRC_FK_IN", OracleDbType.Int32, 1, "PRC_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["PRC_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ONC_FK_IN", OracleDbType.Int32, 1, "ONC_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["ONC_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PRC_MODE_FK_IN", OracleDbType.Int32, 1, "PRC_MODE_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["PRC_MODE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ONC_MODE_FK_IN", OracleDbType.Int32, 1, " ONC_MODE_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["ONC_MODE_FK_IN"].SourceVersion = DataRowVersion.Current;
                //End
                //Manjunath for Total, Received and Balance Quantity and Weight
                insCommand.Parameters.Add("WIN_TOTAL_QTY_IN", OracleDbType.Int32, 10, "WIN_TOTAL_QTY").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_TOTAL_QTY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_REC_QTY_IN", OracleDbType.Int32, 10, "WIN_REC_QTY").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_REC_QTY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_BALANCE_QTY_IN", OracleDbType.Int32, 10, "WIN_BALANCE_QTY").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_BALANCE_QTY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_TOTAL_WT_IN", OracleDbType.Int32, 10, "WIN_TOTAL_WT").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_TOTAL_WT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_REC_WT_IN", OracleDbType.Int32, 10, "WIN_REC_WT").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_REC_WT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_BALANCE_WT_IN", OracleDbType.Int32, 10, "WIN_BALANCE_WT").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_BALANCE_WT_IN"].SourceVersion = DataRowVersion.Current;
                //End
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "job_card_air_exp_pk").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_CARD_AIR_EXP_TBL_UPD";
                var _with4 = _with3.Parameters;

                updCommand.Parameters.Add("JOB_CARD_AIR_EXP_PK_IN", OracleDbType.Int32, 10, "job_card_air_exp_pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_AIR_EXP_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("UCR_NO_IN", OracleDbType.Varchar2, 40, "ucr_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["UCR_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BOOKING_AIR_FK_IN", OracleDbType.Int32, 10, "booking_air_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["BOOKING_AIR_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOBCARD_REF_NO_IN", jobCardRefNumber).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("JOBCARD_DATE_IN", OracleDbType.Date, 20, "jobcard_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOBCARD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOB_CARD_STATUS_IN", OracleDbType.Int32, 1, "job_card_status").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOB_CARD_CLOSED_ON_IN", OracleDbType.Date, 20, "job_card_closed_on").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_XML_STATUS_IN", OracleDbType.Int32, 1, "WIN_XML_STATUS").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_XML_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 200, "remarks").Direction = ParameterDirection.Input;
                updCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2, 25, "flight_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["FLIGHT_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AIRLINE_SCHEDULE_TRN_FK_IN", OracleDbType.Int32, 10, "AIRLINE_SCHEDULE_TRN_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["AIRLINE_SCHEDULE_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ARRIVAL_DATE_IN", OracleDbType.Date, 20, "arrival_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DEPARTURE_DATE_IN", OracleDbType.Date, 20, "departure_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEPARTURE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SHIPPER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "shipper_cust_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["SHIPPER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CONSIGNEE_CUST_MST_FK_IN", OracleDbType.Int32, 10, "consignee_cust_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CONSIGNEE_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("NOTIFY1_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify1_cust_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["NOTIFY1_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("NOTIFY2_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify2_cust_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["NOTIFY2_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CB_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cb_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CB_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DP_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "dp_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["DP_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CL_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cl_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "version_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CARGO_MOVE_FK_IN", OracleDbType.Int32, 10, "cargo_move_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PYMT_TYPE_IN", OracleDbType.Int32, 1, "pymt_type").Direction = ParameterDirection.Input;
                updCommand.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", OracleDbType.Int32, 10, "shipping_terms_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["SHIPPING_TERMS_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("INSURANCE_AMT_IN", OracleDbType.Int32, 10, "insurance_amt").Direction = ParameterDirection.Input;
                updCommand.Parameters["INSURANCE_AMT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("INSURANCE_CURRENCY_IN", OracleDbType.Int32, 10, "insurance_currency").Direction = ParameterDirection.Input;
                updCommand.Parameters["INSURANCE_CURRENCY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "commodity_group_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TRANSPORTER_DEPOT_FK_IN", OracleDbType.Int32, 10, "depot_pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["TRANSPORTER_DEPOT_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TRANSPORTER_CARRIER_FK_IN", OracleDbType.Int32, 10, "carrier_pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["TRANSPORTER_CARRIER_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COUNTRY_ORIGIN_FK_IN", OracleDbType.Int32, 10, "country_mst_pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["COUNTRY_ORIGIN_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DA_NUMBER_IN", OracleDbType.Int32, 10, "da_number").Direction = ParameterDirection.Input;
                updCommand.Parameters["DA_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200, "del_address").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;
                //Added By Rijesh To Incorporate Cargo Details On March -30 2006
                //*******************
                updCommand.Parameters.Add("GOODS_DESCRIPTION_IN", OracleDbType.Varchar2, 4000, "GOODS_DESCRIPTION").Direction = ParameterDirection.Input;
                updCommand.Parameters["GOODS_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MARKS_NUMBERS_IN", OracleDbType.Varchar2, 2000, "MARKS_NUMBERS").Direction = ParameterDirection.Input;
                updCommand.Parameters["MARKS_NUMBERS_IN"].SourceVersion = DataRowVersion.Current;
                //***********************

                //Code Added By Anil on 17 Aug 09
                updCommand.Parameters.Add("SB_DATE_IN", OracleDbType.Date, 20, "sb_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["SB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SB_NO_IN", OracleDbType.Varchar2, 20, "sb_number").Direction = ParameterDirection.Input;
                updCommand.Parameters["SB_NO_IN"].SourceVersion = DataRowVersion.Current;
                //End By Anil

                updCommand.Parameters.Add("MASTER_JC_AIR_EXP_FK_IN", OracleDbType.Int32, 10, "MASTER_JC_AIR_EXP_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["MASTER_JC_AIR_EXP_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ADDVATOS_FLAG_IN", AddVATOSFLAG).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("LC_SHIPMENT_IN", OracleDbType.Int32, 1, "LC_SHIPMENT").Direction = ParameterDirection.Input;
                updCommand.Parameters["LC_SHIPMENT_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SHIPPING_INST_DT"].ToString()))
                {
                    updCommand.Parameters.Add("SI_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("SI_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["SHIPPING_INST_DT"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["SI_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["RFS_Date"].ToString()))
                {
                    updCommand.Parameters.Add("RFS_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("RFS_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["RFS_Date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["RFS_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CRQ_Date"].ToString()))
                {
                    updCommand.Parameters.Add("CRQ_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("CRQ_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["CRQ_Date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["CRQ_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SI_IN", OracleDbType.Int32, 1, "SHIPPING_INST_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RFS_IN", OracleDbType.Int32, 1, "RFS").Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CRQ_IN", OracleDbType.Int32, 1, "CRQ").Direction = ParameterDirection.Input;

                //nomination parameters
                updCommand.Parameters.Add("CHK_NOMINATED_IN", OracleDbType.Int32, 1, "CHK_NOMINATED").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHK_NOMINATED_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CHK_CSR_IN", OracleDbType.Int32, 1, "CHK_CSR").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHK_CSR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("EXECUTIVE_MST_FK_IN", OracleDbType.Int32, 10, "SALES_EXEC_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["EXECUTIVE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //------------------------------------------------------------------------------------
                updCommand.Parameters.Add("CC_REQ_IN", OracleDbType.Int32, 1, "cc_req").Direction = ParameterDirection.Input;
                updCommand.Parameters["CC_REQ_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CC_IE_IN", OracleDbType.Int32, 1, "cc_ie").Direction = ParameterDirection.Input;
                updCommand.Parameters["CC_IE_IN"].SourceVersion = DataRowVersion.Current;

                //Added by Raghavendra

                updCommand.Parameters.Add("PRC_FK_IN", OracleDbType.Int32, 1, "PRC_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["PRC_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ONC_FK_IN", OracleDbType.Int32, 1, "ONC_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["ONC_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PRC_MODE_FK_IN", OracleDbType.Int32, 1, "PRC_MODE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["PRC_MODE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ONC_MODE_FK_IN", OracleDbType.Int32, 1, "ONC_MODE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["ONC_MODE_FK_IN"].SourceVersion = DataRowVersion.Current;

                //End
                //Manjunath for Total, Received and Balance Quantity and Weight
                updCommand.Parameters.Add("WIN_TOTAL_QTY_IN", OracleDbType.Int32, 10, "WIN_TOTAL_QTY").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_TOTAL_QTY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_REC_QTY_IN", OracleDbType.Int32, 10, "WIN_REC_QTY").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_REC_QTY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_BALANCE_QTY_IN", OracleDbType.Int32, 10, "WIN_BALANCE_QTY").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_BALANCE_QTY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_TOTAL_WT_IN", OracleDbType.Int32, 10, "WIN_TOTAL_WT").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_TOTAL_WT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_REC_WT_IN", OracleDbType.Int32, 10, "WIN_REC_WT").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_REC_WT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_BALANCE_WT_IN", OracleDbType.Int32, 10, "WIN_BALANCE_WT").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_BALANCE_WT_IN"].SourceVersion = DataRowVersion.Current;
                //End
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with5 = objWK.MyDataAdapter;

                _with5.InsertCommand = insCommand;
                _with5.InsertCommand.Transaction = TRAN;

                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;

                //ADD BY LATHA FOR SAVING PLR AND PLD
                //UpdatePlaces(intBookingAirPk, ColPk, DelPk) 'commented and modified by surya prasad for transaction management
                UpdatePlaces(intBookingAirPk, ColPk, DelPk, TRAN);

                RecAfct = _with5.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    if (isEdting == false)
                    {
                        JobCardPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    }
                }

                var _with6 = insContainerDetails;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_CONT_INS";
                var _with7 = _with6.Parameters;

                insContainerDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insContainerDetails.Parameters.Add("PALETTE_SIZE_IN", OracleDbType.Varchar2, 20, "palette_size").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PALETTE_SIZE_IN"].SourceVersion = DataRowVersion.Current;

                //' Amit 20-June-07
                insContainerDetails.Parameters.Add("AIRFREIGHT_SLABS_TBL_FK_IN", OracleDbType.Int32, 10, "airfreight_slabs_tbl_fk").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["AIRFREIGHT_SLABS_TBL_FK_IN"].SourceVersion = DataRowVersion.Current;

                // Amit 20-June-07
                insContainerDetails.Parameters.Add("ULD_NUMBER_IN", OracleDbType.Varchar2, 20, "uld_number").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["ULD_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "volume_in_cbm").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "chargeable_weight").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "pack_type_mst_fk").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "pack_count").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Varchar2, 50, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "load_date").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_EXP_CONT_PK").Direction = ParameterDirection.Output;
                insContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with8 = updContainerDetails;
                _with8.Connection = objWK.MyConnection;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_CONT_UPD";
                var _with9 = _with8.Parameters;

                updContainerDetails.Parameters.Add("JOB_TRN_AIR_EXP_CONT_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_cont_pk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["JOB_TRN_AIR_EXP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updContainerDetails.Parameters.Add("PALETTE_SIZE_IN", OracleDbType.Varchar2, 20, "palette_size").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PALETTE_SIZE_IN"].SourceVersion = DataRowVersion.Current;

                //' Amit 20-June-07
                updContainerDetails.Parameters.Add("AIRFREIGHT_SLABS_TBL_FK_IN", OracleDbType.Int32, 10, "airfreight_slabs_tbl_fk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["AIRFREIGHT_SLABS_TBL_FK_IN"].SourceVersion = DataRowVersion.Current;

                // Amit 20-June-07
                updContainerDetails.Parameters.Add("ULD_NUMBER_IN", OracleDbType.Varchar2, 20, "uld_number").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["ULD_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "volume_in_cbm").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "chargeable_weight").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "pack_type_mst_fk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "pack_count").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Varchar2, 50, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "load_date").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with10 = delContainerDetails;
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_CONT_DEL";

                delContainerDetails.Parameters.Add("JOB_TRN_AIR_EXP_CONT_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_cont_pk").Direction = ParameterDirection.Input;
                delContainerDetails.Parameters["JOB_TRN_AIR_EXP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                delContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with11 = objWK.MyDataAdapter;

                _with11.InsertCommand = insContainerDetails;
                _with11.InsertCommand.Transaction = TRAN;

                _with11.UpdateCommand = updContainerDetails;
                _with11.UpdateCommand.Transaction = TRAN;

                _with11.DeleteCommand = delContainerDetails;
                _with11.DeleteCommand.Transaction = TRAN;

                RecAfct = _with11.Update(dsContainerData.Tables[0]);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                objWK.MyCommand.Transaction = TRAN;
                int rowCnt = 0;
                if (dsContainerData.Tables[0].Rows.Count > 0)
                {
                    try
                    {
                        for (rowCnt = 0; rowCnt <= dsContainerData.Tables[0].Rows.Count - 1; rowCnt++)
                        {
                            var _with12 = objWK.MyCommand;
                            _with12.Parameters.Clear();
                            _with12.CommandType = CommandType.Text;
                            objWK.MyCommand.Parameters.Clear();

                            if (CommodityGroup == HAZARDOUS)
                            {
                                arrMessage = SaveTransactionHZSpcl(objWK.MyCommand, objWK.MyUserName, getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "").ToString(), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
                            }
                            else if (CommodityGroup == REEFER)
                            {
                                arrMessage = SaveTransactionReefer(objWK.MyCommand, objWK.MyUserName, getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "").ToString(), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                }

                //Manjunath for Cargo Pick up & Drop Address
                if ((dsPickUpDetails != null))
                {
                    var _with13 = insPickUpDetails;
                    _with13.Connection = objWK.MyConnection;
                    _with13.CommandType = CommandType.StoredProcedure;
                    _with13.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_INS";

                    _with13.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with13.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with13.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with13.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with13.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with13.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with13.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with13.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with13.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with13.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with13.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with13.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with13.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with13.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with13.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with13.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with13.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with13.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with13.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with13.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with13.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Output;
                    _with13.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with14 = updPickUpDetails;
                    _with14.Connection = objWK.MyConnection;
                    _with14.CommandType = CommandType.StoredProcedure;
                    _with14.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_UPD";

                    _with14.Parameters.Add("PICK_DROP_MST_PK_IN", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Input;
                    _with14.Parameters["PICK_DROP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with14.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with14.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with14.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with14.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with14.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with14.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with14.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with14.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with14.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with14.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with14.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with14.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with14.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with14.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with14.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with14.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with14.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with14.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with14.Parameters.Add("MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with14.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with14.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with15 = objWK.MyDataAdapter;

                    _with15.InsertCommand = insPickUpDetails;
                    _with15.InsertCommand.Transaction = TRAN;

                    _with15.UpdateCommand = updPickUpDetails;
                    _with15.UpdateCommand.Transaction = TRAN;

                    RecAfct = _with15.Update(dsPickUpDetails);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "Saved") > 0)
                        {
                            arrMessage.Clear();
                        }
                    }
                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }

                if ((dsDropDetails != null))
                {
                    var _with16 = insDropDetails;
                    _with16.Connection = objWK.MyConnection;
                    _with16.CommandType = CommandType.StoredProcedure;
                    _with16.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_INS";

                    _with16.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with16.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with16.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with16.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with16.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with16.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with16.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with16.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with16.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with16.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with16.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with16.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with16.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with16.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with16.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with16.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with16.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with16.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with16.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with16.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with16.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Output;
                    _with16.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with17 = updDropDetails;
                    _with17.Connection = objWK.MyConnection;
                    _with17.CommandType = CommandType.StoredProcedure;
                    _with17.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_UPD";

                    _with17.Parameters.Add("PICK_DROP_MST_PK_IN", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Input;
                    _with17.Parameters["PICK_DROP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with17.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with17.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with17.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with17.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with17.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with17.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with17.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with17.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with17.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with17.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with17.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with17.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with17.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with17.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with17.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with17.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with17.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with17.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with17.Parameters.Add("MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with17.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with17.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with18 = objWK.MyDataAdapter;

                    _with18.InsertCommand = insDropDetails;
                    _with18.InsertCommand.Transaction = TRAN;

                    _with18.UpdateCommand = updDropDetails;
                    _with18.UpdateCommand.Transaction = TRAN;

                    RecAfct = _with18.Update(dsDropDetails);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "Saved") > 0)
                        {
                            arrMessage.Clear();
                        }
                    }
                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                //End Manjunath

                var _with19 = insTPDetails;
                _with19.Connection = objWK.MyConnection;
                _with19.CommandType = CommandType.StoredProcedure;
                _with19.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_TP_INS";
                var _with20 = _with19.Parameters;

                insTPDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insTPDetails.Parameters.Add("PORT_MST_FK_IN", OracleDbType.Int32, 10, "port_mst_fk").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["PORT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("TRANSHIPMENT_NO_IN", OracleDbType.Int32, 10, "transhipment_no").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["TRANSHIPMENT_NO_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2, 25, "flight_no").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["FLIGHT_NO_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("AIRLINE_MST_FK_IN", OracleDbType.Int32, 10, "airline_mst_fk").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["AIRLINE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_EXP_TP_PK").Direction = ParameterDirection.Output;
                insTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with21 = updTPDetails;
                _with21.Connection = objWK.MyConnection;
                _with21.CommandType = CommandType.StoredProcedure;
                _with21.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_TP_UPD";
                var _with22 = _with21.Parameters;

                updTPDetails.Parameters.Add("JOB_TRN_AIR_EXP_TP_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_tp_pk").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["JOB_TRN_AIR_EXP_TP_PK_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updTPDetails.Parameters.Add("PORT_MST_FK_IN", OracleDbType.Int32, 10, "port_mst_fk").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["PORT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("TRANSHIPMENT_NO_IN", OracleDbType.Int32, 10, "transhipment_no").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["TRANSHIPMENT_NO_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2, 25, "flight_no").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["FLIGHT_NO_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("AIRLINE_MST_FK_IN", OracleDbType.Int32, 10, "airline_mst_fk").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["AIRLINE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with23 = delTPDetails;
                _with23.Connection = objWK.MyConnection;
                _with23.CommandType = CommandType.StoredProcedure;
                _with23.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_TP_DEL";

                delTPDetails.Parameters.Add("JOB_TRN_AIR_EXP_TP_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_tp_pk").Direction = ParameterDirection.Input;
                delTPDetails.Parameters["JOB_TRN_AIR_EXP_TP_PK_IN"].SourceVersion = DataRowVersion.Current;

                delTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with24 = objWK.MyDataAdapter;

                _with24.InsertCommand = insTPDetails;
                _with24.InsertCommand.Transaction = TRAN;

                _with24.UpdateCommand = updTPDetails;
                _with24.UpdateCommand.Transaction = TRAN;

                _with24.DeleteCommand = delTPDetails;
                _with24.DeleteCommand.Transaction = TRAN;
                RecAfct = _with24.Update(dsTPDetails.Tables[0]);

                if (arrMessage.Count > 0)
                {
                    if (string.Compare(arrMessage[0].ToString(), "Saved") > 0)
                    {
                        arrMessage.Clear();
                    }
                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }

                var _with25 = insFreightDetails;
                _with25.Connection = objWK.MyConnection;
                _with25.CommandType = CommandType.StoredProcedure;
                _with25.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_FD_INS";
                var _with26 = _with25.Parameters;

                insFreightDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                // By Amit Singh on 10-May-07
                insFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                //insFreightDetails.Parameters.Add("LOCATION_ID_IN", OracleClient.OracleDbType.Int32, 20, "location_id").Direction = ParameterDirection.Input
                //insFreightDetails.Parameters["LOCATION_ID_IN"].SourceVersion = DataRowVersion.Current

                insFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                //insFreightDetails.Parameters.Add("CUSTOMER_ID_IN", OracleClient.OracleDbType.Int32, 11, "customer_id").Direction = ParameterDirection.Input
                //insFreightDetails.Parameters["CUSTOMER_ID_IN"].SourceVersion = DataRowVersion.Current
                // End

                //'Rateperbasis
                insFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "Rateperbasis").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                // Added Suresh Kumar 30.03.2006 - Print Check box for MAWB Print
                insFreightDetails.Parameters.Add("PRINT_ON_MAWB_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["PRINT_ON_MAWB_IN"].SourceVersion = DataRowVersion.Current;
                //end

                insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_EXP_FD_PK").Direction = ParameterDirection.Output;
                insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with27 = updFreightDetails;
                _with27.Connection = objWK.MyConnection;
                _with27.CommandType = CommandType.StoredProcedure;
                _with27.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_FD_UPD";
                var _with28 = _with27.Parameters;

                updFreightDetails.Parameters.Add("JOB_TRN_AIR_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_fd_pk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["JOB_TRN_AIR_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                // By Amit Singh on 10-May-07
                updFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                //updFreightDetails.Parameters.Add("LOCATION_ID_IN", OracleClient.OracleDbType.Int32, 20, "location_id").Direction = ParameterDirection.Input
                //updFreightDetails.Parameters["LOCATION_ID_IN"].SourceVersion = DataRowVersion.Current

                updFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                //updFreightDetails.Parameters.Add("CUSTOMER_ID_IN", OracleClient.OracleDbType.Int32, 11, "customer_id").Direction = ParameterDirection.Input
                //updFreightDetails.Parameters["CUSTOMER_ID_IN"].SourceVersion = DataRowVersion.Current
                // End

                updFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                //'Rateperbasis
                updFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "Rateperbasis").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                //' Added Suresh Kumar 30.03.2006 - Print Check box for MAWB Print
                updFreightDetails.Parameters.Add("PRINT_ON_MAWB_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["PRINT_ON_MAWB_IN"].SourceVersion = DataRowVersion.Current;
                //end

                updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with29 = delFreightDetails;
                _with29.Connection = objWK.MyConnection;
                _with29.CommandType = CommandType.StoredProcedure;
                _with29.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_FD_DEL";

                delFreightDetails.Parameters.Add("JOB_TRN_AIR_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_fd_pk").Direction = ParameterDirection.Input;
                delFreightDetails.Parameters["JOB_TRN_AIR_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                delFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with30 = objWK.MyDataAdapter;

                _with30.InsertCommand = insFreightDetails;
                _with30.InsertCommand.Transaction = TRAN;

                _with30.UpdateCommand = updFreightDetails;
                _with30.UpdateCommand.Transaction = TRAN;

                _with30.DeleteCommand = delFreightDetails;
                _with30.DeleteCommand.Transaction = TRAN;

                RecAfct = _with30.Update(dsFreightDetails);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }

                if (!SaveSecondaryServices(objWK, TRAN, Convert.ToInt32(JobCardPK), dsIncomeChargeDetails, dsExpenseChargeDetails))
                {
                    arrMessage.Add("Error while saving secondary service details");
                    return arrMessage;
                }

                var _with31 = insCostDetails;
                _with31.Connection = objWK.MyConnection;
                _with31.CommandType = CommandType.StoredProcedure;
                _with31.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_COST_INS";
                var _with32 = _with31.Parameters;
                insCostDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_EXP_COST_PK").Direction = ParameterDirection.Output;
                insCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with33 = updCostDetails;
                _with33.Connection = objWK.MyConnection;
                _with33.CommandType = CommandType.StoredProcedure;
                _with33.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_COST_UPD";
                var _with34 = _with33.Parameters;

                updCostDetails.Parameters.Add("JOB_TRN_AIR_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_AIR_EXP_COST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["JOB_TRN_AIR_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 50, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with35 = delCostDetails;
                _with35.Connection = objWK.MyConnection;
                _with35.CommandType = CommandType.StoredProcedure;
                _with35.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_COST_DEL";

                delCostDetails.Parameters.Add("JOB_TRN_AIR_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_AIR_EXP_COST_PK").Direction = ParameterDirection.Input;
                delCostDetails.Parameters["JOB_TRN_AIR_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                delCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "CLng(RETURN_VALUE)").Direction = ParameterDirection.Output;
                delCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with36 = objWK.MyDataAdapter;

                _with36.InsertCommand = insCostDetails;
                _with36.InsertCommand.Transaction = TRAN;

                _with36.UpdateCommand = updCostDetails;
                _with36.UpdateCommand.Transaction = TRAN;

                _with36.DeleteCommand = delCostDetails;
                _with36.DeleteCommand.Transaction = TRAN;

                RecAfct = _with36.Update(dsCostDetails.Tables[0]);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                //'End Koteshwari

                var _with37 = insOtherChargesDetails;
                _with37.Connection = objWK.MyConnection;
                _with37.CommandType = CommandType.StoredProcedure;
                _with37.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_OTH_CHRG_INS";
                var _with38 = _with37.Parameters;

                insOtherChargesDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                // By Amit Singh on 27-April-07
                insOtherChargesDetails.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "Payment_Type").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                // End

                // By Amit Singh on 11-May-07
                insOtherChargesDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                // End

                insOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                // Added Suresh Kumar 30.03.2006 - Print Check box for MAWB Print
                insOtherChargesDetails.Parameters.Add("PRINT_ON_MAWB_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["PRINT_ON_MAWB_IN"].SourceVersion = DataRowVersion.Current;
                //end

                insOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "job_trn_air_exp_oth_pk").Direction = ParameterDirection.Output;
                insOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with39 = updOtherChargesDetails;
                _with39.Connection = objWK.MyConnection;
                _with39.CommandType = CommandType.StoredProcedure;
                _with39.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_OTH_CHRG_UPD";
                var _with40 = _with39.Parameters;

                updOtherChargesDetails.Parameters.Add("JOB_TRN_AIR_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_oth_pk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["JOB_TRN_AIR_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                // By Amit Singh on 27-April-07
                updOtherChargesDetails.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "Payment_Type").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                // End

                // By Amit Singh on 11-May-07
                updOtherChargesDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                // End

                updOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                // Added Suresh Kumar 30.03.2006 - Print Check box for MAWB Print
                updOtherChargesDetails.Parameters.Add("PRINT_ON_MAWB_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["PRINT_ON_MAWB_IN"].SourceVersion = DataRowVersion.Current;
                //end

                updOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with41 = delOtherChargesDetails;
                _with41.Connection = objWK.MyConnection;
                _with41.CommandType = CommandType.StoredProcedure;
                _with41.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_OTH_CHRG_DEL";

                delOtherChargesDetails.Parameters.Add("JOB_TRN_AIR_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_oth_pk").Direction = ParameterDirection.Input;
                delOtherChargesDetails.Parameters["JOB_TRN_AIR_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                delOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with42 = objWK.MyDataAdapter;

                _with42.InsertCommand = insOtherChargesDetails;
                _with42.InsertCommand.Transaction = TRAN;

                _with42.UpdateCommand = updOtherChargesDetails;
                _with42.UpdateCommand.Transaction = TRAN;

                _with42.DeleteCommand = delOtherChargesDetails;
                _with42.DeleteCommand.Transaction = TRAN;

                RecAfct = _with42.Update(dsOtherCharges);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    if (intIsAirineUpdate == 1)
                    {
                        if (!string.IsNullOrEmpty(getDefault(strAirlinePK, "").ToString()))
                        {
                            arrMessage = (ArrayList)funUpStreamUpdationBookingAirline(strBookingRefNo, strAirlinePK, TRAN);
                            if (!(string.Compare(arrMessage[0].ToString(), "Saved") > 0))
                            {
                                TRAN.Rollback();
                                return arrMessage;
                            }
                        }
                    }

                    if (!string.IsNullOrEmpty(AirwayBillNo) & !string.IsNullOrEmpty(strAirMstPk))
                    {
                        arrMessage = Update_Airway_Bill_Trn(jobCardRefNumber, strBookingRefNo, AirwayBillNo, strAirMstPk, TRAN);
                        if (!(string.Compare(arrMessage[0].ToString(), "Saved") > 0))
                        {
                            TRAN.Rollback();
                            return arrMessage;
                        }
                    }
                    cls_JobCardView objJCFclLcl = new cls_JobCardView();
                    objJCFclLcl.CREATED_BY = Convert.ToInt64(M_CREATED_BY_FK);
                    objJCFclLcl.LAST_MODIFIED_BY = Convert.ToInt64(M_CREATED_BY_FK);
                    objJCFclLcl.ConfigurationPK = Convert.ToInt64(M_Configuration_PK);
                    //arrMessage = (ArrayList)objJCFclLcl.SaveJobCardDoc(Convert.ToString(JobCardPK), TRAN, dsDoc, 1, 1);
                    //Biztype -1(Air),Process Type -1(Export)
                    if (arrMessage.Count > 0)
                    {
                        if (!(string.Compare(arrMessage[0].ToString(), "Saved") > 0))
                        {
                            TRAN.Rollback();
                            return arrMessage;
                        }
                    }
                    arrMessage = (ArrayList)SaveTrackAndTrace(Convert.ToInt32(JobCardPK), TRAN, M_DataSet, dsTrackNTrace, Convert.ToInt32(userLocation), isEdting, dsMainBookingDetails);
                    if (arrMessage.Count > 0)
                    {
                        TRAN.Commit();
                        if (isEdting == true)
                        {
                            SaveHawbValues(Convert.ToInt32(JobCardPK));
                        }
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                    }
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        /// <summary>
        /// Saves the secondary services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool SaveSecondaryServices(WorkFlow objWK, OracleTransaction TRAN, int JobCardPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            if ((dsIncomeChargeDetails != null))
            {
                //----------------------------------Income Charge Details----------------------------------
                foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                {
                    int Frt_Pk = 0;
                    try
                    {
                        Frt_Pk = Convert.ToInt32(ri["JOB_TRN_AIR_EXP_FD_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Frt_Pk = 0;
                    }
                    var _with43 = objWK.MyCommand;
                    _with43.Parameters.Clear();
                    _with43.Transaction = TRAN;
                    _with43.CommandType = CommandType.StoredProcedure;
                    if (Frt_Pk > 0)
                    {
                        _with43.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_FD_UPD";
                        _with43.Parameters.Add("JOB_TRN_AIR_EXP_FD_PK_IN", ri["JOB_TRN_AIR_EXP_FD_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with43.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_FD_INS";
                        _with43.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }
                    _with43.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("BASIS_IN", getDefault(ri["BASIS"], "")).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("PRINT_ON_MAWB_IN", 1).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("BASIS_FK_IN", getDefault(ri["BASIS_PK"], "")).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("EXCHANGE_RATE_IN", getDefault(ri["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("RATE_PERBASIS_IN", getDefault(ri["RATEPERBASIS"], "")).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("QUANTITY_IN", getDefault(ri["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("SERVICE_MST_FK_IN", ri["SERVICE_MST_PK"]).Direction = ParameterDirection.Input;
                    _with43.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;

                    try
                    {
                        _with43.ExecuteNonQuery();
                        if (Frt_Pk == 0)
                        {
                            _with43.Parameters.Clear();
                            _with43.CommandType = CommandType.StoredProcedure;
                            _with43.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_FRT_SEQ_CURRVAL";
                            _with43.Parameters.Add("BIZ_IN", 1).Direction = ParameterDirection.Input;
                            _with43.Parameters.Add("PROCESS_IN", 1).Direction = ParameterDirection.Input;
                            _with43.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with43.ExecuteNonQuery();
                            Frt_Pk = Convert.ToInt32(_with43.Parameters["RETURN_VALUE"].Value);
                            ri["JOB_TRN_AIR_EXP_FD_PK"] = Frt_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            //----------------------------------Expense Charge Details----------------------------------
            if ((dsExpenseChargeDetails != null))
            {
                foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                {
                    int Cost_Pk = 0;
                    try
                    {
                        Cost_Pk = Convert.ToInt32(re["JOB_TRN_AIR_EXP_COST_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Cost_Pk = 0;
                    }
                    var _with44 = objWK.MyCommand;
                    _with44.Parameters.Clear();
                    _with44.Transaction = TRAN;
                    _with44.CommandType = CommandType.StoredProcedure;
                    if (Cost_Pk > 0)
                    {
                        _with44.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_COST_UPD";
                        _with44.Parameters.Add("JOB_TRN_AIR_EST_PK_IN", re["JOB_TRN_AIR_EXP_COST_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with44.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_COST_INS";
                        _with44.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }

                    _with44.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("VENDOR_MST_FK_IN", re["SUPPLIER_MST_PK"]).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("COST_ELEMENT_FK_IN", re["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("LOCATION_FK_IN", re["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("VENDOR_KEY_IN", re["SUPPLIER_MST_ID"]).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("PTMT_TYPE_IN", re["PTMT_TYPE"]).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("CURRENCY_MST_FK_IN", re["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("ESTIMATED_COST_IN", re["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("TOTAL_COST_IN", re["TOTAL_COST"]).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("BASIS_FK_IN", re["DD_VALUE"]).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("RATEPERBASIS_IN", re["RATEPERBASIS"]).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("QUANTITY_IN", getDefault(re["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("EXCHANGE_RATE_IN", getDefault(re["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("EXT_INT_FLAG_IN", getDefault(re["EXT_INT_FLAG"], 2)).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("SERVICE_MST_FK_IN", re["SERVICE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with44.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with44.ExecuteNonQuery();
                        if (Cost_Pk == 0)
                        {
                            _with44.Parameters.Clear();
                            _with44.CommandType = CommandType.StoredProcedure;
                            _with44.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_COST_SEQ_CURRVAL";
                            _with44.Parameters.Add("BIZ_IN", 1).Direction = ParameterDirection.Input;
                            _with44.Parameters.Add("PROCESS_IN", 1).Direction = ParameterDirection.Input;
                            _with44.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with44.ExecuteNonQuery();
                            Cost_Pk = Convert.ToInt32(_with44.Parameters["RETURN_VALUE"].Value);
                            re["JOB_TRN_AIR_EXP_COST_PK"] = Cost_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            ClearRemovedServices(objWK, TRAN, JobCardPK, dsIncomeChargeDetails, dsExpenseChargeDetails);
            return true;
        }

        /// <summary>
        /// Clears the removed services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool ClearRemovedServices(WorkFlow objWK, OracleTransaction TRAN, int JobCardPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            //Dim objwk As New WorkFlow
            string SelectedFrtPks = "";
            string SelectedCostPks = "";
            if (JobCardPK > 0)
            {
                //objwk.OpenConnection()
                //Dim TRAN As OracleTransaction
                //TRAN = objwk.MyConnection.BeginTransaction()
                try
                {
                    foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedFrtPks))
                        {
                            SelectedFrtPks = ri["JOB_TRN_AIR_EXP_FD_PK"].ToString();
                        }
                        else
                        {
                            SelectedFrtPks += "," + ri["JOB_TRN_AIR_EXP_FD_PK"];
                        }
                    }
                    foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedCostPks))
                        {
                            SelectedCostPks = re["JOB_TRN_AIR_EXP_COST_PK"].ToString();
                        }
                        else
                        {
                            SelectedCostPks += "," + re["JOB_TRN_AIR_EXP_COST_PK"];
                        }
                    }

                    var _with45 = objWK.MyCommand;
                    _with45.Transaction = TRAN;
                    _with45.CommandType = CommandType.StoredProcedure;
                    _with45.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.DELETE_AIR_EXP_SEC_CHG_EXCEPT";
                    _with45.Parameters.Clear();
                    _with45.Parameters.Add("JOB_CARD_AIR_EXP_PK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("JOB_TRN_AIR_EXP_FD_PKS", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("JOB_TRN_AIR_EXP_COST_PKS", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    _with45.ExecuteNonQuery();
                    //TRAN.Commit()
                }
                catch (OracleException oraexp)
                {
                    TRAN.Rollback();
                    //Throw oraexp
                }
                catch (Exception ex)
                {
                    TRAN.Rollback();
                    //Throw ex
                }
                finally
                {
                    //objwk.CloseConnection()
                }
            }
            return false;
        }

        /// <summary>
        /// Saves the hawb values.
        /// </summary>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet SaveHawbValues(int JobCardPK)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with46 = objWK.MyCommand.Parameters;
                _with46.Add("JOB_CARD_AIR_EXP_PK_IN", JobCardPK).Direction = ParameterDirection.Input;
                return objWK.GetDataSet("JOB_CARD_AIR_EXP_TBL_PKG", "UPDATE_HAWB_VALUES");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
        }

        //Private Sub UpdatePlaces(ByVal intBookingAirPk As Long, ByVal ColPk As String, ByVal DelPk As String)
        //ADD BY LATHA TO SAVE PLR
        /// <summary>
        /// Updates the places.
        /// </summary>
        /// <param name="intBookingAirPk">The int booking air pk.</param>
        /// <param name="ColPk">The col pk.</param>
        /// <param name="DelPk">The delete pk.</param>
        /// <param name="TRAN">The tran.</param>
        private void UpdatePlaces(long intBookingAirPk, string ColPk, string DelPk, OracleTransaction TRAN = null)
        {
            WorkFlow objWK = new WorkFlow();

            try
            {
                ColPk = getDefault(ColPk, "").ToString();
                DelPk = getDefault(DelPk, "").ToString();
                string SQLQuery = null;

                SQLQuery = "UPDATE BOOKING_AIR_TBL SET";
                SQLQuery += " col_place_mst_fk   =   '" + ColPk + "'";
                //& ColPk
                SQLQuery += ",";
                SQLQuery += " del_place_mst_fk =  '" + DelPk + "'";
                //& DelPk
                SQLQuery += " WHERE ";
                SQLQuery += " BOOKING_AIR_PK = " + intBookingAirPk;

                if ((TRAN != null))
                {
                    var _with47 = objWK.MyCommand;
                    _with47.Parameters.Clear();
                    _with47.Connection = TRAN.Connection;
                    _with47.CommandType = CommandType.Text;
                    _with47.CommandText = SQLQuery;
                    _with47.Transaction = TRAN;
                    _with47.ExecuteNonQuery();
                }
                else
                {
                    UpdatePlacesRefDel(SQLQuery);
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            // SQLQuery &= " BOOKING_AIR_PK  IN (SELECT BOK.BOOKING_AIR_PK FROM BOOKING_AIR_TBL BOK WHERE BOK.BOOKING_REF_NO = '&txtBookingNo.Text&' "
        }

        /// <summary>
        /// Updates the places reference delete.
        /// </summary>
        /// <param name="SQLQuery">The SQL query.</param>
        public void UpdatePlacesRefDel(string SQLQuery)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                objWF.ExecuteScaler(SQLQuery);
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

        #endregion "Save Function"

        #region "Save Special Requirment"

        /// <summary>
        /// Saves the transaction hz SPCL.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="strSpclRequest">The string SPCL request.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        private ArrayList SaveTransactionHZSpcl(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            try
            {
                if (!string.IsNullOrEmpty(strSpclRequest))
                {
                    arrMessage.Clear();
                    string[] strParam = null;
                    strParam = strSpclRequest.Split('~');
                    try
                    {
                        var _with48 = SCM;
                        _with48.CommandType = CommandType.StoredProcedure;
                        _with48.CommandText = UserName + ".JOB_CARD_SPCL_REQ_PKG.JOB_TRN_SEA_HAZ_SPL_REQ_INS";
                        var _with49 = _with48.Parameters;
                        _with49.Clear();
                        //BKG_TRN_SEA_FK_IN()
                        _with49.Add("JOB_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        //OUTER_PACK_TYPE_MST_FK_IN()
                        _with49.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], "")).Direction = ParameterDirection.Input;
                        //INNER_PACK_TYPE_MST_FK_IN()
                        _with49.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], "")).Direction = ParameterDirection.Input;
                        //MIN_TEMP_IN()
                        _with49.Add("MIN_TEMP_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                        //MIN_TEMP_UOM_IN()
                        _with49.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? "" : strParam[3]), 0)).Direction = ParameterDirection.Input;
                        //MAX_TEMP_IN()
                        _with49.Add("MAX_TEMP_IN", getDefault(strParam[4], "")).Direction = ParameterDirection.Input;
                        //MAX_TEMP_UOM_IN()
                        _with49.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                        //FLASH_PNT_TEMP_IN()
                        _with49.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
                        //FLASH_PNT_TEMP_UOM_IN()
                        _with49.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                        //IMDG_CLASS_CODE_IN()
                        _with49.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
                        //UN_NO_IN()
                        _with49.Add("UN_NO_IN", getDefault(strParam[9], "")).Direction = ParameterDirection.Input;
                        //IMO_SURCHARGE_IN()
                        _with49.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                        //SURCHARGE_AMT_IN()
                        _with49.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                        //IS_MARINE_POLLUTANT_IN()
                        _with49.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                        //EMS_NUMBER_IN()
                        _with49.Add("EMS_NUMBER_IN", getDefault(strParam[13], "")).Direction = ParameterDirection.Input;
                        _with49.Add("PROPER_SHIPPING_NAME_IN", getDefault(strParam[14], "")).Direction = ParameterDirection.Input;
                        _with49.Add("PACK_CLASS_TYPE_IN", getDefault(strParam[15], "")).Direction = ParameterDirection.Input;
                        _with49.Add("PROCESS_TYPE_IN", 1).Direction = ParameterDirection.Input;
                        _with49.Add("BIZ_TYPE_IN", 1).Direction = ParameterDirection.Input;
                        //RETURN_VALUE()
                        _with49.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with48.ExecuteNonQuery();
                        arrMessage.Add("All Data Saved Successfully");
                        return arrMessage;
                    }
                    catch (OracleException oraexp)
                    {
                        arrMessage.Add(oraexp.Message);
                        return arrMessage;
                    }
                    catch (Exception ex)
                    {
                        arrMessage.Add(ex.Message);
                        return arrMessage;
                    }
                }
                arrMessage.Add("All Data Saved Successfully");
                return arrMessage;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Saves the transaction reefer.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="strSpclRequest">The string SPCL request.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        private ArrayList SaveTransactionReefer(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with50 = SCM;
                    _with50.CommandType = CommandType.StoredProcedure;
                    _with50.CommandText = UserName + ".JOB_CARD_SPCL_REQ_PKG.JOB_TRN_SEA_REF_SPL_REQ_INS";
                    var _with51 = _with50.Parameters;
                    _with51.Clear();
                    //BOOKING_TRN_SEA_FK_IN()
                    _with51.Add("JOB_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //VENTILATION_IN()
                    _with51.Add("VENTILATION_IN", strParam[0]).Direction = ParameterDirection.Input;
                    //AIR_COOL_METHOD_IN()
                    _with51.Add("AIR_COOL_METHOD_IN", strParam[1]).Direction = ParameterDirection.Input;
                    //HUMIDITY_FACTOR_IN()
                    _with51.Add("HUMIDITY_FACTOR_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                    //IS_PERSHIABLE_GOODS_IN()
                    _with51.Add("IS_PERSHIABLE_GOODS_IN", strParam[3]).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with51.Add("MIN_TEMP_IN", getDefault(strParam[4], "")).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with51.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with51.Add("MAX_TEMP_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with51.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                    //PACK_TYPE_MST_FK_IN()
                    _with51.Add("PACK_TYPE_MST_FK_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
                    //PACK_COUNT_IN()
                    _with51.Add("PACK_COUNT_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    _with51.Add("PROCESS_TYPE_IN", 1).Direction = ParameterDirection.Input;
                    _with51.Add("BIZ_TYPE_IN", 1).Direction = ParameterDirection.Input;
                    _with51.Add("REF_VENTILATION_IN", getDefault(strParam[10], "")).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with51.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with50.ExecuteNonQuery();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
                catch (OracleException oraexp)
                {
                    arrMessage.Add(oraexp.Message);
                    return arrMessage;
                }
                catch (Exception ex)
                {
                    arrMessage.Add(ex.Message);
                    return arrMessage;
                }
            }
            arrMessage.Add("All Data Saved Successfully");
            return arrMessage;
        }

        #endregion "Save Special Requirment"

        #region "Track N Trace Insertion"

        /// <summary>
        /// Saves the track and trace.
        /// </summary>
        /// <param name="jobPk">The job pk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="dsContainer">The ds container.</param>
        /// <param name="nlocationfk">The nlocationfk.</param>
        /// <param name="IsEditing">if set to <c>true</c> [is editing].</param>
        /// <param name="dsMainBookingDetails">The ds main booking details.</param>
        /// <returns></returns>
        public object SaveTrackAndTrace(int jobPk, OracleTransaction TRAN, DataSet dsMain, DataSet dsContainer, int nlocationfk, bool IsEditing, DataSet dsMainBookingDetails)
        {
            try
            {
                int Cnt = 0;
                bool UpdATD = false;
                bool UpdLDdate = false;
                string strContData = null;

                if (!string.IsNullOrEmpty((dsMain.Tables[0].Rows[Cnt]["departure_date"].ToString())))
                {
                    UpdATD = true;
                }
                objTrackNTrace.DeleteOnSaveTraceExportOnATDLDUpd(jobPk, 1, 1, "Vessel Voyage", "LD-DT-DATA-DEL-AIR-EXP", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                "Null");

                for (Cnt = 0; Cnt <= dsContainer.Tables[0].Rows.Count - 1; Cnt++)
                {
                    if (!string.IsNullOrEmpty((dsContainer.Tables[0].Rows[Cnt]["load_date"].ToString())))
                    {
                        UpdLDdate = true;
                        var _with52 = dsContainer.Tables[0].Rows[Cnt];
                        //strContData = "Loaded Pallete " & .Item("palette_size") & " On  " & .Item("load_date")
                        // Updated by Amit on 05-Jan-07 For Task DTS-1833
                        if (string.IsNullOrEmpty((dsMain.Tables[0].Rows[0]["flight_no"].ToString())))
                        {
                            strContData = "Cargo Loaded " + "~" + _with52["load_date"];
                        }
                        else
                        {
                            strContData = "Cargo Loaded in " + dsMain.Tables[0].Rows[0]["flight_no"] + "~" + _with52["load_date"];
                        }
                        arrMessage = objTrackNTrace.SaveTrackAndTraceExportOnLDUpd(jobPk, 1, 1, "Vessel Voyage", "LD-DT-UPD-JOB-AIR-EXP", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                        strContData);
                    }
                }

                //If UpdLDdate = True Then
                //    arrMessage = objTrackNTrace.SaveTrackAndTraceExportOnLDUpd(jobPk, 1, 1, "Vessel Voyage", "LD-DT-UPD-JOB-AIR-EXP", nlocationfk, TRAN, "INS", "O")
                //End If
                if (UpdATD == true & IsEditing == true)
                {
                    for (Cnt = 0; Cnt <= dsMainBookingDetails.Tables[0].Rows.Count - 1; Cnt++)
                    {
                        if (!string.IsNullOrEmpty((dsMainBookingDetails.Tables[0].Rows[Cnt]["departure_date"].ToString())))
                        {
                            var _with53 = dsMainBookingDetails.Tables[0].Rows[Cnt];
                            // strContData = "Departed from " & .Item("POL") & "~" & .Item("departure_date")
                            strContData = "Departed from " + _with53["POL"];
                            arrMessage = objTrackNTrace.SaveTrackAndTraceExportOnATDUpd(jobPk, 1, 1, "Sail", "ATD-UPD-JOB-AIR-EXP", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                            strContData);
                        }
                    }
                }
                else
                {
                    arrMessage.Clear();
                    arrMessage.Add("All Data Saved Successfully");
                }
                return arrMessage;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Track N Trace Insertion"

        #region "Fetch Main Jobcard for export"

        /// <summary>
        /// Fetches the main job card data exp.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet FetchMainJobCardDataExp(string jobCardPK = "0")
        {
            StringBuilder strSQL = new StringBuilder();

            strSQL.Append("SELECT");
            strSQL.Append("    job_exp.job_card_air_exp_pk,");
            strSQL.Append("    job_exp.booking_air_fk,");
            strSQL.Append("    job_exp.jobcard_ref_no,");
            strSQL.Append("    bat.booking_ref_no,");
            strSQL.Append("    bat.booking_date,");
            strSQL.Append("    bat.cust_customer_mst_fk,");
            strSQL.Append("    cust.customer_id,cust.customer_name, job_exp.del_address,");
            strSQL.Append("    bat.col_place_mst_fk, ");
            strSQL.Append("    col_place.place_name \"CollectionPlace\",");
            strSQL.Append("    bat.port_mst_pol_fk,");
            strSQL.Append("    pol.port_name \"POL\",");
            strSQL.Append("    bat.port_mst_pod_fk, ");
            strSQL.Append("    pod.port_name \"POD\",");
            strSQL.Append("    bat.del_place_mst_fk, ");
            strSQL.Append("    del_place.place_name \"DeliveryPlace\",");
            strSQL.Append("    job_card_status,");
            strSQL.Append("    job_card_closed_on,");
            strSQL.Append("    job_exp.WIN_XML_STATUS,");
            strSQL.Append("    job_exp.WIN_TOTAL_QTY,");
            strSQL.Append("    job_exp.WIN_REC_QTY,");
            strSQL.Append("    job_exp.WIN_BALANCE_QTY,");
            strSQL.Append("    job_exp.WIN_TOTAL_WT,");
            strSQL.Append("    job_exp.WIN_REC_WT,");
            strSQL.Append("    job_exp.WIN_BALANCE_WT,");
            strSQL.Append("    bat.airline_mst_fk,");
            strSQL.Append("    airline.airline_id ,");
            strSQL.Append("    airline.airline_name,");
            strSQL.Append("    job_exp.flight_no,");
            strSQL.Append("    job_exp.AIRLINE_SCHEDULE_TRN_FK,");
            strSQL.Append("    job_exp.eta_date,");
            strSQL.Append("    job_exp.etd_date,");
            strSQL.Append("    job_exp.arrival_date,");
            strSQL.Append("    job_exp.departure_date,");
            strSQL.Append("    job_exp.shipper_cust_mst_fk,");
            strSQL.Append("    shipper.customer_id \"Shipper\",");
            strSQL.Append("    shipper.customer_name \"ShipperName\",");
            strSQL.Append("    job_exp.consignee_cust_mst_fk, ");
            strSQL.Append("    consignee.customer_id \"Consignee\",");
            strSQL.Append("    consignee.customer_name \"ConsigneeName\",");
            strSQL.Append("    job_exp.notify1_cust_mst_fk, ");
            strSQL.Append("    notify1.customer_id \"Notify1\" ,");
            strSQL.Append("    notify1.customer_name \"Notify1Name\" ,");
            strSQL.Append("    job_exp.notify2_cust_mst_fk,");
            strSQL.Append("    notify2.customer_id \"Notify2\",");
            strSQL.Append("    notify2.customer_name \"Notify2Name\",");
            strSQL.Append("    job_exp.cb_agent_mst_fk, ");
            strSQL.Append("    cbagnt.agent_id \"cbAgent\",");
            strSQL.Append("    cbagnt.agent_name \"cbAgentName\",");
            strSQL.Append("    job_exp.dp_agent_mst_fk, ");
            strSQL.Append("    dpagnt.agent_id \"dpAgent\",");
            strSQL.Append("    dpagnt.agent_name \"dpAgentName\",");
            strSQL.Append("    job_exp.cl_agent_mst_fk,");
            strSQL.Append("    clagnt.agent_id \"clAgent\",");
            strSQL.Append("    clagnt.agent_name \"clAgentName\",");
            strSQL.Append("    job_exp.remarks,");
            strSQL.Append("    job_exp.version_no,");
            strSQL.Append("    TO_CHAR(job_exp.jobcard_date,DATEFORMAT) jobcard_date,");
            strSQL.Append("    job_exp.ucr_no,");
            strSQL.Append("    job_exp.cargo_move_fk,");
            strSQL.Append("    job_exp.pymt_type,");
            strSQL.Append("    job_exp.shipping_terms_mst_fk,");
            strSQL.Append("    job_exp.insurance_amt,");
            strSQL.Append("    job_exp.insurance_currency,");
            strSQL.Append("    job_exp.commodity_group_fk,");
            strSQL.Append("    comm.commodity_group_desc,");

            //fields are added after the UAT..
            strSQL.Append("    depot.vendor_id \"depot_id\",");
            strSQL.Append("    depot.vendor_name \"depot_name\",");
            strSQL.Append("    depot.vendor_mst_pk \"depot_pk\",");
            strSQL.Append("    carrier.vendor_id \"carrier_id\",");
            strSQL.Append("    carrier.vendor_name \"carrier_name\",");
            strSQL.Append("    carrier.vendor_mst_pk \"carrier_pk\",");
            strSQL.Append("    country.country_id \"country_id\",");
            strSQL.Append("    country.country_name \"country_name\",");
            strSQL.Append("    country.country_mst_pk \"country_mst_pk\",");
            strSQL.Append("    job_exp.da_number \"da_number\",");
            strSQL.Append("    job_exp.hawb_exp_tbl_fk, ");
            strSQL.Append("    job_exp.mawb_exp_tbl_fk, ");
            strSQL.Append("    job_exp.master_jc_air_exp_fk, ");
            strSQL.Append("    mst.master_jc_ref_no, ");
            strSQL.Append("    mst.MASTER_JC_DATE, ");

            //Added On March 30 - Rijesh
            strSQL.Append("    job_exp.GOODS_DESCRIPTION,");
            strSQL.Append("    job_exp.MARKS_NUMBERS,");

            //Added by Manoharan for display HBL&MBL no.
            strSQL.Append("    hawb.hawb_ref_no hawb_no,");
            strSQL.Append("    mawb.mawb_ref_no mawb_no,");
            strSQL.Append("    hawb.HAWB_DATE,");
            strSQL.Append("    mawb.MAWB_DATE,");
            //end
            //Code Added By Anil on 18 Aug 09
            strSQL.Append("    job_exp.sb_number,job_exp.sb_date, ");
            //End By Anil

            //Added by SuryaPrasad for display Currency ID.
            strSQL.Append("    cur.currency_id,job_exp.ADDVATOS_FLAG,job_exp.LC_SHIPMENT,");
            //end
            strSQL.Append("    job_exp.SHIPPING_INST_FLAG,");
            strSQL.Append("    job_exp.RFS,");
            strSQL.Append("    job_exp.CRQ,");
            strSQL.Append("    job_exp.SHIPPING_INST_DT,");
            strSQL.Append("    job_exp.RFS_DATE,");
            strSQL.Append("    job_exp.CRQ_DATE, ");
            strSQL.Append("    NVL(JOB_EXP.CHK_NOMINATED,0) CHK_NOMINATED,");
            strSQL.Append("    NVL(JOB_EXP.CHK_CSR,1) CHK_CSR,");
            strSQL.Append("    NVL(EMP.EMPLOYEE_MST_PK,NVL(SHP_SE.EMPLOYEE_MST_PK,0)) SALES_EXEC_FK,");
            strSQL.Append("    NVL(EMP.EMPLOYEE_ID,SHP_SE.EMPLOYEE_ID) SALES_EXEC_ID,");
            strSQL.Append("    NVL(EMP.EMPLOYEE_NAME,SHP_SE.EMPLOYEE_NAME) SALES_EXEC_NAME,");
            strSQL.Append("    job_exp.cc_req,job_exp.cc_ie,");
            strSQL.Append("    job_exp.CHA_AGENT_MST_FK,job_exp.PRC_FK,job_exp.ONC_FK,job_exp.PRC_MODE_FK,job_exp.ONC_MODE_FK,");
            strSQL.Append("    CHAAGNT.VENDOR_ID \"CHAAgentID\",");
            strSQL.Append("    CHAAGNT.VENDOR_NAME \"CHAAgentName\" ");
            strSQL.Append(" FROM ");
            strSQL.Append("    job_card_air_exp_tbl job_exp,");
            strSQL.Append("    booking_air_tbl bat,");
            strSQL.Append("    port_mst_tbl POD,");
            strSQL.Append("    port_mst_tbl POL,");
            strSQL.Append("    customer_mst_tbl cust,");
            strSQL.Append("    customer_mst_tbl consignee,");
            strSQL.Append("    customer_mst_tbl shipper,");
            strSQL.Append("    customer_mst_tbl notify1,");
            strSQL.Append("    customer_mst_tbl notify2,");
            strSQL.Append("    place_mst_tbl col_place,");
            strSQL.Append("    place_mst_tbl del_place,");
            strSQL.Append("    airline_mst_tbl airline,");
            strSQL.Append("    agent_mst_tbl clagnt, ");
            strSQL.Append("    agent_mst_tbl dpagnt, ");
            strSQL.Append("    agent_mst_tbl cbagnt,");
            strSQL.Append("    commodity_group_mst_tbl comm, ");
            strSQL.Append("    vendor_mst_tbl  depot,");
            strSQL.Append("    vendor_mst_tbl  carrier,");
            strSQL.Append("    country_mst_tbl country,");
            strSQL.Append("    master_jc_air_exp_tbl mst,");
            strSQL.Append("    hawb_exp_tbl hawb,");
            strSQL.Append("    mawb_exp_tbl mawb,");
            strSQL.Append("     currency_type_mst_tbl cur,");
            strSQL.Append("    EMPLOYEE_MST_TBL        EMP, ");
            strSQL.Append("    EMPLOYEE_MST_TBL        SHP_SE, ");
            strSQL.Append("    VENDOR_MST_TBL          CHAAGNT ");
            //SHIPPER SALES PERSON
            strSQL.Append("WHERE");
            strSQL.Append("    job_exp.job_card_air_exp_pk = " + jobCardPK);
            strSQL.Append("    AND job_exp.booking_air_fk           =  bat.booking_air_pk");
            strSQL.Append("    AND bat.port_mst_pol_fk              =  pol.port_mst_pk");
            strSQL.Append("    AND bat.port_mst_pod_fk              =  pod.port_mst_pk");
            strSQL.Append("    AND bat.col_place_mst_fk             =  col_place.place_pk(+)");
            strSQL.Append("    AND bat.del_place_mst_fk             =  del_place.place_pk(+)");
            strSQL.Append("    AND bat.cust_customer_mst_fk         =  cust.customer_mst_pk(+) ");
            strSQL.Append("    AND bat.airline_mst_fk               =  airline.airline_mst_pk(+)");
            strSQL.Append("    AND job_exp.shipper_cust_mst_fk      =  shipper.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.consignee_cust_mst_fk    =  consignee.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.notify1_cust_mst_fk      =  notify1.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.Notify2_Cust_Mst_Fk      =  notify2.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.cl_agent_mst_fk          =  clagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.cb_agent_mst_fk          =  cbagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.dp_agent_mst_fk          =  dpagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.commodity_group_fk       =  comm.commodity_group_pk(+)");

            //conditions are added after the UAT..
            strSQL.Append("    AND job_exp.transporter_depot_fk     =  depot.vendor_mst_pk(+)");
            strSQL.Append("    AND job_exp.transporter_carrier_fk   =  carrier.vendor_mst_pk(+)");
            strSQL.Append("    AND job_exp.country_origin_fk        =  country.country_mst_pk(+)");
            strSQL.Append("    AND job_exp.master_jc_air_exp_fk     =  mst.master_jc_air_exp_pk(+)");

            strSQL.Append("    AND shipper.REP_EMP_MST_FK=SHP_SE.EMPLOYEE_MST_PK(+) ");
            strSQL.Append("    AND JOB_EXP.EXECUTIVE_MST_FK=EMP.EMPLOYEE_MST_PK(+) ");
            //Added by Manoharan for display HBL&MBL no.
            strSQL.Append("    AND hawb.hawb_exp_tbl_pk(+) = job_exp.hawb_exp_tbl_fk");
            strSQL.Append("    AND mawb.mawb_exp_tbl_pk(+) = job_exp.mawb_exp_tbl_fk");
            strSQL.Append("    AND JOB_EXP.CHA_AGENT_MST_FK = CHAAGNT.VENDOR_MST_PK(+) ");
            //end
            //Added by SuryaPrasad for display Currency ID.
            strSQL.Append("    and cur.currency_mst_pk(+) = job_exp.base_currency_fk");
            //end
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion "Fetch Main Jobcard for export"

        #region " Fetch Container data export"

        /// <summary>
        /// Fetches the container data exp.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <returns></returns>
        public DataSet FetchContainerDataExp(string jobCardPK = "0", string MJCPK = "")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    job_trn_cont_pk,");
                strSQL.Append("    job_trn_cont.palette_size,");
                strSQL.Append("    job_trn_cont.airfreight_slabs_tbl_fk,");
                strSQL.Append("    job_trn_cont.uld_number,");
                strSQL.Append("    airfreight.breakpoint_id,");
                strSQL.Append("    (SELECT ROWTOCOL('SELECT C.COMMODITY_NAME FROM COMMODITY_MST_TBL C WHERE C.COMMODITY_MST_PK IN (' || NVL(JOB_TRN_CONT.COMMODITY_MST_FKS, -1) || ') ORDER BY C.COMMODITY_NAME') FROM DUAL) COMMODITY_MST_FK,");
                //strSQL.Append(vbCrLf & "    pack_type_mst_fk,")
                strSQL.Append(" (SELECT ROWTOCOL('SELECT PT.PACK_TYPE_DESC FROM PACK_TYPE_MST_TBL PT WHERE PT.PACK_TYPE_MST_PK IN (");
                strSQL.Append("     SELECT DISTINCT JC.PACK_TYPE_FK FROM job_trn_cont JOB,JOBCARD_COMMODITY_DTL JC ");
                strSQL.Append("     WHERE JOB.job_trn_cont_pk=JC.JOB_TRN_CONT_FK ");
                strSQL.Append("     AND JOB.job_trn_cont_pk='||JOB_TRN_CONT.job_trn_cont_pk||')') FROM DUAL) PACK_TYPE_MST_FK, ");
                strSQL.Append("    pack_count,");
                //strSQL.Append(vbCrLf & "    job_trn_cont.uld_number,")
                strSQL.Append("    gross_weight,");
                strSQL.Append("    volume_in_cbm,");
                //strSQL.Append(vbCrLf & "    gross_weight,")
                strSQL.Append("    chargeable_weight,");
                //strSQL.Append(vbCrLf & "    pack_type_mst_fk,")
                //strSQL.Append(vbCrLf & "    pack_count,")
                //strSQL.Append(vbCrLf & "    NULL COMMODITY_MST_FK,")
                if (string.IsNullOrEmpty(MJCPK))
                {
                    strSQL.Append("    TO_CHAR(job_trn_cont.load_date ,DATETIMEFORMAT24) load_date,'false' as \"Delete\"");
                }
                else
                {
                    strSQL.Append("    TO_CHAR(job_exp.departure_date ,DATETIMEFORMAT24) load_date,'false' as \"Delete\"");
                }
                strSQL.Append(" ,JOB_TRN_CONT.COMMODITY_MST_FKS ");
                strSQL.Append("FROM");
                strSQL.Append("    job_trn_cont job_trn_cont,");
                strSQL.Append("    pack_type_mst_tbl pack,");
                strSQL.Append("    airfreight_slabs_tbl airfreight,");
                //added for ULD Type
                strSQL.Append("    commodity_mst_tbl comm,");
                strSQL.Append("    JOB_CARD_TRN job_exp");
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_cont.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
                strSQL.Append("    and job_trn_cont.airfreight_slabs_tbl_FK = airfreight.airfreight_slabs_tbl_pk(+)");
                //added for ULD Type
                strSQL.Append("    AND job_trn_cont.commodity_mst_fk = comm.commodity_mst_pk(+)");
                strSQL.Append("    AND job_trn_cont.job_card_trn_fk = job_exp.JOB_CARD_TRN_PK");
                strSQL.Append("    AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK);

                return objWF.GetDataSet(strSQL.ToString());
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

        /// <summary>
        /// Fetches the container data exp new.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <returns></returns>
        public DataSet FetchContainerDataExpNew(string jobCardPK = "0", string MJCPK = "")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    job_trn_air_exp_cont_pk,");
                strSQL.Append("    job_trn_cont.palette_size,");
                strSQL.Append("    job_trn_cont.airfreight_slabs_tbl_fk,");
                strSQL.Append("    airfreight.breakpoint_id,");
                strSQL.Append("    job_trn_cont.uld_number,");
                strSQL.Append("    volume_in_cbm,");
                strSQL.Append("    gross_weight,");
                strSQL.Append("    chargeable_weight,");
                strSQL.Append("    pack_type_mst_fk,");
                strSQL.Append("    pack_count,");
                strSQL.Append("    JOB_TRN_CONT.COMMODITY_MST_FKS,");
                //strSQL.Append(vbCrLf & "   comm.commodity_mst_pk, ")
                if (string.IsNullOrEmpty(MJCPK))
                {
                    strSQL.Append("    TO_CHAR(job_trn_cont.load_date ,DATETIMEFORMAT24) load_date,'false' as \"Delete\"");
                }
                else
                {
                    strSQL.Append("    TO_CHAR(job_exp.departure_date ,DATETIMEFORMAT24) load_date,'false' as \"Delete\"");
                }
                strSQL.Append("FROM");
                strSQL.Append("    job_trn_air_exp_cont job_trn_cont,");
                strSQL.Append("    pack_type_mst_tbl pack,");
                strSQL.Append("    airfreight_slabs_tbl airfreight,");
                //added for ULD Type
                strSQL.Append("    commodity_mst_tbl comm,");
                strSQL.Append("    job_card_air_exp_tbl job_exp");
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_cont.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
                strSQL.Append("    and job_trn_cont.airfreight_slabs_tbl_FK = airfreight.airfreight_slabs_tbl_pk(+)");
                //added for ULD Type
                strSQL.Append("    AND job_trn_cont.commodity_mst_fk = comm.commodity_mst_pk(+)");
                strSQL.Append("    AND job_trn_cont.job_card_air_exp_fk = job_exp.job_card_air_exp_pk");
                strSQL.Append("    AND job_exp.job_card_air_exp_pk =" + jobCardPK);

                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion " Fetch Container data export"

        #region "To Fetch The Jobcard Close Status"

        /// <summary>
        /// Fetches the jobcard close status.
        /// </summary>
        /// <param name="JobCardPk">The job card pk.</param>
        /// <returns></returns>
        public DataSet FetchJobcardCloseStatus(int JobCardPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT (CASE");
            sb.Append("         WHEN J.COLLECTION_DATE > J.PAYEMENT_DATE THEN");
            sb.Append("         ");
            sb.Append("          TO_CHAR(J.COLLECTION_DATE, 'dd/MM/yyyy')");
            sb.Append("         ELSE");
            sb.Append("         ");
            sb.Append("          TO_CHAR(J.PAYEMENT_DATE, 'dd/MM/yyyy')");
            sb.Append("       END) JOCARDCLOSEDATE");
            sb.Append("  FROM JOB_CARD_AIR_EXP_TBL J");
            sb.Append(" WHERE J.JOB_CARD_AIR_EXP_PK = " + JobCardPk);
            sb.Append("   AND J.COLLECTION_STATUS = 1");
            sb.Append("   AND J.PAYEMENT_STATUS = 1");
            sb.Append("");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "To Fetch The Jobcard Close Status"

        #region "Frieght Element"

        //added by thiyagarajan for disable entry in jobcard
        /// <summary>
        /// Fetches the fret.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <returns></returns>
        public DataSet FetchFret(int jobcardpk)
        {
            try
            {
                string strsql = null;
                WorkFlow objWF = new WorkFlow();
                strsql = "select con.Frt_Oth_Element_Fk from CONSOL_INVOICE_TRN_TBl con where JOB_CARD_FK = " + jobcardpk + " AND FRT_OTH_ELEMENT = 1";
                return objWF.GetDataSet(strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the agent fret.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <returns></returns>
        public DataSet FetchAgentFret(int jobcardpk)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT *");
                sb.Append("  FROM INV_AGENT_TBL INV, INV_AGENT_TRN_TBL INVTRN");
                sb.Append(" WHERE INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
                sb.Append("   AND INV.JOB_CARD_FK = " + jobcardpk);
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Frieght Element"

        #region " Fetch Freight data export"

        //added by manoharan 4/11/2006
        /// <summary>
        /// Gets the FRT det.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet GetFrtDet(string jobCardPK = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");

                strSQL.Append("    job_trn_fd.inv_cust_trn_air_exp_fk,");
                strSQL.Append("    job_trn_fd.inv_agent_trn_air_exp_fk,");
                strSQL.Append("    job_trn_fd.consol_invoice_trn_fk");
                strSQL.Append("FROM");
                strSQL.Append("    job_trn_air_exp_fd job_trn_fd,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    freight_element_mst_tbl freight,");
                strSQL.Append("    job_card_air_exp_tbl job_exp");
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_fd.job_card_air_exp_fk = job_exp.job_card_air_exp_pk");
                strSQL.Append("    AND job_trn_fd.Currency_Mst_Fk = curr.currency_mst_pk");
                strSQL.Append("    AND job_trn_fd.freight_element_mst_fk = freight.freight_element_mst_pk");
                strSQL.Append("    AND job_exp.job_card_air_exp_pk =" + jobCardPK);
                strSQL.Append("    AND NVL(job_trn_fd.SERVICE_TYPE_FLAG,0)<>1");
                strSQL.Append("    ORDER BY freight.freight_element_id");

                return objWF.GetDataSet(strSQL.ToString());
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

        /// <summary>
        /// Fetches the freight data exp.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="BaseCurrFk">The base curr fk.</param>
        /// <returns></returns>
        public DataSet FetchFreightDataExp(string jobCardPK = "0", string BaseCurrFk = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append(" SELECT QRY.* FROM (");
                strSQL.Append(" SELECT Q.* FROM (");
                strSQL.Append(" SELECT");
                strSQL.Append("    job_trn_fd_pk,");
                strSQL.Append("    freight.freight_element_id,");
                strSQL.Append("    freight.freight_element_name,");
                strSQL.Append("    freight.freight_element_mst_pk,");
                strSQL.Append("    DECODE(job_trn_fd.basis,0,' ',1,'%',2,'Flat rate',3,'Kgs',4,'Unit') basis,");
                strSQL.Append("    job_trn_fd.quantity,");
                strSQL.Append("    DECODE(job_trn_fd.freight_type,1,'Prepaid',2,'Collect') freight_type,");
                //To introduced LOCATION & FREIGHT PAYER Column
                //by Amit Singh on 9-May-07
                strSQL.Append("    job_trn_fd.location_mst_fk, ");
                //strSQL.Append(vbCrLf & "    (CASE WHEN  job_trn_fd.freight_type=1 THEN lmt.location_id ELSE pmt.port_id END) ""location_id"",")
                strSQL.Append("    lmt.location_id ,");
                strSQL.Append("    job_trn_fd.frtpayer_cust_mst_fk,");
                strSQL.Append("    cmt.customer_id, ");
                //End
                strSQL.Append("    job_trn_fd.currency_mst_fk,");
                //'
                //strSQL.Append(vbCrLf & "    job_trn_fd.Rateperbasis, ") ''Added By Koteshwari for Rate Column
                strSQL.Append("  nvl(job_trn_fd.Rateperbasis, 0) Rateperbasis,");
                //strSQL.Append(vbCrLf & "    freight_amt,")
                strSQL.Append("  nvl(job_trn_fd.freight_amt, 0) freight_amt,");
                //strSQL.Append(vbCrLf & "    job_trn_fd.currency_mst_fk,")
                if (Convert.ToInt32(BaseCurrFk) != 0)
                {
                    strSQL.Append("      NVL(GET_EX_RATE(job_trn_fd.CURRENCY_MST_FK, " + BaseCurrFk + ", job_exp.JOBCARD_DATE),0) AS ROE,");
                    strSQL.Append("       (job_trn_fd.freight_amt *NVL(GET_EX_RATE(job_trn_fd.CURRENCY_MST_FK, " + BaseCurrFk + ", job_exp.JOBCARD_DATE),0)) total_amt,");
                }
                else
                {
                    strSQL.Append("    EXCHANGE_RATE \"ROE\",");
                    strSQL.Append("    (job_trn_fd.freight_amt*job_trn_fd.Exchange_Rate) total_amt,");
                }
                strSQL.Append("    'false' as \"Delete\", job_trn_fd.PRINT_ON_MAWB \"Print\"");
                strSQL.Append("FROM");
                strSQL.Append("    job_trn_fd job_trn_fd,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    freight_element_mst_tbl freight,");
                strSQL.Append("    parameters_tbl prm,");
                strSQL.Append("    JOB_CARD_TRN job_exp,");
                //To introduced LOCATION & FREIGHT PAYER Column
                //by Amit Singh on 9-May-07
                strSQL.Append("    location_mst_tbl lmt,");
                //strSQL.Append(vbCrLf & "    port_mst_tbl pmt,")
                strSQL.Append("    customer_mst_tbl cmt");
                //End
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_fd.job_card_trn_fk = job_exp.JOB_CARD_TRN_PK");
                strSQL.Append("    AND job_trn_fd.Currency_Mst_Fk = curr.currency_mst_pk");
                strSQL.Append("    AND job_trn_fd.freight_element_mst_fk = freight.freight_element_mst_pk");
                strSQL.Append("    AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK);
                strSQL.Append("   AND job_trn_fd.freight_element_mst_fk = prm.frt_afc_fk");
                //To introduced LOCATION & FREIGHT PAYER Column
                //by Amit Singh on 9-May-07
                strSQL.Append("   AND job_trn_fd.location_mst_fk = lmt.location_mst_pk (+)");
                //strSQL.Append(vbCrLf & "   AND job_trn_fd.location_mst_fk = pmt.port_mst_pk (+)")
                strSQL.Append("   AND job_trn_fd.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+)");
                strSQL.Append("    AND NVL(job_trn_fd.SERVICE_TYPE_FLAG,0)<>1");
                strSQL.Append("    ORDER BY FREIGHT.PREFERENCE ) Q");

                //End
                strSQL.Append(" UNION ALL ");
                strSQL.Append("  SELECT Q1.* FROM (");
                strSQL.Append(" SELECT");
                strSQL.Append("    job_trn_fd_pk,");
                strSQL.Append("    freight.freight_element_id,");
                strSQL.Append("    freight.freight_element_name,");
                strSQL.Append("    freight.freight_element_mst_pk,");
                strSQL.Append("    DECODE(job_trn_fd.basis,0,' ',1,'%',2,'Flat rate',3,'Kgs',4,'Unit') basis,");
                strSQL.Append("    job_trn_fd.quantity,");
                strSQL.Append("    DECODE(job_trn_fd.freight_type,1,'Prepaid',2,'Collect') freight_type,");
                //by Amit Singh on 9-May-07
                strSQL.Append("    job_trn_fd.location_mst_fk, ");
                strSQL.Append("    lmt.location_id ,");
                strSQL.Append("    job_trn_fd.frtpayer_cust_mst_fk,");
                strSQL.Append("    cmt.customer_id, ");
                //End
                strSQL.Append("    job_trn_fd.currency_mst_fk,");
                //strSQL.Append(vbCrLf & "    job_trn_fd.Rateperbasis, ") ''Added By Koteshwari for Rate Column
                strSQL.Append("  nvl(job_trn_fd.Rateperbasis, 0) Rateperbasis,");
                //strSQL.Append(vbCrLf & "    freight_amt,")
                strSQL.Append("  nvl(job_trn_fd.freight_amt, 0) freight_amt,");
                //strSQL.Append(vbCrLf & "    job_trn_fd.currency_mst_fk,")
                if (Convert.ToInt32(BaseCurrFk) != 0)
                {
                    strSQL.Append("      NVL(GET_EX_RATE(job_trn_fd.CURRENCY_MST_FK, " + BaseCurrFk + ", job_exp.JOBCARD_DATE),0) AS ROE,");
                    strSQL.Append("       (job_trn_fd.freight_amt *NVL(GET_EX_RATE(job_trn_fd.CURRENCY_MST_FK, " + BaseCurrFk + ", job_exp.JOBCARD_DATE),0)) total_amt,");
                }
                else
                {
                    strSQL.Append("    EXCHANGE_RATE \"ROE\",");
                    strSQL.Append("    (job_trn_fd.freight_amt*job_trn_fd.Exchange_Rate) total_amt,");
                }
                strSQL.Append("    'false' as \"Delete\", job_trn_fd.PRINT_ON_MAWB \"Print\"");
                strSQL.Append("FROM");
                strSQL.Append("    job_trn_fd job_trn_fd,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    freight_element_mst_tbl freight,");
                strSQL.Append("    parameters_tbl prm,");
                strSQL.Append("    JOB_CARD_TRN job_exp,");
                //by Amit Singh on 9-May-07
                strSQL.Append("    location_mst_tbl lmt,");
                //strSQL.Append(vbCrLf & "    port_mst_tbl pmt,")
                strSQL.Append("    customer_mst_tbl cmt");
                //End
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_fd.job_card_trn_fk = job_exp.JOB_CARD_TRN_PK");
                strSQL.Append("    AND job_trn_fd.Currency_Mst_Fk = curr.currency_mst_pk");
                strSQL.Append("    AND job_trn_fd.freight_element_mst_fk = freight.freight_element_mst_pk");
                strSQL.Append("    AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK);
                strSQL.Append("   AND job_trn_fd.freight_element_mst_fk not in prm.frt_afc_fk");
                //by Amit Singh on 9-May-07
                strSQL.Append("   AND job_trn_fd.location_mst_fk = lmt.location_mst_pk (+)");
                //strSQL.Append(vbCrLf & "   AND job_trn_fd.location_mst_fk = pmt.port_mst_pk (+)")
                strSQL.Append("   AND job_trn_fd.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+)");
                strSQL.Append("    AND NVL(job_trn_fd.SERVICE_TYPE_FLAG,0)<>1");
                strSQL.Append("    ORDER BY FREIGHT.PREFERENCE ) Q1");
                strSQL.Append("     ) QRY,FREIGHT_ELEMENT_MST_TBL FMT ");
                strSQL.Append("     WHERE QRY.FREIGHT_ELEMENT_MST_PK=FMT.FREIGHT_ELEMENT_MST_PK");
                strSQL.Append("             ORDER BY FMT.PREFERENCE");

                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion " Fetch Freight data export"

        #region " Fetch Purchase Inventory data export"

        /// <summary>
        /// Gets the pia.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet GetPIA(string jobCardPK = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");

                strSQL.Append("    job_trn_pia.invoice_sea_tbl_fk,");
                strSQL.Append("    job_trn_pia.inv_agent_trn_air_exp_fk,");
                strSQL.Append("    job_trn_pia.inv_supplier_fk ");
                strSQL.Append("FROM");
                strSQL.Append("    job_trn_air_exp_pia  job_trn_pia,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    cost_element_mst_tbl cost_ele,");
                strSQL.Append("    job_card_air_exp_tbl job_exp");
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_pia.job_card_air_exp_fk = job_exp.job_card_air_exp_pk");
                strSQL.Append("    AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk");
                strSQL.Append("    AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk");
                strSQL.Append("    AND job_exp.job_card_air_exp_pk =" + jobCardPK);
                return objWF.GetDataSet(strSQL.ToString());
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

        /// <summary>
        /// Fetches the purchase inv data exp.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet FetchPurchaseInvDataExp(string jobCardPK = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT DISTINCT ");
                strSQL.Append("    job_trn_air_exp_pia_pk,");
                strSQL.Append("    vendor_key,\t");
                strSQL.Append("    cost_element_id,");
                strSQL.Append("    invoice_number,\t");
                strSQL.Append("    to_char(job_trn_pia.invoice_date, dateformat) invoice_date,");
                strSQL.Append("    currency_mst_fk,");
                strSQL.Append("    invoice_amt,\t");
                strSQL.Append("    tax_percentage,");
                strSQL.Append("    tax_amt,\t");
                strSQL.Append("    estimated_amt,");
                // estimate amount is changed by gopi
                strSQL.Append("    invoice_amt - estimated_amt diff_amt,");
                strSQL.Append("    vendor_mst_fk,");
                strSQL.Append("    cost_element_mst_fk,");
                strSQL.Append("    job_trn_pia.attached_file_name,'false' as \"Delete\", MJC_TRN_AIR_EXP_PIA_FK");
                strSQL.Append("FROM");
                strSQL.Append("    job_trn_air_exp_pia  job_trn_pia,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    cost_element_mst_tbl cost_ele,");
                strSQL.Append("    job_card_air_exp_tbl job_exp");
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_pia.job_card_air_exp_fk = job_exp.job_card_air_exp_pk");
                strSQL.Append("    AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk");
                strSQL.Append("    AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk");
                strSQL.Append("    AND job_exp.job_card_air_exp_pk =" + jobCardPK);
                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion " Fetch Purchase Inventory data export"

        #region " Fetch Cost details data export"

        /// <summary>
        /// Fetches the cost detail data exp.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="basecurrency">The basecurrency.</param>
        /// <returns></returns>
        public DataSet FetchCostDetailDataExp(string jobCardPK = "0", int basecurrency = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT JEC.JOB_TRN_AIR_EXP_COST_PK,");
                strSQL.Append("       JEC.JOB_CARD_AIR_EXP_FK,");
                strSQL.Append("       VMT.VENDOR_MST_PK,");
                strSQL.Append("       CMT.COST_ELEMENT_MST_PK,");
                strSQL.Append("       JEC.VENDOR_KEY,");
                strSQL.Append("       CMT.COST_ELEMENT_ID,");
                strSQL.Append("       CMT.COST_ELEMENT_NAME,");
                strSQL.Append("       DECODE(JEC.PTMT_TYPE,1,'Prepaid',2,'Collect')PTMT_TYPE,");
                strSQL.Append("       LMT.LOCATION_ID,");
                strSQL.Append("       CURR.CURRENCY_ID,");
                strSQL.Append("       JEC.ESTIMATED_COST,");
                strSQL.Append("       ROUND(GET_EX_RATE_BUY(JEC.CURRENCY_MST_FK, " + basecurrency + ", round(TO_DATE(JOB.JOBCARD_DATE,DATEFORMAT) - .5)), 6) AS ROE,");
                strSQL.Append("       JEC.TOTAL_COST,");
                strSQL.Append("       '' DEL_FLAG,");
                strSQL.Append("       'true' SEL_FLAG,");
                strSQL.Append("       JEC.LOCATION_MST_FK,");
                strSQL.Append("       JEC.CURRENCY_MST_FK");
                strSQL.Append("  FROM JOB_TRN_AIR_EXP_COST JEC,");
                strSQL.Append("       JOB_CARD_AIR_EXP_TBL  JOB,");
                strSQL.Append("       VENDOR_MST_TBL        VMT,");
                strSQL.Append("       COST_ELEMENT_MST_TBL  CMT,");
                strSQL.Append("       CURRENCY_TYPE_MST_TBL CURR,");
                strSQL.Append("       LOCATION_MST_TBL      LMT");
                strSQL.Append(" WHERE JEC.JOB_CARD_AIR_EXP_FK = JOB.JOB_CARD_AIR_EXP_PK");
                strSQL.Append("   AND JEC.COST_ELEMENT_MST_FK = CMT.COST_ELEMENT_MST_PK");
                strSQL.Append("   AND JEC.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
                strSQL.Append("   AND JEC.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                strSQL.Append("   AND JEC.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                strSQL.Append("   AND NVL(JEC.SERVICE_TYPE_FLAG,0)<>1");
                strSQL.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = " + jobCardPK);
                strSQL.Append("  ORDER BY CMT.PREFERENCE");
                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion " Fetch Cost details data export"

        #region " Fetch TP data export"

        /// <summary>
        /// Fetches the tp data exp.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet FetchTPDataExp(string jobCardPK = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    job_trn_tp.job_trn_air_exp_tp_pk,");
                strSQL.Append("    job_trn_tp.transhipment_no,");
                strSQL.Append("    job_trn_tp.port_mst_fk,");
                strSQL.Append("    port.port_id,");
                strSQL.Append("    port.port_name,");
                strSQL.Append("    job_trn_tp.airline_mst_fk,");
                strSQL.Append("    job_trn_tp.flight_no,");
                //strSQL.Append(vbCrLf & "    job_trn_tp.eta_date,")
                strSQL.Append("    TO_CHAR(job_trn_tp.eta_date,dateTimeFormat24) eta_date,");
                strSQL.Append("    TO_CHAR(job_trn_tp.etd_date,dateTimeFormat24) etd_date,");
                //strSQL.Append(vbCrLf & "    job_trn_tp.etd_date,")
                strSQL.Append("    'false' \"Delete\"");
                strSQL.Append("FROM");
                strSQL.Append("    job_trn_air_exp_tp  job_trn_tp,");
                strSQL.Append("    port_mst_tbl port,");
                strSQL.Append("    job_card_air_exp_tbl job_exp");
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_tp.job_card_air_exp_fk = job_exp.job_card_air_exp_pk");
                strSQL.Append("    AND job_trn_tp.port_mst_fk = port.port_mst_pk");
                strSQL.Append("    AND job_exp.job_card_air_exp_pk = " + jobCardPK);
                strSQL.Append("    ORDER BY transhipment_no");

                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion " Fetch TP data export"

        #region " Fetch base currency Exchange rate export"

        /// <summary>
        /// Fetches the roe.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchROE()
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    CURR.CURRENCY_MST_PK,");
                strSQL.Append("    CURR.CURRENCY_ID,");
                strSQL.Append("    ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK," + HttpContext.Current.Session["Currency_mst_pk"] + ",round(sysdate - .5)),6) AS ROE");
                strSQL.Append("FROM");
                strSQL.Append("    CURRENCY_TYPE_MST_TBL CURR");
                strSQL.Append("WHERE");
                strSQL.Append("    CURR.ACTIVE_FLAG = 1");
                //end
                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion " Fetch base currency Exchange rate export"

        #region " Fetch ROE"

        /// <summary>
        /// Fetch_s the roe.
        /// </summary>
        /// <param name="CurrencyPK">The currency pk.</param>
        /// <param name="ConversionDate">The conversion date.</param>
        /// <returns></returns>
        public DataSet Fetch_ROE(string CurrencyPK, string ConversionDate = "")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            if (string.IsNullOrEmpty(ConversionDate))
            {
                ConversionDate = " TO_DATE(SYSDATE,DATEFORMAT) ";
            }
            else
            {
                ConversionDate = " TO_DATE('" + ConversionDate + "',DATEFORMAT) ";
            }
            try
            {
                strSQL.Append("SELECT ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK," + HttpContext.Current.Session["currency_mst_pk"] + " , ");
                strSQL.Append(" round(" + ConversionDate + " - .5)),6) AS ROE, ");
                strSQL.Append(" ROUND(GET_EX_RATE_BUY(CURR.CURRENCY_MST_PK," + HttpContext.Current.Session["currency_mst_pk"] + " , ");
                strSQL.Append(" round(" + ConversionDate + " - .5)),6) AS ROE_BUY ");
                strSQL.Append(" FROM CURRENCY_TYPE_MST_TBL CURR ");
                strSQL.Append(" WHERE CURR.ACTIVE_FLAG = 1 ");
                strSQL.Append(" AND CURR.CURRENCY_MST_PK = '" + CurrencyPK + "' ");
                DataSet dsExchRt = new DataSet();
                dsExchRt = objWF.GetDataSet(strSQL.ToString());
                if (dsExchRt.Tables[0].Rows.Count == 0)
                {
                    DataRow dr = null;
                    dr = dsExchRt.Tables[0].NewRow();
                    dr[0] = 0;
                    dr[1] = 0;
                    dsExchRt.Tables[0].Rows.Add(dr);
                }
                return dsExchRt;
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

        #endregion " Fetch ROE"

        #region " Fetch ROE data export"

        /// <summary>
        /// Fetches the air line.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchAirLine()
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT ");
                strSQL.Append("    amt.airline_mst_pk,");
                strSQL.Append("    amt.airline_id,");
                strSQL.Append("    amt.airline_name");
                strSQL.Append("FROM");
                strSQL.Append("    airline_mst_tbl amt");
                strSQL.Append("WHERE");
                strSQL.Append("    amt.active_flag = 1 order by amt.airline_id asc ");

                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion " Fetch ROE data export"

        #region " Fetch ROE data export"

        /// <summary>
        /// Fetches the air FRT.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchAirFrt()
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append(" SELECT A.AIRFREIGHT_SLABS_TBL_PK, ");
                strSQL.Append("    A.BREAKPOINT_ID, ");
                strSQL.Append("    A.BREAKPOINT_DESC ");
                strSQL.Append("    FROM AIRFREIGHT_SLABS_TBL A ");
                strSQL.Append(" WHERE ");
                strSQL.Append("    A.ACTIVE_FLAG = 1 AND A.BASIS = 2   AND A.BREAKPOINT_TYPE = 2 ");

                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion " Fetch ROE data export"

        #region " Fetch Revenue data export"

        /// <summary>
        /// Fetches the revenue data.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet FetchRevenueData(string jobCardPK = "0")
        {
            WorkFlow objWF = new WorkFlow();

            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with54 = objWF.MyCommand.Parameters;
                _with54.Add("JOBCARD_PK", jobCardPK).Direction = ParameterDirection.Input;
                _with54.Add("JOB_AIR_EXP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_AIR_EXP");
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Fetch Revenue data export"

        #region "GenerateUCRNumber"

        /// <summary>
        /// Generates the ucr number.
        /// </summary>
        /// <param name="customerID">The customer identifier.</param>
        /// <returns></returns>
        public string GenerateUCRNumber(string customerID)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strSQL.Append("SELECT (SELECT TO_CHAR(SYSDATE,'yy')  FROM dual)||country.country_id||(SELECT substr(max(gst_no),1,15) FROM CORPORATE_MST_TBL)");
                strSQL.Append("    FROM");
                strSQL.Append("    customer_contact_dtls cust_det,");
                strSQL.Append("    customer_mst_tbl cmt,");
                strSQL.Append("    country_mst_tbl country,");
                strSQL.Append("    location_mst_tbl lmt");
                strSQL.Append("    WHERE");
                strSQL.Append("    cmt.customer_mst_pk =" + customerID);
                strSQL.Append("    AND cust_det.customer_mst_fk = cmt.customer_mst_pk(+)");
                strSQL.Append("    AND cust_det.adm_location_mst_fk = lmt.location_mst_pk(+)");
                strSQL.Append("    AND lmt.country_mst_fk = country.country_mst_pk(+)");

                return Convert.ToString(objWF.ExecuteScaler(strSQL.ToString()));
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

        #endregion "GenerateUCRNumber"

        #region "Fill Combo"

        /// <summary>
        /// Fills the cargo move code.
        /// </summary>
        /// <returns></returns>
        public DataSet FillCargoMoveCode()
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT");
            strSQL.Append("      c.cargo_move_pk,");
            strSQL.Append("      c.cargo_move_code");
            strSQL.Append(" FROM");
            strSQL.Append("      cargo_move_mst_tbl c");
            strSQL.Append(" WHERE");
            strSQL.Append("      c.active_flag = 1");
            strSQL.Append("      order by cargo_move_code");

            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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

        /// <summary>
        /// Fills the shipping terms combo.
        /// </summary>
        /// <returns></returns>
        public DataSet FillShippingTermsCombo()
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append("SELECT");
            strSQL.Append("      s.shipping_terms_mst_pk,");
            strSQL.Append("      s.inco_code ");
            strSQL.Append(" FROM");
            strSQL.Append("      shipping_terms_mst_tbl s ");
            strSQL.Append(" WHERE");
            strSQL.Append("      s.active_flag = 1 ");
            strSQL.Append("      order by inco_code ");

            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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

        /// <summary>
        /// Fills the currency combo.
        /// </summary>
        /// <returns></returns>
        public DataSet FillCurrencyCombo()
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append("select c.currency_mst_pk,c.currency_id from currency_type_mst_tbl c where c.active_flag =1 order by currency_id");
            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion "Fill Combo"

        #region "GetRevenueDetails"

        /// <summary>
        /// Gets the revenue details.
        /// </summary>
        /// <param name="actualCost">The actual cost.</param>
        /// <param name="actualRevenue">The actual revenue.</param>
        /// <param name="estimatedCost">The estimated cost.</param>
        /// <param name="estimatedRevenue">The estimated revenue.</param>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="LocationPk">The location pk.</param>
        /// <returns></returns>
        public DataSet GetRevenueDetails(decimal actualCost, decimal actualRevenue, decimal estimatedCost, decimal estimatedRevenue, string jobCardPK, int LocationPk)
        {
            //Dim SQL As New System.Text.StringBuilder
            WorkFlow objWF = new WorkFlow();
            //Snigdharani - 10/11/2008 - making the values same as consolidation screen
            try
            {
                DataSet DS = new DataSet();
                var _with55 = objWF.MyCommand.Parameters;
                _with55.Add("JCPK", jobCardPK).Direction = ParameterDirection.Input;
                //adding by thiyagarajan on 24/11/08 for location based currency task
                _with55.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with55.Add("JOB_EXP_AIR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_COST_REVENUE_PROFIT", "FETCH_JOBCARD_EXP_AIR");
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
            //End Snigdharani
            //Commented by Snigdharani - 10/11/2008
            //Dim actualEstimatedCost As String
            //Dim costArray As String()
            //Dim temporary As String
            //Dim oraReader As OracleDataReader

            //SQL.Append(vbCrLf & "SELECT")
            //SQL.Append(vbCrLf & "      sum(ROUND(q.EstimatedCost * ")
            //SQL.Append(vbCrLf & "      (case when ")
            //SQL.Append(vbCrLf & "                           q.currency_mst_pk = corp.currency_mst_fk")
            //SQL.Append(vbCrLf & "            then")
            //SQL.Append(vbCrLf & "                           1")
            //SQL.Append(vbCrLf & "            else")
            //SQL.Append(vbCrLf & "                           (select")
            //SQL.Append(vbCrLf & "                                   exch.exchange_rate")
            //SQL.Append(vbCrLf & "                            from")
            //SQL.Append(vbCrLf & "                                   exchange_rate_trn exch")
            //SQL.Append(vbCrLf & "                            where")
            //SQL.Append(vbCrLf & "                                   q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))")
            //SQL.Append(vbCrLf & "                                   AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ")
            //SQL.Append(vbCrLf & "                            )end")
            //SQL.Append(vbCrLf & "         ),4))""Estimated Cost"", ")
            //SQL.Append(vbCrLf & "      sum(ROUND(ActualCost * ")
            //SQL.Append(vbCrLf & "      (case when ")
            //SQL.Append(vbCrLf & "                            q.currency_mst_pk = corp.currency_mst_fk")
            //SQL.Append(vbCrLf & "           then")
            //SQL.Append(vbCrLf & "                            1")
            //SQL.Append(vbCrLf & "           else")
            //SQL.Append(vbCrLf & "                            (select")
            //SQL.Append(vbCrLf & "                                    exch.exchange_rate")
            //SQL.Append(vbCrLf & "                             from")
            //SQL.Append(vbCrLf & "                                    exchange_rate_trn exch")
            //SQL.Append(vbCrLf & "                             where")
            //SQL.Append(vbCrLf & "                                    q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))")
            //SQL.Append(vbCrLf & "                                    AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ")
            //SQL.Append(vbCrLf & "                            )end ")
            //SQL.Append(vbCrLf & "    ),4)) ""Actual Cost"" ")
            //SQL.Append(vbCrLf & "FROM")
            //SQL.Append(vbCrLf & "    (SELECT")
            //SQL.Append(vbCrLf & "           job_exp.jobcard_date,")
            //SQL.Append(vbCrLf & "           curr.currency_mst_pk,")
            //SQL.Append(vbCrLf & "           SUM(job_trn_pia.Estimated_Amt) EstimatedCost,")
            //SQL.Append(vbCrLf & "           SUM(job_trn_pia.Invoice_Amt) ActualCost")
            //SQL.Append(vbCrLf & "     FROM")
            //SQL.Append(vbCrLf & "           job_trn_air_exp_pia  job_trn_pia,")
            //SQL.Append(vbCrLf & "           currency_type_mst_tbl curr,")
            //SQL.Append(vbCrLf & "           cost_element_mst_tbl cost_ele,")
            //SQL.Append(vbCrLf & "           job_card_air_exp_tbl job_exp")
            //SQL.Append(vbCrLf & "     WHERE")
            //SQL.Append(vbCrLf & "           job_trn_pia.job_card_air_exp_fk = job_exp.job_card_air_exp_pk")
            //SQL.Append(vbCrLf & "           AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk")
            //SQL.Append(vbCrLf & "           AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk")
            //SQL.Append(vbCrLf & "           AND job_exp.job_card_air_exp_pk =" + jobCardPK)

            //'by Thiyagarajan on 1/4/08 for location based currency : PTS TASK GEN-FEB-003
            //'SQL.Append(vbCrLf & "     GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp")
            //SQL.Append(vbCrLf & "     GROUP BY jobcard_date,currency_mst_pk)q,")
            //SQL.Append(vbCrLf & "  (select country.currency_mst_fk from country_mst_tbl country where country.country_mst_pk in (select loc.country_mst_fk from ")
            //SQL.Append(vbCrLf & "  location_mst_tbl loc where loc.location_mst_pk=" & LocationPk & ")) corp ")
            //'end

            //oraReader = objWF.GetDataReader(SQL.ToString())

            //While oraReader.Read
            //    If Not (oraReader(0) Is "") Then
            //        estimatedCost = oraReader(0)
            //    End If

            //    If Not (oraReader(1) Is "") Then
            //        actualCost = oraReader(1)
            //    End If
            //End While

            //oraReader.Close()

            //SQL = New System.Text.StringBuilder

            //SQL.Append(vbCrLf & "SELECT  ")
            //SQL.Append(vbCrLf & "   sum(ROUND(q.freight_amt * ")
            //SQL.Append(vbCrLf & "   (case when ")
            //SQL.Append(vbCrLf & "           q.currency_mst_pk = corp.currency_mst_fk")
            //SQL.Append(vbCrLf & "         then ")
            //SQL.Append(vbCrLf & "           1")
            //SQL.Append(vbCrLf & "         else ")
            //SQL.Append(vbCrLf & "           (select ")
            //SQL.Append(vbCrLf & "               exch.exchange_rate")
            //SQL.Append(vbCrLf & "            from")
            //SQL.Append(vbCrLf & "               exchange_rate_trn exch ")
            //SQL.Append(vbCrLf & "            where")
            //SQL.Append(vbCrLf & "               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))")
            //SQL.Append(vbCrLf & "               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ")
            //SQL.Append(vbCrLf & "           )end ")
            //SQL.Append(vbCrLf & "   ),4)) ""Estimated Revenue""")
            //SQL.Append(vbCrLf & "FROM")
            //SQL.Append(vbCrLf & "   (SELECT")
            //SQL.Append(vbCrLf & "       job_exp.jobcard_date,")
            //SQL.Append(vbCrLf & "       curr.currency_mst_pk,")
            //SQL.Append(vbCrLf & "       sum(job_trn_fd.freight_amt) freight_amt")
            //SQL.Append(vbCrLf & "    FROM")
            //SQL.Append(vbCrLf & "       job_trn_air_exp_fd  job_trn_fd,")
            //SQL.Append(vbCrLf & "       currency_type_mst_tbl curr,")
            //SQL.Append(vbCrLf & "       job_card_air_exp_tbl job_exp")
            //SQL.Append(vbCrLf & "    WHERE")
            //SQL.Append(vbCrLf & "       job_trn_fd.job_card_air_exp_fk = job_exp.job_card_air_exp_pk")
            //SQL.Append(vbCrLf & "       AND job_trn_fd.currency_mst_fk =curr.currency_mst_pk")
            //SQL.Append(vbCrLf & "       AND job_exp.job_card_air_exp_pk =" + jobCardPK)

            //'by Thiyagarajan on 1/4/08 for location based currency : PTS TASK GEN-FEB-003
            //'SQL.Append(vbCrLf & "     GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp")
            //SQL.Append(vbCrLf & "     GROUP BY jobcard_date,currency_mst_pk)q,")
            //SQL.Append(vbCrLf & "  (select country.currency_mst_fk from country_mst_tbl country where country.country_mst_pk in (select loc.country_mst_fk from ")
            //SQL.Append(vbCrLf & "  location_mst_tbl loc where loc.location_mst_pk=" & LocationPk & ")) corp ")
            //'end

            //oraReader = objWF.GetDataReader(SQL.ToString())
            //While oraReader.Read
            //    If Not (oraReader(0) Is "") Then
            //        estimatedRevenue = oraReader(0)
            //    End If
            //End While
            //oraReader.Close()

            //SQL = New System.Text.StringBuilder

            //SQL.Append(vbCrLf & "SELECT  ")
            //SQL.Append(vbCrLf & "   sum(ROUND(q.freight_amt * ")
            //SQL.Append(vbCrLf & "   (case when ")
            //SQL.Append(vbCrLf & "           q.currency_mst_pk = corp.currency_mst_fk")
            //SQL.Append(vbCrLf & "         then ")
            //SQL.Append(vbCrLf & "           1")
            //SQL.Append(vbCrLf & "         else ")
            //SQL.Append(vbCrLf & "           (select ")
            //SQL.Append(vbCrLf & "               exch.exchange_rate")
            //SQL.Append(vbCrLf & "            from")
            //SQL.Append(vbCrLf & "               exchange_rate_trn exch")
            //SQL.Append(vbCrLf & "            where")
            //SQL.Append(vbCrLf & "               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))")
            //SQL.Append(vbCrLf & "               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ")
            //SQL.Append(vbCrLf & "           )end ")
            //SQL.Append(vbCrLf & "   ),4)) ""Estimated Revenue""")
            //SQL.Append(vbCrLf & "FROM")
            //SQL.Append(vbCrLf & "   (SELECT")
            //SQL.Append(vbCrLf & "       job_exp.jobcard_date,")
            //SQL.Append(vbCrLf & "       curr.currency_mst_pk,")
            //SQL.Append(vbCrLf & "       sum(job_trn_othr.amount) freight_amt")
            //SQL.Append(vbCrLf & "    FROM")
            //SQL.Append(vbCrLf & "       job_trn_air_exp_oth_chrg  job_trn_othr,")
            //SQL.Append(vbCrLf & "       currency_type_mst_tbl curr,")
            //SQL.Append(vbCrLf & "       job_card_air_exp_tbl job_exp")
            //SQL.Append(vbCrLf & "    WHERE")
            //SQL.Append(vbCrLf & "       job_trn_othr.job_card_air_exp_fk = job_exp.job_card_air_exp_pk")
            //SQL.Append(vbCrLf & "       AND job_trn_othr.currency_mst_fk =curr.currency_mst_pk")
            //SQL.Append(vbCrLf & "       AND job_exp.job_card_air_exp_pk =" + jobCardPK)

            //'by Thiyagarajan on 1/4/08 for location based currency : PTS TASK GEN-FEB-003
            //'SQL.Append(vbCrLf & "     GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp")
            //SQL.Append(vbCrLf & "     GROUP BY jobcard_date,currency_mst_pk)q,")
            //SQL.Append(vbCrLf & "  (select country.currency_mst_fk from country_mst_tbl country where country.country_mst_pk in (select loc.country_mst_fk from ")
            //SQL.Append(vbCrLf & "  location_mst_tbl loc where loc.location_mst_pk=" & LocationPk & ")) corp ")
            //'end

            //oraReader = objWF.GetDataReader(SQL.ToString())
            //While oraReader.Read
            //    If Not (oraReader(0) Is "") Then
            //        estimatedRevenue = estimatedRevenue + oraReader(0)
            //    End If
            //End While
            //oraReader.Close()

            //'temporary = objWF.ExecuteScaler(SQL.ToString)
            //'If temporary <> "" Then
            //'    estimatedRevenue = CDec(temporary)
            //'End If

            //'SQL = New System.Text.StringBuilder

            //'SQL.Append(vbCrLf & "SELECT  ")
            //'SQL.Append(vbCrLf & "   sum(ROUND(q.actual_revenue * ")
            //'SQL.Append(vbCrLf & "   (case when")
            //'SQL.Append(vbCrLf & "           q.currency_mst_pk = corp.currency_mst_fk")
            //'SQL.Append(vbCrLf & "    then")
            //'SQL.Append(vbCrLf & "           1")
            //'SQL.Append(vbCrLf & "    else")
            //'SQL.Append(vbCrLf & "           (select")
            //'SQL.Append(vbCrLf & "                  exch.exchange_rate")
            //'SQL.Append(vbCrLf & "            from")
            //'SQL.Append(vbCrLf & "                  exchange_rate_trn exch")
            //'SQL.Append(vbCrLf & "            where")
            //'SQL.Append(vbCrLf & "                  q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))")
            //'SQL.Append(vbCrLf & "                  AND q.currency_mst_pk = exch.currency_mst_fk")
            //'SQL.Append(vbCrLf & "           )end ")
            //'SQL.Append(vbCrLf & "   ),4)) ""Actual Revenue""")
            //'SQL.Append(vbCrLf & "FROM")
            //'SQL.Append(vbCrLf & "   (SELECT")
            //'SQL.Append(vbCrLf & "       job_exp.jobcard_date,")
            //'SQL.Append(vbCrLf & "       curr.currency_mst_pk,")
            //'SQL.Append(vbCrLf & "       sum (nvl(inv_cust.invoice_amt,0) + nvl(inv_cust.vat_amt,0) - nvl(inv_cust.discount_amt,0) ) actual_revenue")
            //'SQL.Append(vbCrLf & "   FROM")
            //'SQL.Append(vbCrLf & "       inv_cust_air_exp_tbl inv_cust,")
            //'SQL.Append(vbCrLf & "       currency_type_mst_tbl curr,")
            //'SQL.Append(vbCrLf & "       job_card_air_exp_tbl job_exp")
            //'SQL.Append(vbCrLf & "   WHERE")
            //'SQL.Append(vbCrLf & "       inv_cust.job_card_air_exp_fk = job_exp.job_card_air_exp_pk")
            //'SQL.Append(vbCrLf & "       AND inv_cust.currency_mst_fk =curr.currency_mst_pk")
            //'SQL.Append(vbCrLf & "       AND job_exp.job_card_air_exp_pk =" + jobCardPK)
            //'SQL.Append(vbCrLf & "   GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp")

            //'oraReader = objWF.GetDataReader(SQL.ToString())
            //'While oraReader.Read
            //'    If Not (oraReader(0) Is "") Then
            //'        actualRevenue = oraReader(0)
            //'    End If
            //'End While
            //'oraReader.Close()

            ///temporary = objWF.ExecuteScaler(SQL.ToString)
            ///If temporary <> "" Then
            ///    actualRevenue = CDec(temporary)
            ///End If
            //Try
            //    objWF.MyCommand.Parameters.Clear()
            //    With objWF.MyCommand.Parameters
            //        .Add("JOBCARD_PK", jobCardPK).Direction = ParameterDirection.Input
            //        '.Add("LocationsPk", LocationPk).Direction = ParameterDirection.Input 'adding by Thiyagarajan for location based currency
            //        'Commented By Anand Reason:Location Based Currency Is Not Moved to Eqa
            //        .Add("JOB_AIR_EXP_CUR", OracleClient.OracleDbType.RefCursor).Direction = ParameterDirection.Output
            //    End With
            //Catch sqlExp As Exception
            //    Throw sqlExp
            //End Try

            //oraReader = objWF.GetDataReader("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_AIR_EXP_ACTREV")
            //While oraReader.Read
            //    If Not (oraReader(0) Is "") Then
            //        actualRevenue = oraReader(0)
            //    End If
            //End While
            //oraReader.Close()
        }

        /// <summary>
        /// Fills the booking other charges data set.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public DataSet FillBookingOtherChargesDataSet(string pk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append("SELECT");
            strSQL.Append("         '' job_trn_air_exp_oth_pk,");
            strSQL.Append("         frt.freight_element_mst_pk,");
            strSQL.Append("         frt.freight_element_id,");
            strSQL.Append("         frt.freight_element_name,");
            strSQL.Append("         curr.currency_mst_pk, '' \"ROE\",");
            strSQL.Append("         oth_chrg.amount amount,");
            strSQL.Append("         'false' \"Delete\", 1 \"Print\"");
            strSQL.Append("FROM");
            strSQL.Append("         booking_trn_air_oth_chrg oth_chrg,");
            strSQL.Append("         booking_air_tbl  booking_mst,");
            strSQL.Append("         freight_element_mst_tbl frt,");
            strSQL.Append("         currency_type_mst_tbl curr");
            strSQL.Append("WHERE");
            strSQL.Append("         oth_chrg.booking_trn_air_fk = booking_mst.booking_air_pk");
            strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.booking_trn_air_fk         = " + pk);
            strSQL.Append("ORDER BY freight_element_id ");

            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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

        //added by manoharan 4/11/2006
        /// <summary>
        /// Gets the oth ch.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public DataSet GetOthCh(string pk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append("         SELECT");
            strSQL.Append("         oth_chrg.inv_cust_trn_air_exp_fk,");
            strSQL.Append("         oth_chrg.inv_agent_trn_air_exp_fk,");
            strSQL.Append("         oth_chrg.consol_invoice_trn_fk");

            strSQL.Append("FROM");
            strSQL.Append("         job_trn_air_exp_oth_chrg oth_chrg,");
            strSQL.Append("         job_card_air_exp_tbl jobcard_mst,");
            strSQL.Append("         freight_element_mst_tbl frt,");
            strSQL.Append("         currency_type_mst_tbl curr");
            strSQL.Append("WHERE");
            strSQL.Append("         oth_chrg.job_card_air_exp_fk = jobcard_mst.job_card_air_exp_pk");
            strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.job_card_air_exp_fk    = " + pk);
            strSQL.Append("ORDER BY freight_element_id ");

            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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

        /// <summary>
        /// Fills the job card other charges data set.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <param name="basecurrency">The basecurrency.</param>
        /// <returns></returns>
        public DataSet FillJobCardOtherChargesDataSet(string pk = "0", int basecurrency = 0)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append("         SELECT");
            strSQL.Append("         oth_chrg.job_trn_air_exp_oth_pk,");
            strSQL.Append("         frt.freight_element_mst_pk,");
            strSQL.Append("         frt.freight_element_id,");
            strSQL.Append("         frt.freight_element_name,");
            strSQL.Append("         DECODE(oth_chrg.freight_type,1,'Prepaid',2,'Collect') Payment_Type, ");
            // By Amit on 27-April-2007
            //To introduced LOCATION & FREIGHT PAYER Column
            //By Amit Singh on 11-May-07
            strSQL.Append("         oth_chrg.location_mst_fk,");
            //strSQL.Append(vbCrLf & "         (CASE WHEN  oth_chrg.freight_type=1 THEN lmt.location_id ELSE pmt.port_id END) ""location_id"",")
            strSQL.Append("         lmt.location_id ,");
            strSQL.Append("         oth_chrg.frtpayer_cust_mst_fk,");
            strSQL.Append("         cmt.customer_id,");
            //End
            strSQL.Append("         curr.currency_mst_pk,");
            if (basecurrency != 0)
            {
                strSQL.Append("    ROUND(GET_EX_RATE(oth_chrg.currency_mst_fk," + basecurrency + ",round(sysdate - .5)),4) AS ROE ,");
            }
            else
            {
                strSQL.Append("  oth_chrg.EXCHANGE_RATE \"ROE\",");
            }

            strSQL.Append("         oth_chrg.amount amount,");
            strSQL.Append("         'false' \"Delete\", oth_chrg.PRINT_ON_MAWB \"Print\"");
            strSQL.Append("FROM");
            strSQL.Append("         job_trn_air_exp_oth_chrg oth_chrg,");
            strSQL.Append("         job_card_air_exp_tbl jobcard_mst,");
            strSQL.Append("         freight_element_mst_tbl frt,");
            strSQL.Append("         currency_type_mst_tbl curr,");
            //To introduced LOCATION & FREIGHT PAYER Column
            //By Amit Singh on 11-May-07
            strSQL.Append("         location_mst_tbl lmt,");
            //strSQL.Append(vbCrLf & "         port_mst_tbl pmt,")
            strSQL.Append("         customer_mst_tbl cmt");
            //End
            strSQL.Append("WHERE");
            strSQL.Append("         oth_chrg.job_card_air_exp_fk = jobcard_mst.job_card_air_exp_pk");
            strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.job_card_air_exp_fk    = " + pk);
            //To introduced LOCATION & FREIGHT PAYER Column
            //By Amit Singh on 11-May-07
            strSQL.Append("         AND oth_chrg.location_mst_fk = lmt.location_mst_pk (+)");
            // strSQL.Append(vbCrLf & "         AND oth_chrg.location_mst_fk = pmt.port_mst_pk (+)")
            strSQL.Append("         AND oth_chrg.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+)");
            //End
            strSQL.Append("ORDER BY freight_element_id ");

            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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

        #endregion "GetRevenueDetails"

        //.....Chandra Added for Reports...........

        #region "Fetch Export Acknowledgement Details"

        /// <summary>
        /// Fetches the air acknowledgement.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchAirAcknowledgement(string JOBPK)
        {
            string Strsql = null;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = "SELECT JAE.JOB_CARD_TRN_PK JOBPK,";
                Strsql += "JAE.JOBCARD_REF_NO JOBREFNO,";
                Strsql += "BAT.BOOKING_MST_PK BKGPK,";
                Strsql += "BAT.BOOKING_REF_NO BKGREFNO,";
                Strsql += "BAT.BOOKING_DATE BKGDATE,";
                Strsql += "JAE.VOYAGE_FLIGHT_NO VESFLIGHT,";
                Strsql += "HAWB.HAWB_REF_NO HBLREFNO,";
                Strsql += " MAWB.MAWB_REF_NO MBLREFNO,";
                Strsql += "JAE.MARKS_NUMBERS MARKS,";
                Strsql += "JAE.GOODS_DESCRIPTION GOODS,";
                Strsql += " 2 CARGO_TYPE ,";
                Strsql += "JAE.UCR_NO UCRNO,";
                Strsql += "'' CLEARANCEPOINT,";
                Strsql += "JAE.ETD_DATE ETD,";
                Strsql += "JAE.SHIPPER_CUST_MST_FK,";
                Strsql += "CMST.CUSTOMER_NAME SHIPNAME,";
                Strsql += "BAT.CUSTOMER_REF_NO SHIPREFNO,";
                Strsql += "CDTLS.ADM_ADDRESS_1 SHIPADD1,";
                Strsql += "CDTLS.ADM_ADDRESS_2 SHIPADD2,";
                Strsql += "CDTLS.ADM_ADDRESS_3 SHIPADD3,";
                Strsql += "CDTLS.ADM_CITY SHIPCITY,";
                Strsql += "CDTLS.ADM_ZIP_CODE SHIPZIP,";
                Strsql += "CDTLS.ADM_EMAIL_ID AS SHIPEMAIL,";
                Strsql += "CDTLS.ADM_PHONE_NO_1 SHIPPHONE,";
                Strsql += "CDTLS.ADM_FAX_NO SHIPFAX,";
                Strsql += "SHICOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
                Strsql += "CONSIGNEE.CUSTOMER_NAME CONSIGNEENAME,";
                Strsql += "CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGADD1,";
                Strsql += "CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGADD2,";
                Strsql += "CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGADD3,";
                Strsql += "CONSIGNEEDTLS.ADM_CITY CONSIGCITY,";
                Strsql += "CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGZIP,";
                Strsql += "CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGEMAIL,";
                Strsql += "CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGPHONE,";
                Strsql += "CONSIGNEEDTLS.ADM_FAX_NO CONSIGFAX,";
                Strsql += " CONSIGCOUNTRY.COUNTRY_NAME CONSICOUNTRY,";
                Strsql += "POL.PORT_NAME POLNAME,";
                Strsql += "POD.PORT_NAME PODNAME,";
                Strsql += "PLD.PLACE_NAME DELNAME,";
                Strsql += "COLPLD.PLACE_NAME COLNAME,";
                Strsql += "DBAMST.AGENT_MST_PK DBAGENTPK,";
                Strsql += "DBAMST.AGENT_NAME  DBAGENTNAME,";
                Strsql += "DBADTLS.ADM_ADDRESS_1  DBAGENTADD1,";
                Strsql += "DBADTLS.ADM_ADDRESS_2  DBAGENTADD2,";
                Strsql += "DBADTLS.ADM_ADDRESS_3  DBAGENTADD3,";
                Strsql += "DBADTLS.ADM_CITY  DBAGENTCITY,";
                Strsql += "DBADTLS.ADM_ZIP_CODE DBAGENTZIP,";
                Strsql += "DBADTLS.ADM_EMAIL_ID DBAGENTEMAIL,";
                Strsql += "DBADTLS.ADM_PHONE_NO_1 DBAGENTPHONE,";
                Strsql += "DBADTLS.ADM_FAX_NO DBAGENTFAX,";
                Strsql += "DBCOUNTRY.COUNTRY_NAME DBCOUNTRY,";
                Strsql += "STMST.INCO_CODE TERMS,";
                Strsql += "NVL(JAE.INSURANCE_AMT,0) INSURANCE,";
                Strsql += "JAE.PYMT_TYPE ,";
                Strsql += "CGMST.commodity_group_desc COMMCODE,";
                Strsql += "JAE.ETA_DATE ETA,";
                Strsql += "SUM(JTAEC.GROSS_WEIGHT) GROSS,";
                Strsql += "SUM(JTAEC.CHARGEABLE_WEIGHT) CHARWT,";
                Strsql += "'' NETWT,";
                Strsql += "SUM(JTAEC.VOLUME_IN_CBM) VOLUME";
                Strsql += "FROM   JOB_CARD_TRN JAE,";
                //Strsql &= vbCrLf & "JOB_TRN_AIR_EXP_TP JTAEP,"
                Strsql += "JOB_TRN_CONT JTAEC,";
                Strsql += "BOOKING_MST_TBL BAT,";
                Strsql += "CUSTOMER_MST_TBL CMST,";
                Strsql += "CUSTOMER_CONTACT_DTLS CDTLS,";
                Strsql += "CUSTOMER_MST_TBL CONSIGNEE,";
                Strsql += "CUSTOMER_CONTACT_DTLS CONSIGNEEDTLS,";
                Strsql += "COUNTRY_MST_TBL SHICOUNTRY,";
                Strsql += "COUNTRY_MST_TBL CONSIGCOUNTRY,";
                Strsql += "PORT_MST_TBL POL,";
                Strsql += "PORT_MST_TBL POD,";
                Strsql += "PLACE_MST_TBL PLD,";
                Strsql += "PLACE_MST_TBL COLPLD,";
                Strsql += "AGENT_MST_TBL DBAMST,";
                Strsql += "AGENT_CONTACT_DTLS DBADTLS,";
                Strsql += "COUNTRY_MST_TBL DBCOUNTRY,";
                Strsql += "SHIPPING_TERMS_MST_TBL STMST,";
                Strsql += "COMMODITY_GROUP_MST_TBL CGMST,";
                Strsql += "HAWB_EXP_TBL HAWB,";
                Strsql += "MAWB_EXP_TBL MAWB";
                Strsql += "WHERE JAE.JOB_CARD_TRN_PK IN(" + JOBPK + " )";
                //Strsql &= vbCrLf & "AND JTAEP.JOB_CARD_TRN_FK(+)=JAE.JOB_CARD_TRN_PK"
                //Strsql &= vbCrLf & "AND  nvl(JTAEP.ETA_DATE,sysdate)=(SELECT  nvl(MAX(JTT.ETA_DATE),sysdate) FROM JOB_TRN_AIR_EXP_TP JTT WHERE JTT.JOB_CARD_TRN_FK=JTAEP.JOB_CARD_TRN_FK)"
                Strsql += " AND JTAEC.JOB_CARD_TRN_FK(+)=JAE.JOB_CARD_TRN_PK";
                Strsql += "AND JAE.HBL_HAWB_FK=HAWB.HAWB_EXP_TBL_PK(+)";
                Strsql += "AND JAE.MBL_MAWB_FK=MAWB.MAWB_EXP_TBL_PK(+)";
                Strsql += "AND  CMST.CUSTOMER_MST_PK(+)=JAE.SHIPPER_CUST_MST_FK";
                Strsql += "AND  CDTLS.CUSTOMER_MST_FK(+)=CMST.CUSTOMER_MST_PK";
                Strsql += "AND CONSIGNEE.CUSTOMER_MST_PK(+)=JAE.CONSIGNEE_CUST_MST_FK";
                Strsql += "AND  CONSIGNEEDTLS.CUSTOMER_MST_FK(+)=CONSIGNEE.CUSTOMER_MST_PK";
                Strsql += "AND  JAE.BOOKING_MST_FK=BAT.BOOKING_MST_PK(+)";
                Strsql += "AND  BAT.PORT_MST_POL_FK=POL.PORT_MST_PK(+)";
                Strsql += "AND  BAT.PORT_MST_POD_FK=POD.PORT_MST_PK(+)";
                Strsql += "AND  BAT.DEL_PLACE_MST_FK=PLD.PLACE_PK(+)";
                Strsql += "AND   BAT.COL_PLACE_MST_FK=COLPLD.PLACE_PK(+)";
                Strsql += "AND  JAE.DP_AGENT_MST_FK=DBAMST.AGENT_MST_PK(+)";
                Strsql += "AND  DBAMST.AGENT_MST_PK=DBADTLS.AGENT_MST_FK(+)";
                Strsql += "AND DBCOUNTRY.COUNTRY_MST_PK(+)=DBADTLS.ADM_COUNTRY_MST_FK";
                Strsql += "AND CONSIGCOUNTRY.COUNTRY_MST_PK(+)=CONSIGNEE.COUNTRY_MST_FK";
                Strsql += "AND SHICOUNTRY.COUNTRY_MST_PK(+)=CMST.COUNTRY_MST_FK";
                Strsql += "AND  STMST.SHIPPING_TERMS_MST_PK(+)=JAE.SHIPPING_TERMS_MST_FK";
                Strsql += "AND  JAE.COMMODITY_GROUP_FK=CGMST.COMMODITY_GROUP_PK(+)";
                Strsql += "GROUP BY";
                Strsql += "JAE.JOB_CARD_TRN_PK ,";
                Strsql += "JAE.JOBCARD_REF_NO ,";
                Strsql += "BAT.BOOKING_MST_PK ,";
                Strsql += "BAT.BOOKING_REF_NO ,";
                Strsql += "BAT.BOOKING_DATE ,";
                Strsql += "JAE.VOYAGE_FLIGHT_NO ,";
                Strsql += " HAWB.HAWB_REF_NO,";
                Strsql += " MAWB.MAWB_REF_NO ,";
                Strsql += "JAE.MARKS_NUMBERS,";
                Strsql += "JAE.GOODS_DESCRIPTION,";
                Strsql += "JAE.UCR_NO ,";
                Strsql += "JAE.ETD_DATE ,";
                Strsql += "JAE.SHIPPER_CUST_MST_FK,";
                Strsql += "CMST.CUSTOMER_NAME ,";
                Strsql += "BAT.CUSTOMER_REF_NO,";
                Strsql += "CDTLS.ADM_ADDRESS_1,";
                Strsql += "CDTLS.ADM_ADDRESS_2 ,";
                Strsql += "CDTLS.ADM_ADDRESS_3 ,";
                Strsql += "CDTLS.ADM_CITY ,";
                Strsql += "CDTLS.ADM_ZIP_CODE,";
                Strsql += "CDTLS.ADM_EMAIL_ID,";
                Strsql += "CDTLS.ADM_PHONE_NO_1 ,";
                Strsql += "CDTLS.ADM_FAX_NO ,";
                Strsql += "SHICOUNTRY.COUNTRY_NAME ,";
                Strsql += "CONSIGNEE.CUSTOMER_NAME ,";
                Strsql += "CONSIGNEEDTLS.ADM_ADDRESS_1 ,";
                Strsql += "CONSIGNEEDTLS.ADM_ADDRESS_2,";
                Strsql += "CONSIGNEEDTLS.ADM_ADDRESS_3 ,";
                Strsql += "CONSIGNEEDTLS.ADM_CITY,";
                Strsql += "CONSIGNEEDTLS.ADM_ZIP_CODE,";
                Strsql += "CONSIGNEEDTLS.ADM_EMAIL_ID,";
                Strsql += "CONSIGNEEDTLS.ADM_PHONE_NO_1,";
                Strsql += "CONSIGNEEDTLS.ADM_FAX_NO ,";
                Strsql += "CONSIGCOUNTRY.COUNTRY_NAME ,";
                Strsql += "POL.PORT_NAME , 2,";
                Strsql += "POD.PORT_NAME ,";
                Strsql += "PLD.PLACE_NAME,";
                Strsql += "COLPLD.PLACE_NAME,";
                Strsql += "DBAMST.AGENT_MST_PK ,";
                Strsql += "DBAMST.AGENT_NAME ,";
                Strsql += "DBADTLS.ADM_ADDRESS_1,";
                Strsql += "DBADTLS.ADM_ADDRESS_2  ,";
                Strsql += "DBADTLS.ADM_ADDRESS_3 ,";
                Strsql += "DBADTLS.ADM_CITY ,";
                Strsql += "DBADTLS.ADM_ZIP_CODE ,";
                Strsql += "DBADTLS.ADM_EMAIL_ID,";
                Strsql += "DBADTLS.ADM_PHONE_NO_1 ,";
                Strsql += "DBADTLS.ADM_FAX_NO,";
                Strsql += "DBCOUNTRY.COUNTRY_NAME,";
                Strsql += "STMST.INCO_CODE,";
                Strsql += " NVL(JAE.INSURANCE_AMT,0),";
                Strsql += " JAE.PYMT_TYPE ,";
                Strsql += " CGMST.commodity_group_desc,JAE.ETA_DATE";

                return ObjWF.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the sea containers.
        /// </summary>
        /// <param name="JobRefPK">The job reference pk.</param>
        /// <returns></returns>
        public DataSet FetchSeaContainers(string JobRefPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT JAE.JOB_CARD_TRN_FK JOBPK ,JAE.PALETTE_SIZE CONTAINER FROM JOB_TRN_CONT JAE ";
            Strsql += "WHERE JAE.JOB_CARD_TRN_FK IN(" + JobRefPK + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Export Acknowledgement Details"

        #region "Header Document Report"

        /// <summary>
        /// Fetches the sea imp header docment.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchSeaImpHeaderDocment(int JOBPK)
        {
            string Strsql = null;
            WorkFlow ObjWF = new WorkFlow();
            Strsql = "select JobAirExp.Job_Card_Air_Exp_Pk Jobpk," + "JobAirExp.Jobcard_Ref_No JOBNoREFNO," + "JobAirExp.Ucr_No UCRNO," + " HAWB.HAWB_REF_NO HBLREFNO," + " MAWB.MAWB_REF_NO  MBLREFNO, " + "JobAirExp.Flight_No Ves_Flight, " + "CustMstShipper.Customer_Name Shipper," + "CustShipperDtls.Adm_Address_1 ShiAddress1," + "CustShipperDtls.Adm_Address_2 ShiAddress2, " + "CustShipperDtls.Adm_Address_3 ShiAddress3, " + "CustShipperDtls.Adm_City ShiCity," + "CUSTSHIPPERDTLS.ADM_ZIP_CODE SHIZIP," + "CUSTSHIPPERDTLS.ADM_PHONE_NO_1 SHIPHONE," + "CUSTSHIPPERDTLS.ADM_FAX_NO  SHIPFAX," + "CUSTSHIPPERDTLS.ADM_EMAIL_ID SHIPEMAIL," + "SHIPCOUNTRY.COUNTRY_NAME SHICOUNTRY," + "CustMstConsignee.Customer_Name Consignee," + "CustConsigneeDtls.Adm_Address_1 ConsiAddress1," + "CustConsigneeDtls.Adm_Address_2 ConsiAddress2," + "CustConsigneeDtls.Adm_Address_3 ConsiAddress3," + "CustConsigneeDtls.Adm_City ConsiCity," + "CUSTCONSIGNEEDTLS.ADM_ZIP_CODE CONSIZIP," + "CUSTCONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIPHONE," + "CUSTCONSIGNEEDTLS.ADM_FAX_NO CONSIFAX," + "CUSTCONSIGNEEDTLS.ADM_EMAIL_ID CONSIEMAIL," + "CONSIGCOUNTRY.COUNTRY_NAME CONSICOUNTRY," + "AgtMst.Agent_Name," + "AgtCntDtls.Adm_Address_1 AgtAddress1," + "AgtCntDtls.Adm_Address_2 AgtAddress2," + "AgtCntDtls.Adm_Address_3 AgtAddress3," + "AgtCntDtls.Adm_City AgtCity," + "AGTCNTDTLS.ADM_ZIP_CODE AGTZIP," + "AGTCNTDTLS.ADM_PHONE_NO_1 AGTPHONE," + "AGTCNTDTLS.ADM_FAX_NO AGTFAX," + "AGTCNTDTLS.ADM_EMAIL_ID AGTEMAIL," + "AGTCOUNTRY.COUNTRY_NAME AGTCOUNTR," + "POL.PORT_NAME POL," + "POD.PORT_NAME POD," + "COLMST.PLACE_NAME COLPLACE," + "DELMST.PLACE_NAME DELPLACE," + "STMST.INCO_CODE TERMS," + "NVL(JOBAIREXP.INSURANCE_AMT,0) INSURANCE," + "BKGAIR.CARGO_TYPE CARGOTYPE," + "JOBAIREXP.MARKS_NUMBERS MARKS," + "JOBAIREXP.GOODS_DESCRIPTION GOODS," + "CGMST.COMMODITY_GROUP_DESC COMMODITY," + "JOBAIREXP.ETD_DATE ETD," + "SUM(JTAEC.VOLUME_IN_CBM) VOLUME," + "SUM(JTAEC.GROSS_WEIGHT) GROSS," + "SUM(JTAEC.CHARGEABLE_WEIGHT) CHARWT," + "MAX(JTAET.ETA_DATE) ETA," + "'' NETWT" + "from job_card_air_exp_tbl JobAirExp," + "JOB_TRN_AIR_EXP_TP JTAET," + "JOB_TRN_AIR_EXP_CONT JTAEC," + "Customer_Mst_Tbl CustMstShipper," + "Customer_Mst_Tbl CustMstConsignee," + "hawb_exp_tbl hawb," + "mawb_exp_tbl mawb," + "Port_Mst_Tbl POL," + "Port_Mst_Tbl POD," + "Place_Mst_Tbl DELMST," + "PLACE_MST_TBL COLMST," + "Booking_Air_Tbl BkgAir," + "Agent_Mst_Tbl AgtMst," + "Agent_Contact_Dtls AgtCntDtls," + "Customer_Contact_Dtls CustShipperDtls," + "Customer_Contact_Dtls CustConsigneeDtls," + "COUNTRY_MST_TBL SHIPCOUNTRY," + "COUNTRY_MST_TBL CONSIGCOUNTRY," + "COUNTRY_MST_TBL AGTCOUNTRY," + "SHIPPING_TERMS_MST_TBL STMST," + "COMMODITY_GROUP_MST_TBL CGMST" + "where JobAirExp.Shipper_Cust_Mst_Fk = CustMstShipper.Customer_Mst_Pk(+)" + "and JobAirExp.Consignee_Cust_Mst_Fk=CustMstConsignee.Customer_Mst_Pk(+)" + "AND JTAEC.JOB_CARD_AIR_EXP_FK(+)=JOBAIREXP.JOB_CARD_AIR_EXP_PK" + "AND JTAET.JOB_CARD_AIR_EXP_FK(+)=JOBAIREXP.JOB_CARD_AIR_EXP_PK" + "AND  nvl(JTAET.ETA_DATE,sysdate)=(SELECT  nvl(MAX(JTT.ETA_DATE),sysdate) FROM JOB_TRN_AIR_EXP_TP JTT WHERE JTT.JOB_CARD_AIR_EXP_FK=JTAET.JOB_CARD_AIR_EXP_FK)" + "and POL.PORT_MST_PK(+)=BkgAir.Port_Mst_Pol_Fk" + "and POD.PORT_MST_PK(+)=BkgAir.Port_Mst_Pod_Fk" + "AND COLMST.PLACE_PK(+)=BKGAIR.COL_PLACE_MST_FK" + " and DELMST.PLACE_PK(+)=BkgAir.Del_Place_Mst_Fk" + "and JobAirExp.Dp_Agent_Mst_Fk=AgtMst.Agent_Mst_Pk(+)" + "and AgtMst.Agent_Mst_Pk=AgtCntDtls.Agent_Mst_Fk(+)" + "and CustMstShipper.Customer_Mst_Pk=CustShipperDtls.Customer_Mst_Fk(+)" + "and CustMstConsignee.Customer_Mst_Pk=CustConsigneeDtls.Customer_Mst_Fk(+)" + "and JobAirExp.Booking_Air_Fk=BkgAir.Booking_Air_Pk" + "and hawb.hawb_exp_tbl_pk(+)=JobAirExp.Hawb_Exp_Tbl_Fk" + "and mawb.mawb_exp_tbl_pk(+)=JobAirExp.Mawb_Exp_Tbl_Fk" + "AND SHIPCOUNTRY.COUNTRY_MST_PK(+)=CUSTSHIPPERDTLS.ADM_COUNTRY_MST_FK" + "AND CONSIGCOUNTRY.COUNTRY_MST_PK(+)=CUSTCONSIGNEEDTLS.ADM_COUNTRY_MST_FK" + " AND STMST.SHIPPING_TERMS_MST_PK(+)=JOBAIREXP.Shipping_Terms_Mst_Fk" + "AND CGMST.COMMODITY_GROUP_PK(+)=JOBAIREXP.COMMODITY_GROUP_FK" + "AND AGTCOUNTRY.COUNTRY_MST_PK(+)=AGTCNTDTLS.ADM_COUNTRY_MST_FK" + "and JobAirExp.Job_Card_Air_Exp_Pk=" + JOBPK + "GROUP BY" + "JobAirExp.Job_Card_Air_Exp_Pk ," + "JobAirExp.Jobcard_Ref_No," + "JobAirExp.Ucr_No," + "HAWB.HAWB_REF_NO ," + " MAWB.MAWB_REF_NO," + "JobAirExp.Flight_No," + "CustMstShipper.Customer_Name," + "CustShipperDtls.Adm_Address_1," + "CustShipperDtls.Adm_Address_2 ," + "CustShipperDtls.Adm_Address_3 ," + "CustShipperDtls.Adm_City," + "CUSTSHIPPERDTLS.ADM_ZIP_CODE ," + "CUSTSHIPPERDTLS.ADM_PHONE_NO_1 ," + "CUSTSHIPPERDTLS.ADM_FAX_NO," + " CUSTSHIPPERDTLS.ADM_EMAIL_ID ," + " SHIPCOUNTRY.COUNTRY_NAME ," + " CustMstConsignee.Customer_Name," + "CustConsigneeDtls.Adm_Address_1 ," + "CustConsigneeDtls.Adm_Address_2," + "CustConsigneeDtls.Adm_Address_3 ," + "CustConsigneeDtls.Adm_City," + "CUSTCONSIGNEEDTLS.ADM_ZIP_CODE ," + "CUSTCONSIGNEEDTLS.ADM_PHONE_NO_1," + "CUSTCONSIGNEEDTLS.ADM_FAX_NO ," + "CUSTCONSIGNEEDTLS.ADM_EMAIL_ID ," + "CONSIGCOUNTRY.COUNTRY_NAME ," + "AgtMst.Agent_Name," + "AgtCntDtls.Adm_Address_1," + "AgtCntDtls.Adm_Address_2 ," + "AgtCntDtls.Adm_Address_3 ," + "AgtCntDtls.Adm_City," + "AGTCNTDTLS.ADM_ZIP_CODE ," + "AGTCNTDTLS.ADM_PHONE_NO_1," + "AGTCNTDTLS.ADM_FAX_NO," + "AGTCNTDTLS.ADM_EMAIL_ID ," + "AGTCOUNTRY.COUNTRY_NAME," + "POL.PORT_NAME ," + "POD.PORT_NAME ," + "COLMST.PLACE_NAME ," + "DELMST.PLACE_NAME," + "STMST.INCO_CODE ," + "NVL(JOBAIREXP.INSURANCE_AMT,0)," + " BKGAIR.CARGO_TYPE ," + "JOBAIREXP.MARKS_NUMBERS," + "JOBAIREXP.GOODS_DESCRIPTION," + "CGMST.COMMODITY_GROUP_DESC," + "JOBAIREXP.ETD_DATE ETD";

            try
            {
                return (ObjWF.GetDataSet(Strsql));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the imp header docment.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchImpHeaderDocment(int JOBPK)
        {
            string Strsql = null;
            WorkFlow ObjWF = new WorkFlow();
            Strsql = "SELECT JAI.JOB_CARD_TRN_PK JOBPK," + "JAI.JOBCARD_REF_NO JOBREFNO," + "'' BKGPK," + " '' BKGREFNO," + " '' BKGDATE," + "JAI.VOYAGE_FLIGHT_NO VESFLIGHT," + " JAI.HBL_HAWB_REF_NO HBLREFNO," + " JAI.MBL_MAWB_REF_NO MBLREFNO," + " JAI.MARKS_NUMBERS MARKS," + "JAI.GOODS_DESCRIPTION GOODS," + "1  CARGO_TYPE," + "JAI.UCR_NO UCRNO," + "JAI.CLEARANCE_ADDRESS CLEARANCEPOINT," + " JAI.ETD_DATE ETD," + " JAI.SHIPPER_CUST_MST_FK," + "CMST.CUSTOMER_NAME SHIPNAME," + "'' SHIPREFNO," + "CDTLS.ADM_ADDRESS_1 SHIPADD1," + "CDTLS.ADM_ADDRESS_2 SHIPADD2," + "CDTLS.ADM_ADDRESS_3 SHIPADD3," + "CDTLS.ADM_CITY SHIPCITY," + "CDTLS.ADM_ZIP_CODE SHIPZIP," + "CDTLS.ADM_EMAIL_ID AS SHIPEMAIL," + "CDTLS.ADM_PHONE_NO_1 SHIPPHONE," + "CDTLS.ADM_FAX_NO SHIPFAX," + "SHICOUNTRY.COUNTRY_NAME SHIPCOUNTRY," + "CONSIGNEE.CUSTOMER_NAME CONSIGNEENAME," + "CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGADD1," + "CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGADD2," + "CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGADD3," + "CONSIGNEEDTLS.ADM_CITY CONSIGCITY," + "CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGZIP," + " CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGEMAIL," + "CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGPHONE," + "CONSIGNEEDTLS.ADM_FAX_NO CONSIGFAX," + "CONSIGCOUNTRY.COUNTRY_NAME CONSICOUNTRY," + "POL.PORT_NAME POLNAME," + "POD.PORT_NAME PODNAME," + "PLD.PLACE_NAME DELNAME, " + "DBAMST.AGENT_MST_PK DBAGENTPK," + "DBAMST.AGENT_NAME DBAGENTNAME," + "DBADTLS.ADM_ADDRESS_1 DBAGENTADD1," + "DBADTLS.ADM_ADDRESS_2 DBAGENTADD2," + "DBADTLS.ADM_ADDRESS_3 DBAGENTADD3," + "DBADTLS.ADM_CITY DBAGENTCITY," + "DBADTLS.ADM_ZIP_CODE DBAGENTZIP," + "DBADTLS.ADM_EMAIL_ID DBAGENTEMAIL," + "DBADTLS.ADM_PHONE_NO_1 DBAGENTPHONE," + "DBADTLS.ADM_FAX_NO DBAGENTFAX," + "DBCOUNTRY.COUNTRY_NAME DBCOUNTRY," + "STMST.INCO_CODE TERMS," + "NVL(JAI.INSURANCE_AMT, 0) INSURANCE," + "JAI.PYMT_TYPE," + "CGMST.commodity_group_desc COMMCODE," + "JAI.ETA_DATE ETA," + "SUM(JTAEC.GROSS_WEIGHT) GROSS," + "SUM(JTAEC.CHARGEABLE_WEIGHT) CHARWT," + " '' NETWT," + "SUM(JTAEC.VOLUME_IN_CBM) VOLUME" + "FROM JOB_CARD_TRN JAI," + "JOB_TRN_TP   JTAEP," + "JOB_TRN_CONT JTAEC," + "CUSTOMER_MST_TBL      CMST," + "CUSTOMER_CONTACT_DTLS CDTLS," + "CUSTOMER_MST_TBL      CONSIGNEE," + "CUSTOMER_CONTACT_DTLS CONSIGNEEDTLS," + "COUNTRY_MST_TBL       SHICOUNTRY," + "COUNTRY_MST_TBL       CONSIGCOUNTRY," + "PORT_MST_TBL          POL," + "PORT_MST_TBL          POD," + "PLACE_MST_TBL         PLD," + "AGENT_MST_TBL           DBAMST," + "AGENT_CONTACT_DTLS      DBADTLS," + "COUNTRY_MST_TBL         DBCOUNTRY," + "SHIPPING_TERMS_MST_TBL  STMST," + " COMMODITY_GROUP_MST_TBL CGMST " + "WHERE " + "JAI.JOB_CARD_TRN_PK IN(" + JOBPK + ")" + "AND JTAEP.JOB_CARD_TRN_FK(+) = JAI.JOB_CARD_TRN_PK " + "AND  nvl(JTAEP.ETA_DATE,sysdate)=(SELECT  nvl(MAX(JTT.ETA_DATE),sysdate) FROM JOB_TRN_AIR_IMP_TP JTT WHERE JTT.JOB_CARD_TRN_FK=JTAEP.JOB_CARD_TRN_FK)" + "AND JTAEC.JOB_CARD_TRN_FK(+) = JAI.JOB_CARD_TRN_PK" + "AND CMST.CUSTOMER_MST_PK(+) = JAI.SHIPPER_CUST_MST_FK" + "AND CDTLS.CUSTOMER_MST_FK(+) = CMST.CUSTOMER_MST_PK" + "AND CONSIGNEE.CUSTOMER_MST_PK(+) = JAI.CONSIGNEE_CUST_MST_FK" + "AND CONSIGNEEDTLS.CUSTOMER_MST_FK(+) = CONSIGNEE.CUSTOMER_MST_PK" + "AND JAI.PORT_MST_POL_FK = POL.PORT_MST_PK(+)" + "AND JAI.PORT_MST_POD_FK = POD.PORT_MST_PK(+)" + "AND JAI.DEL_PLACE_MST_FK = PLD.PLACE_PK(+)" + "AND JAI.POL_AGENT_MST_FK = DBAMST.AGENT_MST_PK(+)" + "AND DBAMST.AGENT_MST_PK = DBADTLS.AGENT_MST_FK(+)" + "AND DBCOUNTRY.COUNTRY_MST_PK(+) = DBADTLS.ADM_COUNTRY_MST_FK" + "AND CONSIGCOUNTRY.COUNTRY_MST_PK(+) = CONSIGNEE.COUNTRY_MST_FK" + "AND SHICOUNTRY.COUNTRY_MST_PK(+) = CMST.COUNTRY_MST_FK" + "AND STMST.SHIPPING_TERMS_MST_PK(+) = JAI.SHIPPING_TERMS_MST_FK" + "AND JAI.COMMODITY_GROUP_FK = CGMST.COMMODITY_GROUP_PK(+)" + "GROUP BY JAI.JOB_CARD_TRN_PK," + "JAI.JOBCARD_REF_NO,  " + "JAI.VOYAGE_FLIGHT_NO," + "JAI.HBL_HAWB_REF_NO," + "JAI.MBL_MAWB_REF_NO," + " JAI.MARKS_NUMBERS," + "JAI.GOODS_DESCRIPTION," + "JAI.UCR_NO, " + "JAI.CLEARANCE_ADDRESS," + "JAI.ETD_DATE," + "JAI.SHIPPER_CUST_MST_FK," + "CMST.CUSTOMER_NAME," + "CDTLS.ADM_ADDRESS_1," + "CDTLS.ADM_ADDRESS_2," + "CDTLS.ADM_ADDRESS_3," + "CDTLS.ADM_CITY," + "CDTLS.ADM_ZIP_CODE," + "CDTLS.ADM_EMAIL_ID," + "CDTLS.ADM_PHONE_NO_1," + "CDTLS.ADM_FAX_NO," + "SHICOUNTRY.COUNTRY_NAME," + "CONSIGNEE.CUSTOMER_NAME," + " CONSIGNEEDTLS.ADM_ADDRESS_1," + "CONSIGNEEDTLS.ADM_ADDRESS_2," + "CONSIGNEEDTLS.ADM_ADDRESS_3," + "CONSIGNEEDTLS.ADM_CITY," + "CONSIGNEEDTLS.ADM_ZIP_CODE," + "CONSIGNEEDTLS.ADM_EMAIL_ID," + "CONSIGNEEDTLS.ADM_PHONE_NO_1," + "CONSIGNEEDTLS.ADM_FAX_NO," + "CONSIGCOUNTRY.COUNTRY_NAME," + " POL.PORT_NAME," + " POD.PORT_NAME," + " PLD.PLACE_NAME," + " DBAMST.AGENT_MST_PK," + "DBAMST.AGENT_NAME," + "DBADTLS.ADM_ADDRESS_1," + "DBADTLS.ADM_ADDRESS_2," + "DBADTLS.ADM_ADDRESS_3," + "DBADTLS.ADM_CITY," + "DBADTLS.ADM_ZIP_CODE," + "DBADTLS.ADM_EMAIL_ID," + "DBADTLS.ADM_PHONE_NO_1," + "DBADTLS.ADM_FAX_NO," + "DBCOUNTRY.COUNTRY_NAME," + "STMST.INCO_CODE," + "NVL(JAI.INSURANCE_AMT, 0)," + "JAI.PYMT_TYPE," + "CGMST.commodity_group_desc,JAI.ETA_DATE";
            try
            {
                return (ObjWF.GetDataSet(Strsql));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the air imp containers.
        /// </summary>
        /// <param name="JobRefPK">The job reference pk.</param>
        /// <returns></returns>
        public DataSet FetchAIRImpContainers(string JobRefPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT JAI.JOB_CARD_TRN_FK JOBPK ,JAI.PALETTE_SIZE CONTAINER FROM JOB_TRN_CONT JAI ";
            Strsql += "WHERE JAI.JOB_CARD_TRN_FK IN(" + JobRefPK + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Header Document Report"

        #region "Movement Loading List Details"

        /// <summary>
        /// Fetches the movement listing.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchMovementListing(int JOBPK)
        {
            string Strsql = null;
            WorkFlow ObjWF = new WorkFlow();
            Strsql = "SELECT JSE.JOB_CARD_TRN_PK JOBPK,";
            Strsql += "JSE.JOBCARD_REF_NO JOBREFNO,";
            Strsql += "JSE.VOYAGE_FLIGHT_NO VES_FLIGHT,";
            Strsql += "to_char(JSE.ETD_DATE ,dateformat) ETD,";
            Strsql += "to_char(JSE.ETA_DATE ,dateformat) ETA,";
            Strsql += "DECODE(BST.CARGO_TYPE, 1, 'FCL', 2, 'LCL') CONTAINER,";
            Strsql += "AMST.AGENT_NAME AGENTNAME,";
            Strsql += "ADTLS.ADM_ADDRESS_1 DPADDRESS1,";
            Strsql += "ADTLS.ADM_ADDRESS_2 DPADDRESS2,";
            Strsql += "ADTLS.ADM_ADDRESS_3 DPADDRESS3,";
            Strsql += "ADTLS.ADM_CITY DPCITY,";
            Strsql += "ADTLS.ADM_ZIP_CODE DPZIP,";
            Strsql += "ADTLS.ADM_PHONE_NO_1 DPPHONE,";
            Strsql += "ADTLS.ADM_FAX_NO DPFAX,";
            Strsql += "ADTLS.ADM_EMAIL_ID DPEMAIL,";
            Strsql += "ADCOUNTRY.COUNTRY_NAME DPCOUNTRY,";
            Strsql += "POL.PORT_NAME POLNAME,";
            Strsql += "POD.PORT_NAME PODNAME,";
            Strsql += "SHIPPER.CUSTOMER_NAME SHIPPER,";
            Strsql += "JSE.MARKS_NUMBERS MARKS,";
            Strsql += "JSE.GOODS_DESCRIPTION GOODS,";
            Strsql += "SUM(JTSEC.PACK_COUNT) PACKCOUNT,";
            Strsql += "SUM(JTSEC.VOLUME_IN_CBM) VOLUME,";
            Strsql += "SUM(JTSEC.GROSS_WEIGHT) GROSSWT,";
            Strsql += "'' NETWT,";
            Strsql += "SUM(JTSEC.COMMODITY_MST_FK) CHRWT";
            Strsql += "FROM JOB_CARD_TRN JSE,";
            Strsql += "JOB_TRN_CONT JTSEC,";
            Strsql += "JOB_TRN_TP JTSET,";
            Strsql += "CUSTOMER_MST_TBL  SHIPPER,";
            Strsql += "BOOKING_MST_TBL      BST,";
            Strsql += "PORT_MST_TBL         POL,";
            Strsql += "PORT_MST_TBL         POD,";
            Strsql += "AGENT_MST_TBL        AMST,";
            Strsql += "AGENT_CONTACT_DTLS   ADTLS,";
            Strsql += "COUNTRY_MST_TBL ADCOUNTRY";
            Strsql += "WHERE JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
            Strsql += "AND JTSEC.JOB_CARD_TRN_FK(+) = JSE.JOB_CARD_TRN_PK";
            Strsql += "AND JTSET.JOB_CARD_TRN_FK(+)=JSE.JOB_CARD_TRN_PK";
            Strsql += "AND NVL(JTSET.ETA_DATE,SYSDATE)=(SELECT NVL(MAX(JTS.ETA_DATE),SYSDATE) FROM JOB_TRN_TP JTS WHERE JTS.JOB_CARD_TRN_FK=JSE.JOB_CARD_TRN_PK)";
            Strsql += "AND JSE.BOOKING_MST_FK = BST.BOOKING_MST_PK";
            Strsql += "AND POL.PORT_MST_PK(+) = BST.PORT_MST_POL_FK";
            Strsql += "AND POD.PORT_MST_PK(+) = BST.PORT_MST_POD_FK";
            Strsql += "AND JSE.DP_AGENT_MST_FK = AMST.AGENT_MST_PK(+)";
            Strsql += "AND AMST.AGENT_MST_PK = ADTLS.AGENT_MST_FK(+)";
            Strsql += "AND ADCOUNTRY.COUNTRY_MST_PK(+) = ADTLS.ADM_COUNTRY_MST_FK";
            Strsql += "AND JSE.JOB_CARD_TRN_PK =" + JOBPK;
            Strsql += "GROUP BY";
            Strsql += "JSE.JOB_CARD_TRN_PK ,";
            Strsql += "JSE.JOBCARD_REF_NO,";
            Strsql += "JSE.VOYAGE_FLIGHT_NO,";
            Strsql += "JSE.ETD_DATE,";
            Strsql += "JSE.ETA_DATE,";
            Strsql += "DECODE(BST.CARGO_TYPE, 1, 'FCL', 2, 'LCL') ,";
            Strsql += "AMST.AGENT_NAME ,";
            Strsql += "ADTLS.ADM_ADDRESS_1,";
            Strsql += "ADTLS.ADM_ADDRESS_2 ,";
            Strsql += "ADTLS.ADM_ADDRESS_3 ,";
            Strsql += "ADTLS.ADM_CITY ,";
            Strsql += "ADTLS.ADM_ZIP_CODE,";
            Strsql += "ADTLS.ADM_PHONE_NO_1 ,";
            Strsql += "ADTLS.ADM_FAX_NO ,";
            Strsql += "ADTLS.ADM_EMAIL_ID ,";
            Strsql += "ADCOUNTRY.COUNTRY_NAME,";
            Strsql += "POL.PORT_NAME,";
            Strsql += "POD.PORT_NAME ,";
            Strsql += "SHIPPER.CUSTOMER_NAME ,";
            Strsql += "JSE.MARKS_NUMBERS,";
            Strsql += "JSE.GOODS_DESCRIPTION";
            try
            {
                return (ObjWF.GetDataSet(Strsql));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Movement Loading List Details"

        //..........End Chandra........
        //Rijesh Added for Cost Element Fetch According To Vendor Type

        #region "enhance search for COST ELEMENT"

        /// <summary>
        /// Fetches the cost element by vendor.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCostElementByVendor(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            string strReq = null;
            Array arr = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(1));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_COST_ELM_JOB_PKG.GET_COST_ELEMENT";
                var _with56 = SCM.Parameters;
                _with56.Add("VENDOR_PK_IN", strReq);
                _with56.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
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

        #endregion "enhance search for COST ELEMENT"

        #region "Fetch Data for Standard Shipping Note"

        /// <summary>
        /// Fetches the SSN.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchSSN(string JOBPK)
        {
            string Strsql = null;
            WorkFlow ObjWF = new WorkFlow();

            try
            {
                Strsql = " SELECT                                                       ";
                Strsql += " JAE.JOB_CARD_TRN_PK JOBPK,                     ";
                Strsql += " JAE.JOBCARD_REF_NO JOBREFNO,                       ";
                Strsql += " BAT.BOOKING_MST_PK BKGPK,                          ";
                Strsql += " BAT.BOOKING_REF_NO BKGREFNO,                       ";
                Strsql += " BAT.BOOKING_DATE BKGDATE,                          ";
                Strsql += " JAE.SHIPPER_CUST_MST_FK EXPORTERPK,                ";
                Strsql += " CUSTOMER.CUSTOMER_NAME EXPORTERNAME,               ";
                Strsql += " CUSTDTLS.ADM_ADDRESS_1 EXPORTERADD1,               ";
                Strsql += " CUSTDTLS.ADM_ADDRESS_2 EXPORTERADD2,               ";
                Strsql += " CUSTDTLS.ADM_ADDRESS_3 EXPORTERADD3,               ";
                Strsql += " CUSTDTLS.ADM_CITY EXPORTERCITY,                    ";
                Strsql += " CUSTDTLS.ADM_LOCATION_MST_FK EXPORTERLOCFK,        ";
                Strsql += " EXPORTERLOC.LOCATION_NAME EXPORTERLOCNAME,         ";
                Strsql += " CUSTDTLS.ADM_COUNTRY_MST_FK EXPORTERCOUNTRYFK,     ";
                Strsql += " EXPORTERCOUNTRY.COUNTRY_NAME EXPORTERCOUNTRYNAME,  ";
                Strsql += " CUSTOMER.VAT_NO EXPORTERSREF,                      ";
                Strsql += " CUSTDTLS.ADM_ZIP_CODE EXPORTERZIP,                 ";
                Strsql += " CUSTDTLS.ADM_PHONE_NO_1 EXPORTERPHONE1,            ";
                Strsql += " CUSTDTLS.ADM_PHONE_NO_2 EXPORTERPHONE2,            ";
                Strsql += " CUSTDTLS.ADM_FAX_NO     EXPORTERFAX,               ";
                Strsql += " CUSTDTLS.ADM_EMAIL_ID   EXPORTEREMAIL,             ";
                Strsql += " CUSTDTLS.ADM_URL        EXPORTERURL,               ";
                Strsql += " BAT.CUSTOMS_CODE_MST_FK CUSTOMSFK,                 ";
                Strsql += " CUSTOMSTAT.CUSTOMS_STATUS_CODE CUSTOMSCODE,        ";
                Strsql += " CUSTOMER.VAT_NO EXPORTERREF,                       ";
                Strsql += " CORP.CORPORATE_NAME CORPNAME,                      ";
                Strsql += " CORP.ADDRESS_LINE1 CORPADD1,                       ";
                Strsql += " CORP.ADDRESS_LINE2 CORPADD2,                       ";
                Strsql += " CORP.ADDRESS_LINE3 CORPADD3,                       ";
                Strsql += " CORP.CITY          CORPCITY,                       ";
                Strsql += " CORP.STATE_MST_FK CORPSTATEFK,                     ";
                Strsql += " STATE.STATE_NAME CORPSTATENAME,                    ";
                Strsql += " CORP.COUNTRY_MST_FK CORPCOUNTRYFK,                 ";
                Strsql += " COUNTRY.COUNTRY_NAME CORPCOUNTRY,                  ";
                Strsql += " CORP.POST_CODE       CORPZIP,                      ";
                Strsql += " CORP.PHONE           CORPPHONE,                    ";
                Strsql += " CORP.FAX             CORPFAX,                      ";
                Strsql += " CORP.EMAIL           CORPEMAIL,                    ";
                Strsql += " CORP.HOME_PAGE       CORPURL,                      ";
                Strsql += " BAT.CARRIER_MST_FK INTLCARRFK,                     ";
                Strsql += " AIRLINE.AIRLINE_NAME INTLCARRNAME,                 ";
                Strsql += " ' ' OTHUKTRANS,                                    ";
                Strsql += " JAE.VOYAGE_FLIGHT_NO VSL_OR_FLIGHT_NO,                    ";
                Strsql += " TO_CHAR(JAE.ETD_DATE,'" + dateFormat + "') VSL_OR_FLIGHT_DATE,  ";
                Strsql += " BAT.PORT_MST_POL_FK PORTOFLANDFK ,                 ";
                Strsql += " PORTOFLANDING.PORT_NAME PORTOFLANDNAME,            ";
                Strsql += " BAT.PORT_MST_POD_FK PORTOFDISCHFK,                 ";
                Strsql += " PORTOFDISCHARGE.PORT_NAME PORTOFDISCHNAME,         ";
                Strsql += " BAT.DEL_PLACE_MST_FK DELPLACEFK,                   ";
                Strsql += " PLD.PLACE_NAME DELPLACENAME,                       ";
                Strsql += " (CASE                                              ";
                Strsql += " WHEN JAE.HBL_HAWB_FK IS NOT NULL THEN          ";
                Strsql += " jAE.MARKS_NUMBERS                                  ";
                Strsql += " ELSE                                               ";
                Strsql += " JAE.MARKS_NUMBERS                                  ";
                Strsql += " END) MARKS,                                        ";
                Strsql += " (CASE                                              ";
                Strsql += " WHEN JAE.HBL_HAWB_FK IS NOT NULL THEN          ";
                Strsql += " JAE.GOODS_DESCRIPTION                              ";
                Strsql += " ELSE                                               ";
                Strsql += " JAE.GOODS_DESCRIPTION                              ";
                Strsql += " END) GOODS,                                        ";
                Strsql += " SUM(JTAEC.GROSS_WEIGHT) GROSSWT,                   ";
                Strsql += " SUM(JTAEC.VOLUME_IN_CBM) VOL_IN_CBM,               ";
                Strsql += " SUM(JTAEC.GROSS_WEIGHT) TOTGROSSWT,                ";
                Strsql += " SUM(JTAEC.VOLUME_IN_CBM) TOT_VOL_IN_CBM,           ";
                Strsql += " JTAEC.PALETTE_SIZE CONTAINERNO,                    ";
                Strsql += " ' ' SEALNO,                                        ";
                Strsql += " ' ' CONTAINERSIZE,                                 ";
                Strsql += " ' ' TAREWT,                                        ";
                Strsql += " JTAEC.PACK_COUNT TOTOFBOXES,                       ";
                Strsql += " CORP.CORPORATE_NAME NAMEOFCOMP,                    ";
                Strsql += " TO_CHAR(SYSDATE,'" + dateFormat + "') TODAYSDATE,          ";
                Strsql += " JAE.TRANSPORTER_CARRIER_FK HAULIERFK,              ";
                Strsql += " TRANSPORTER.VENDOR_NAME HAULIERNAME,          ";
                Strsql += " ' ' VEHICLEREGNO                                   ";
                Strsql += " FROM JOB_CARD_TRN    JAE,                  ";
                Strsql += " JOB_TRN_CONT    JTAEC,                     ";
                Strsql += " BOOKING_MST_TBL         BAT,                       ";
                Strsql += " PLACE_MST_TBL           PLD,                       ";
                Strsql += " COMMODITY_GROUP_MST_TBL CGMST,                     ";
                Strsql += " CUSTOMS_STATUS_MST_TBL  CUSTOMSTAT,                ";
                Strsql += " CORPORATE_MST_TBL       CORP,                      ";
                Strsql += " COUNTRY_MST_TBL         COUNTRY,                   ";
                Strsql += " AIRLINE_MST_TBL         AIRLINE,                   ";
                Strsql += " PORT_MST_TBL            PORTOFLANDING,             ";
                Strsql += " PORT_MST_TBL            PORTOFDISCHARGE,           ";
                Strsql += " VENDOR_MST_TBL TRANSPORTER,                   ";
                Strsql += " STATE_MST_TBL STATE,                               ";
                Strsql += " CUSTOMER_MST_TBL CUSTOMER,                         ";
                Strsql += " CUSTOMER_CONTACT_DTLS CUSTDTLS,                    ";
                Strsql += " LOCATION_MST_TBL EXPORTERLOC,                      ";
                Strsql += " COUNTRY_MST_TBL EXPORTERCOUNTRY                    ";
                Strsql += " WHERE JAE.JOB_CARD_TRN_PK IN(" + JOBPK + " )   ";
                Strsql += " AND JTAEC.JOB_CARD_TRN_FK(+) = JAE.JOB_CARD_TRN_PK";
                Strsql += " AND JAE.BOOKING_MST_FK = BAT.BOOKING_MST_PK(+)";
                Strsql += " AND JAE.COMMODITY_GROUP_FK = CGMST.COMMODITY_GROUP_PK(+)";
                Strsql += " AND BAT.CUSTOMS_CODE_MST_FK = CUSTOMSTAT.CUSTOMS_CODE_MST_PK(+)";
                Strsql += " AND CORP.COUNTRY_MST_FK     = COUNTRY.COUNTRY_MST_PK(+)";
                Strsql += " AND BAT.CARRIER_MST_FK      = AIRLINE.AIRLINE_MST_PK(+)";
                Strsql += " AND BAT.PORT_MST_POL_FK     = PORTOFLANDING.PORT_MST_PK(+)";
                Strsql += " AND BAT.PORT_MST_POD_FK     = PORTOFDISCHARGE.PORT_MST_PK(+)";
                Strsql += " AND BAT.DEL_PLACE_MST_FK    = PLD.PLACE_PK(+)";
                Strsql += " AND JAE.TRANSPORTER_CARRIER_FK = TRANSPORTER.VENDOR_MST_PK(+)";
                Strsql += " AND CORP.STATE_MST_FK          = STATE.STATE_MST_PK(+)";
                Strsql += " AND JAE.SHIPPER_CUST_MST_FK    = CUSTOMER.CUSTOMER_MST_PK";
                Strsql += " AND CUSTDTLS.CUSTOMER_MST_FK   = CUSTOMER.CUSTOMER_MST_PK(+)";
                Strsql += " AND CUSTDTLS.ADM_LOCATION_MST_FK = EXPORTERLOC.LOCATION_MST_PK(+)";
                Strsql += " AND CUSTDTLS.ADM_COUNTRY_MST_FK  = EXPORTERCOUNTRY.COUNTRY_MST_PK(+)";
                Strsql += " and CGMST.COMMODITY_GROUP_CODE NOT LIKE 'DGS'";
                Strsql += " GROUP BY JAE.JOB_CARD_TRN_PK,                  ";
                Strsql += " JAE.JOBCARD_REF_NO,                                ";
                Strsql += " BAT.BOOKING_MST_PK,                                ";
                Strsql += " BAT.BOOKING_REF_NO,                                ";
                Strsql += " BAT.BOOKING_DATE,                                  ";
                Strsql += " JAE.SHIPPER_CUST_MST_FK ,                          ";
                Strsql += " CUSTOMER.CUSTOMER_NAME ,                           ";
                Strsql += " CUSTDTLS.ADM_ADDRESS_1 ,                           ";
                Strsql += " CUSTDTLS.ADM_ADDRESS_2 ,                           ";
                Strsql += " CUSTDTLS.ADM_ADDRESS_3 ,                           ";
                Strsql += " CUSTDTLS.ADM_CITY ,                                ";
                Strsql += " CUSTDTLS.ADM_LOCATION_MST_FK ,                     ";
                Strsql += " EXPORTERLOC.LOCATION_NAME ,                        ";
                Strsql += " CUSTDTLS.ADM_COUNTRY_MST_FK ,                      ";
                Strsql += " EXPORTERCOUNTRY.COUNTRY_NAME ,                     ";
                Strsql += " CUSTOMER.VAT_NO ,                                  ";
                Strsql += " CUSTDTLS.ADM_ZIP_CODE ,                            ";
                Strsql += " CUSTDTLS.ADM_PHONE_NO_1 ,                          ";
                Strsql += " CUSTDTLS.ADM_PHONE_NO_2 ,                          ";
                Strsql += " CUSTDTLS.ADM_FAX_NO     ,                          ";
                Strsql += " CUSTDTLS.ADM_EMAIL_ID   ,                          ";
                Strsql += " CUSTDTLS.ADM_URL        ,                          ";
                Strsql += " BAT.CUSTOMS_CODE_MST_FK ,                          ";
                Strsql += " CUSTOMSTAT.CUSTOMS_STATUS_CODE ,                   ";
                Strsql += " ' ' ,                                              ";
                Strsql += " CORP.CORPORATE_NAME ,                              ";
                Strsql += " CORP.ADDRESS_LINE1 ,                               ";
                Strsql += " CORP.ADDRESS_LINE2 ,                               ";
                Strsql += " CORP.ADDRESS_LINE3 ,                               ";
                Strsql += " CORP.CITY          ,                               ";
                Strsql += " CORP.STATE_MST_FK ,                                ";
                Strsql += " STATE.STATE_NAME ,                                 ";
                Strsql += " CORP.COUNTRY_MST_FK ,                              ";
                Strsql += " COUNTRY.COUNTRY_NAME ,                             ";
                Strsql += " CORP.POST_CODE       ,                             ";
                Strsql += " CORP.PHONE           ,                             ";
                Strsql += " CORP.FAX             ,                             ";
                Strsql += " CORP.EMAIL           ,                             ";
                Strsql += " CORP.HOME_PAGE       ,                             ";
                Strsql += " BAT.CARRIER_MST_FK ,                           ";
                Strsql += " AIRLINE.AIRLINE_NAME,                          ";
                Strsql += " ' ',                                           ";
                Strsql += " JAE.VOYAGE_FLIGHT_NO,                                 ";
                Strsql += " TO_CHAR(JAE.ETD_DATE,'" + dateFormat + "'),            ";
                Strsql += " BAT.PORT_MST_POL_FK,                           ";
                Strsql += " PORTOFLANDING.PORT_NAME,                       ";
                Strsql += " BAT.PORT_MST_POD_FK ,                          ";
                Strsql += " PORTOFDISCHARGE.PORT_NAME,                     ";
                Strsql += " BAT.DEL_PLACE_MST_FK,                          ";
                Strsql += " PLD.PLACE_NAME,                                ";
                Strsql += " (CASE                                          ";
                Strsql += " WHEN JAE.HBL_HAWB_FK IS NOT NULL THEN      ";
                Strsql += " JAE.MARKS_NUMBERS                              ";
                Strsql += " ELSE                                           ";
                Strsql += " JAE.MARKS_NUMBERS                              ";
                Strsql += " END),                                          ";
                Strsql += " (CASE                                          ";
                Strsql += " WHEN JAE.HBL_HAWB_FK IS NOT NULL THEN      ";
                Strsql += " JAE.GOODS_DESCRIPTION                          ";
                Strsql += " ELSE                                           ";
                Strsql += " JAE.GOODS_DESCRIPTION                          ";
                Strsql += " END),                                           ";
                Strsql += " JTAEC.PALETTE_SIZE,                             ";
                Strsql += " ' ',                                            ";
                Strsql += " ' ',                                            ";
                Strsql += " ' ',                                            ";
                Strsql += " JTAEC.PACK_COUNT,                               ";
                Strsql += " CORP.CORPORATE_NAME,                            ";
                Strsql += " TO_CHAR(SYSDATE,'" + dateFormat + "'),                  ";
                Strsql += " JAE.TRANSPORTER_CARRIER_FK ,                    ";
                Strsql += " TRANSPORTER.VENDOR_NAME ,                  ";
                Strsql += " ' '                                             ";

                return ObjWF.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Data for Standard Shipping Note"

        #region "Fetch Volume and Gross Weight"

        /// <summary>
        /// Fetches the volume.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public Int32 FetchVolume(string JOBPK)
        {
            string Strsql = null;
            Int32 TotVol = 0;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = " select sum(nvl(j.volume_in_cbm,0)) ";
                Strsql += " from JOB_TRN_CONT j where j.JOB_CARD_TRN_FK=" + JOBPK;
                TotVol = Convert.ToInt32(ObjWF.ExecuteScaler(Strsql));
                return TotVol;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the gross wt.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public Int32 FetchGrossWt(string JOBPK)
        {
            string Strsql = null;
            Int32 TotGrossWt = 0;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = " select sum(nvl(j.gross_weight,0)) ";
                Strsql += " from JOB_TRN_CONT j where j.JOB_CARD_TRN_FK=" + JOBPK;
                TotGrossWt = Convert.ToInt32(ObjWF.ExecuteScaler(Strsql));
                return TotGrossWt;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Volume and Gross Weight"

        #region "Enhance Search Function"

        /// <summary>
        /// Fetches the country.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCountry(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            var strNull = "";
            arr = strCond.Split('~');

            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_COUNTRY_PKG.GETCOUNTRY_COMMON";

                var _with57 = selectCommand.Parameters;
                _with57.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with57.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with57.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
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

        /// <summary>
        /// Fetches the master job card air.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchMasterJobCardAir(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strReq = null;
            string POL = null;
            string POD = null;
            string DPAgent = null;

            var strNull = "";
            arr = strCond.Split('~');

            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            POL = Convert.ToString(arr.GetValue(2));
            POD = Convert.ToString(arr.GetValue(3));
            // DPAgent = arr(4)

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBCARD_MASTER_PKG.GET_MASTERJOBCARDAIR";

                var _with58 = selectCommand.Parameters;

                _with58.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with58.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with58.Add("POL_IN", POL).Direction = ParameterDirection.Input;
                _with58.Add("POD_IN", POD).Direction = ParameterDirection.Input;
                _with58.Add("LOC_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                //Snigdharani - 15/12/2008
                // .Add("DP_AGENT_IN", DPAgent).Direction = ParameterDirection.Input
                _with58.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;

                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
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

        #endregion "Enhance Search Function"

        #region " Fetch Max Contract No."

        /// <summary>
        /// Fetches the job card reference no.
        /// </summary>
        /// <param name="strMasterJobCardNo">The string master job card no.</param>
        /// <returns></returns>
        public string FetchJobCardReferenceNo(string strMasterJobCardNo)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                string strResult = null;

                strSQL = " SELECT NVL(MAX(T.jobcard_ref_no),0) FROM job_card_air_exp_tbl T " + " WHERE t.master_jc_air_exp_fk = " + strMasterJobCardNo + " ORDER BY T.jobcard_ref_no ";

                strResult = objWF.ExecuteScaler(strSQL);

                if (strResult == "0")
                {
                    strSQL = " select m.master_jc_ref_no from master_jc_air_exp_tbl m " + " where m.master_jc_air_exp_pk = " + strMasterJobCardNo;
                    strResult = objWF.ExecuteScaler(strSQL);
                }

                return strResult;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Fetch Max Contract No."

        #region "Get Invoice PK"

        //0 - means no invoice exists.
        //-1 - means more then one invoice extists
        //pk value of invoice

        //invice Type: 1-Invoice to customer.
        //invice Type: 2-Invoice to CB Agent.
        //invice Type: 3-Invoice to DP Agent.
        /// <summary>
        /// Gets the customer invoice.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="invoiceType">Type of the invoice.</param>
        /// <returns></returns>
        public long GetCustInvoice(string jobCardPK, Int16 invoiceType = 1)
        {
            System.Text.StringBuilder SQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            OracleDataReader oraReader = null;
            int invoiceCount = 0;
            long invoicePK = 0;

            if (invoiceType == 1)
            {
                //SQL.Append(vbCrLf & "select i.inv_cust_air_exp_pk from inv_cust_air_exp_tbl i where i.job_card_air_exp_fk = " & jobCardPK)
                SQL.Append("SELECT CON.CONSOL_INVOICE_PK");
                SQL.Append("  FROM CONSOL_INVOICE_TBL CON, CONSOL_INVOICE_TRN_TBL CONTRN");
                SQL.Append(" WHERE CON.CONSOL_INVOICE_PK = CONTRN.CONSOL_INVOICE_FK");
                SQL.Append("  AND CON.BUSINESS_TYPE = 1");
                SQL.Append("  AND CON.PROCESS_TYPE = 1");
                SQL.Append("   AND CONTRN.JOB_CARD_FK = " + jobCardPK);
            }
            else if (invoiceType == 2)
            {
                SQL.Append("select i.inv_agent_pk from inv_agent_tbl i where i.CB_DP_LOAD_AGENT=1 AND  i.job_card_fk = " + jobCardPK);
            }
            else if (invoiceType == 3)
            {
                SQL.Append("select i.inv_agent_pk from inv_agent_tbl i where  i.CB_DP_LOAD_AGENT=2 AND i.job_card_fk = " + jobCardPK);
            }

            oraReader = objWF.GetDataReader(SQL.ToString());

            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    invoicePK = Convert.ToInt64(oraReader[0]);
                    invoiceCount += 1;
                }
            }

            if (invoiceCount == 0)
            {
                return 0;
            }
            else if (invoiceCount > 1)
            {
                return -1;
            }
            else
            {
                return invoicePK;
            }

            oraReader.Close();

            try
            {
                return invoicePK;
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

        #endregion "Get Invoice PK"

        #region "Fetch EnableDisableAirlineStatus & Save Airline in Booking"

        /// <summary>
        /// Funs the e disable airline status.
        /// </summary>
        /// <param name="strBookingRefNo">The string booking reference no.</param>
        /// <returns></returns>
        public string funEDisableAirlineStatus(string strBookingRefNo)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string strReturn = null;
            strBuilder.Append(" SELECT ");
            strBuilder.Append(" BAT.BOOKING_AIR_PK");
            strBuilder.Append(" FROM");
            strBuilder.Append(" BOOKING_AIR_TBL BAT ,");
            strBuilder.Append(" JOB_CARD_AIR_EXP_TBL JHDR");
            strBuilder.Append(" WHERE");
            strBuilder.Append(" BAT.AIRLINE_UPDATE_STATUS=1 ");
            strBuilder.Append(" AND JHDR.BOOKING_AIR_FK=BAT.BOOKING_AIR_PK");
            strBuilder.Append(" AND (JHDR.HAWB_EXP_TBL_FK IS NULL AND JHDR.MAWB_EXP_TBL_FK IS NULL)");
            strBuilder.Append(" AND BAT.BOOKING_REF_NO='" + strBookingRefNo + "'");
            try
            {
                strReturn = objWF.ExecuteScaler(strBuilder.ToString());
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Funs up stream updation booking airline.
        /// </summary>
        /// <param name="strBookingRefNo">The string booking reference no.</param>
        /// <param name="strAirlinePK">The string airline pk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <returns></returns>
        private object funUpStreamUpdationBookingAirline(string strBookingRefNo, string strAirlinePK, OracleTransaction TRAN)
        {
            try
            {
                arrMessage.Clear();
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                OracleCommand OCUpdCmd = new OracleCommand();
                Int16 intReturn = default(Int16);
                var _with59 = OCUpdCmd;
                _with59.CommandType = CommandType.StoredProcedure;
                _with59.CommandText = objWF.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.UPDATE_UPSTREAM_BOOKINGAIRLINE";
                _with59.Connection = TRAN.Connection;
                _with59.Transaction = TRAN;

                _with59.Parameters.Clear();
                _with59.Parameters.Add("BOOKING_REFNO_IN", strBookingRefNo).Direction = ParameterDirection.Input;
                _with59.Parameters.Add("AIRLINE_FK_IN", strAirlinePK).Direction = ParameterDirection.Input;
                _with59.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                intReturn = Convert.ToInt16(_with59.ExecuteNonQuery());

                if (intReturn == 1)
                {
                    arrMessage.Add("All Data Saved Successfully");
                }
                else
                {
                    arrMessage.Add("Upstream updation failed, Check Airline");
                }
                return arrMessage;
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

        #endregion "Fetch EnableDisableAirlineStatus & Save Airline in Booking"

        #region "Update Airway Bill Trn"

        /// <summary>
        /// Update_s the airway_ bill_ TRN.
        /// </summary>
        /// <param name="jobcardNo">The jobcard no.</param>
        /// <param name="BkgNo">The BKG no.</param>
        /// <param name="AirwayBillNo">The airway bill no.</param>
        /// <param name="AirwayPk">The airway pk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <returns></returns>
        public ArrayList Update_Airway_Bill_Trn(string jobcardNo, string BkgNo, string AirwayBillNo, string AirwayPk, OracleTransaction TRAN)
        {
            WorkFlow objWK = new WorkFlow();
            Int16 exe = default(Int16);
            OracleCommand cmd = new OracleCommand();
            System.Text.StringBuilder strQuery = null;
            arrMessage.Clear();
            try
            {
                cmd.CommandType = CommandType.Text;
                cmd.Connection = TRAN.Connection;
                cmd.Transaction = TRAN;
                //add by latha for updating  the mst table for cancelled records

                cmd.Parameters.Clear();

                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_mst_tbl AMT  set ");
                strQuery.Append(" AMT.total_nos_used = (AMT.total_nos_used - 1),  ");
                strQuery.Append("  AMT.total_nos_cancelled = (AMT.total_nos_cancelled + 1)  ");
                strQuery.Append(" Where AMT.Airway_Bill_Mst_Pk in (select abt.airway_bill_mst_fk  from airway_bill_trn ABT Where ABT.REFERENCE_NO = '" + BkgNo + "') ");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                cmd.Parameters.Clear();

                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_mst_tbl AMT  set ");
                strQuery.Append("   AMT.total_nos_used = (AMT.total_nos_used - 1),  ");
                strQuery.Append("   AMT.total_nos_cancelled = (AMT.total_nos_cancelled + 1)  ");
                strQuery.Append(" Where AMT.Airway_Bill_Mst_Pk in (select abt.airway_bill_mst_fk  from airway_bill_trn ABT Where ABT.REFERENCE_NO = '" + jobcardNo + "') ");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                cmd.Parameters.Clear();
                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_trn ABT ");
                strQuery.Append(" set ABT.Status = 0, ABT.Used_At = 0, ABT.Reference_No = Null ");
                strQuery.Append(" Where ABT.REFERENCE_NO = '" + BkgNo + "'");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                cmd.Parameters.Clear();
                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_trn ABT ");
                strQuery.Append(" set ABT.Status = 0, ABT.Used_At = 0, ABT.Reference_No = Null ");
                strQuery.Append(" Where ABT.REFERENCE_NO = '" + jobcardNo + "'");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());
                //add by latha
                if (AirwayPk != null)
                {
                    cmd.Parameters.Clear();

                    strQuery = new System.Text.StringBuilder();
                    strQuery.Append(" update airway_bill_trn ABT ");
                    strQuery.Append("   set ABT.Status       = 3, ");
                    strQuery.Append("       ABT.Used_At      = 4, ");
                    strQuery.Append("       ABT.Reference_No = '" + jobcardNo + "'");
                    strQuery.Append(" Where ABT.Airway_Bill_Mst_Fk = " + AirwayPk);
                    strQuery.Append("   And ABT.AIRWAY_BILL_NO = " + AirwayBillNo);
                    strQuery.Append("");
                    cmd.CommandText = strQuery.ToString();
                    exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                    //'code to update the Airway Bill Master file with the number of used field.
                    cmd.Parameters.Clear();

                    strQuery = new System.Text.StringBuilder();
                    strQuery.Append(" update airway_bill_mst_tbl ABTT ");
                    strQuery.Append(" set ABTT.total_nos_used = ( select count(*) + 1 from airway_bill_trn trn ");
                    strQuery.Append(" where(trn.reference_no Is Not null) and trn.airway_bill_mst_fk= " + AirwayPk);
                    strQuery.Append(") Where ABTT.Airway_Bill_Mst_pk = " + AirwayPk);
                    strQuery.Append("");
                    cmd.CommandText = strQuery.ToString();
                    exe = Convert.ToInt16(cmd.ExecuteNonQuery());
                }

                //    arrMessage.Add("All data saved successfully")
                //    Return arrMessage

                //Catch oraexp As OracleException
                //    arrMessage.Add(oraexp.Message)
                //    Return arrMessage
                //Catch ex As Exception
                //    arrMessage.Add(ex.Message)
                //    Return arrMessage
                //End Try
                if (exe == 1)
                {
                    arrMessage.Add("All Data Saved Successfully");
                }
                else
                {
                    arrMessage.Add("Upstream updation failed, Check MAWB Int32");
                }

                return arrMessage;
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

        #endregion "Update Airway Bill Trn"

        #region "fetch MaWB Nr"

        /// <summary>
        /// Fetch_s the m awb nr.
        /// </summary>
        /// <param name="jobcardNo">The jobcard no.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <returns></returns>
        public DataSet Fetch_MAwbNr(string jobcardNo, long usrLocFK = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                //strQuery.Append("   select abn.airway_bill_no" & vbCrLf)
                //strQuery.Append("     from airway_bill_trn abn" & vbCrLf)
                //strQuery.Append("    where abn.reference_no = '" & jobcardNo & "'" & vbCrLf)
                //strQuery.Append("" & vbCrLf)

                strQuery.Append("   select abn.airway_bill_no");
                strQuery.Append("     from airway_bill_trn abn , airway_bill_mst_tbl am ");
                strQuery.Append("   where abn.airway_bill_mst_fk = am.airway_bill_mst_pk  ");
                strQuery.Append("   and  abn.reference_no = '" + jobcardNo + "'");
                strQuery.Append(" and am.location_mst_fk=" + usrLocFK);
                strQuery.Append("");

                return ObjWk.GetDataSet(strQuery.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "fetch MaWB Nr"

        #region "Fetch Aiwaybill MST Fk "

        /// <summary>
        /// Fecth_s the airway_mst_ fk.
        /// </summary>
        /// <param name="ref_nr">The ref_nr.</param>
        /// <param name="Air_Pk">The air_ pk.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <returns></returns>
        public DataSet fecth_Airway_mst_Fk(string ref_nr, string Air_Pk, long usrLocFK = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                strQuery.Append("select a.airway_bill_mst_fk");
                strQuery.Append("from airway_bill_trn a, airway_bill_mst_tbl am");
                strQuery.Append("where am.airway_bill_mst_pk = a.airway_bill_mst_fk");
                strQuery.Append("and am.airline_mst_fk=" + Air_Pk);
                strQuery.Append("and a.airway_bill_no= '" + ref_nr + "'");
                //add by latha
                strQuery.Append("and am.location_mst_fk =" + usrLocFK);
                strQuery.Append("");

                return ObjWk.GetDataSet(strQuery.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Aiwaybill MST Fk "

        #region "Fetch MJCPK"

        //Added by rabbani
        /// <summary>
        /// Fetches the MJCPK.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public string FetchMJCPK(string jobCardPK = "0")
        {
            string strSQL = null;
            strSQL = "select job_exp.master_jc_air_exp_fk" + "from job_card_air_exp_tbl job_exp" + "where job_exp.job_card_air_exp_pk=" + jobCardPK;
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch MJCPK"

        #region "FETCH FREIGHT"

        /// <summary>
        /// Fetches the freight elemet.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BKGPK">The BKGPK.</param>
        /// <returns></returns>
        public DataSet FetchFreightElemet(string JOBPK, string BKGPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT DISTINCT JFD.FREIGHT_ELEMENT_MST_FK, QFT.CHECK_ADVATOS");
                sb.Append("  FROM JOB_TRN_AIR_EXP_FD         JFD,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL    FMT,");
                sb.Append("       QUOT_AIR_TRN_FREIGHT_TBL   QFT,");
                sb.Append("       QUOTATION_AIR_TBL          QAT,");
                sb.Append("       QUOT_GEN_TRN_AIR_TBL       QTN,");
                sb.Append("       JOB_CARD_AIR_EXP_TBL       JOB,");
                sb.Append("       BOOKING_AIR_TBL            BKG,");
                sb.Append("       BOOKING_TRN_AIR            BTS");
                sb.Append(" WHERE JFD.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("    AND QFT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("    AND JOB.JOB_CARD_AIR_EXP_PK = JFD.JOB_CARD_AIR_EXP_FK");
                sb.Append("    AND JOB.BOOKING_AIR_FK = BKG.BOOKING_AIR_PK");
                sb.Append("    AND BTS.TRANS_REF_NO = QAT.QUOTATION_REF_NO");
                sb.Append("    AND QAT.QUOTATION_AIR_PK = QTN.QUOT_GEN_AIR_FK ");
                sb.Append("    AND QFT.QUOT_GEN_AIR_TRN_FK = QTN.QUOT_GEN_AIR_TRN_PK ");
                sb.Append("    AND BTS.BOOKING_AIR_FK = BKG.BOOKING_AIR_PK ");
                sb.Append("    AND JFD.JOB_CARD_AIR_EXP_FK = " + JOBPK);
                sb.Append("    AND QFT.CHECK_ADVATOS = 1 ");
                sb.Append("    AND JFD.ADVATOS_FLAG = 0 ");
                sb.Append("    AND BKG.BOOKING_AIR_PK =" + BKGPK);
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

        /// <summary>
        /// Fetches the booking pk.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <returns></returns>
        public string FetchBookingPk(string Jobpk)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSQL.Append(" select bkg.booking_air_pk from ");
            strSQL.Append(" booking_air_tbl bkg, ");
            strSQL.Append(" job_card_air_exp_tbl  jair ");
            strSQL.Append(" where jair.booking_air_fk = bkg.booking_air_pk ");
            strSQL.Append(" and jair.job_card_air_exp_pk = " + Jobpk + " ");
            try
            {
                return objWF.ExecuteScaler(strSQL.ToString());
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

        /// <summary>
        /// Updates the advatos.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="FrtPK">The FRT pk.</param>
        public void UpdateADVATOS(string JobPK, int FrtPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            OracleTransaction TRAN = null;
            int nRecAfct = 0;
            string strSQL = null;
            string strSQL1 = null;
            Int16 upd = default(Int16);
            try
            {
                if (string.IsNullOrEmpty(JobPK) | string.IsNullOrEmpty(JobPK))
                {
                    return;
                }
                ObjWk.OpenConnection();
                TRAN = ObjWk.MyConnection.BeginTransaction();
                var _with60 = objCommand;
                _with60.Connection = ObjWk.MyConnection;
                _with60.CommandType = CommandType.Text;
                _with60.CommandText = strSQL;
                _with60.Transaction = TRAN;
                //nRecAfct = .ExecuteNonQuery()
                strSQL1 = "update JOB_TRN_AIR_EXP_FD J set J.ADVATOS_FLAG = 1 where J.JOB_CARD_AIR_EXP_FK= " + JobPK;
                strSQL1 += " AND J.FREIGHT_ELEMENT_MST_FK=" + FrtPK;
                _with60.CommandText = strSQL1;
                upd = Convert.ToInt16(_with60.ExecuteNonQuery());
                if (upd > 0)
                {
                    TRAN.Commit();
                }
                else
                {
                    TRAN.Rollback();
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                ObjWk.MyConnection.Close();
            }
        }

        #endregion "FETCH FREIGHT"

        #region "FETCH FREIGHT ELEMENT PK"

        /// <summary>
        /// Fetches the FRT ele pk.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BKGPK">The BKGPK.</param>
        /// <returns></returns>
        public DataSet FetchFrtElePK(string JOBPK, string BKGPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append(" SELECT DISTINCT JFD.FREIGHT_ELEMENT_MST_FK, QFT.CHECK_ADVATOS");
                sb.Append("  FROM JOB_TRN_AIR_EXP_FD       JFD,");
                sb.Append("      FREIGHT_ELEMENT_MST_TBL  FMT,");
                sb.Append("      QUOTATION_AIR_TBL        QAT,");
                sb.Append("      QUOTATION_TRN_AIR QTN,");
                sb.Append("      QUOTATION_TRN_AIR_FRT_DTLS QFT,");
                sb.Append("      JOB_CARD_AIR_EXP_TBL     JOB,");
                sb.Append("      BOOKING_AIR_TBL            BKG,");
                sb.Append("      BOOKING_TRN_AIR            BTS");
                sb.Append(" WHERE JFD.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("    AND QFT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("    AND JOB.JOB_CARD_AIR_EXP_PK = JFD.JOB_CARD_AIR_EXP_FK");
                sb.Append("    AND JOB.BOOKING_AIR_FK = BKG.BOOKING_AIR_PK");
                sb.Append("    AND BTS.TRANS_REF_NO = QAT.QUOTATION_REF_NO");
                sb.Append("    AND QAT.QUOTATION_AIR_PK=QTN.QUOTATION_AIR_FK ");
                sb.Append("    AND QTN.QUOTE_TRN_AIR_PK=QFT.QUOTE_TRN_AIR_FK ");
                sb.Append("    AND BTS.BOOKING_AIR_FK = BKG.BOOKING_AIR_PK");
                sb.Append("    AND JFD.JOB_CARD_AIR_EXP_FK = " + JOBPK);
                sb.Append("    AND QFT.CHECK_ADVATOS = 1 ");
                sb.Append("    AND BKG.BOOKING_AIR_PK =" + BKGPK);
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

        #endregion "FETCH FREIGHT ELEMENT PK"

        #region "FETCH FREIGHT AMOUNT"

        /// <summary>
        /// Gets the reference details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BKGPK">The BKGPK.</param>
        /// <param name="Quot_Type">Type of the quot_.</param>
        /// <returns></returns>
        public DataSet GetRefDetails(string JOBPK, string BKGPK, int Quot_Type)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT DISTINCT QAT.QUOTATION_REF_NO");
                if (Quot_Type == 0)
                {
                    sb.Append("  ,QGT.TRANS_REFERED_FROM");
                }
                else
                {
                    sb.Append("  ,QTN.TRANS_REFERED_FROM");
                }
                sb.Append("  FROM QUOTATION_AIR_TBL        QAT,");
                if (Quot_Type == 0)
                {
                    sb.Append("       QUOT_GEN_TRN_AIR_TBL     QGT,");
                }
                else
                {
                    sb.Append("       QUOTATION_TRN_AIR     QTN,");
                }
                sb.Append("       BOOKING_AIR_TBL          BKG,");
                sb.Append("       BOOKING_TRN_AIR          BTN,");
                sb.Append("       JOB_CARD_AIR_EXP_TBL     JOB,");
                sb.Append("       JOB_TRN_AIR_EXP_FD       JFD");
                sb.Append(" WHERE ");
                //'General Quotation
                if (Quot_Type == 0)
                {
                    sb.Append("   QAT.QUOTATION_AIR_PK = QGT.QUOT_GEN_AIR_FK");
                    //'Specific Quotation
                }
                else
                {
                    sb.Append("  QAT.QUOTATION_AIR_PK = QTN.QUOTATION_AIR_FK");
                }
                sb.Append("   AND BTN.TRANS_REF_NO = QAT.QUOTATION_REF_NO");
                sb.Append("   AND BKG.BOOKING_AIR_PK = BTN.BOOKING_AIR_FK");
                sb.Append("   AND BKG.BOOKING_AIR_PK = JOB.BOOKING_AIR_FK");
                sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = JFD.JOB_CARD_AIR_EXP_FK");
                sb.Append("   AND BKG.BOOKING_AIR_PK = " + BKGPK + "");
                sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = " + JOBPK + "");
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

        /// <summary>
        /// Gets the air job details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BKGPK">The BKGPK.</param>
        /// <returns></returns>
        public DataSet GetAirJobDetails(string JOBPK, string BKGPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT DISTINCT BKG.PORT_MST_POD_FK,");
                sb.Append("                BKG.PORT_MST_POL_FK,");
                sb.Append("                JCT.PACK_TYPE_MST_FK");
                sb.Append("  FROM BOOKING_AIR_TBL      BKG,");
                sb.Append("       JOB_CARD_AIR_EXP_TBL JOB,");
                sb.Append("       JOB_TRN_AIR_EXP_FD   JFD,");
                sb.Append("       JOB_TRN_AIR_EXP_CONT JCT");
                sb.Append(" WHERE JOB.BOOKING_AIR_FK = BKG.BOOKING_AIR_PK");
                sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = JCT.JOB_CARD_AIR_EXP_FK");
                sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = JFD.JOB_CARD_AIR_EXP_FK");
                sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK =" + JOBPK + "");
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

        /// <summary>
        /// Gets the hd details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BKGPK">The BKGPK.</param>
        /// <param name="RefFrom">The reference from.</param>
        /// <param name="PolFK">The pol fk.</param>
        /// <param name="PodFK">The pod fk.</param>
        /// <param name="Quot_Type">Type of the quot_.</param>
        /// <returns></returns>
        public DataSet GetHDDetails(string JOBPK, string BKGPK, int RefFrom, int PolFK, int PodFK, int Quot_Type)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                //'for Airline Tariff and Gen.Tariff
                if (RefFrom == 5 | RefFrom == 6)
                {
                    sb.Append("SELECT DISTINCT");
                    //'General Quotation
                    if (Quot_Type == 0)
                    {
                        sb.Append("  QGT.REF_NO REFNR,");
                        //'Specific Quotation
                    }
                    else
                    {
                        sb.Append(" QTN.TRANS_REF_NO REFNR,");
                    }
                    sb.Append("                TMA.TARIFF_DATE REFDATE,");
                    sb.Append("                TMA.VALID_FROM,");
                    sb.Append("                TMA.VALID_TO");
                    sb.Append("  FROM QUOTATION_AIR_TBL    QAT,");
                    //'General Quotation
                    if (Quot_Type == 0)
                    {
                        sb.Append("       QUOT_GEN_TRN_AIR_TBL QGT,");
                        //'Specific Quotation  1
                    }
                    else
                    {
                        sb.Append("       QUOTATION_TRN_AIR QTN,");
                    }
                    sb.Append("       TARIFF_MAIN_AIR_TBL  TMA,");
                    sb.Append("       BOOKING_AIR_TBL      BKG,");
                    sb.Append("       BOOKING_TRN_AIR      BTN,");
                    sb.Append("       JOB_CARD_AIR_EXP_TBL JOB,");
                    sb.Append("       JOB_TRN_AIR_EXP_FD   JFD");
                    sb.Append(" WHERE");
                    //'General Quotation
                    if (Quot_Type == 0)
                    {
                        sb.Append("    QAT.QUOTATION_AIR_PK = QGT.QUOT_GEN_AIR_FK");
                        sb.Append("   AND TMA.TARIFF_REF_NO = QGT.REF_NO");
                        //'Specific Quotation
                    }
                    else
                    {
                        sb.Append("    QAT.QUOTATION_AIR_PK=QTN.QUOTATION_AIR_FK");
                        sb.Append("    AND TMA.TARIFF_REF_NO = QTN.TRANS_REF_NO");
                    }
                    sb.Append("   AND BTN.TRANS_REF_NO=QAT.QUOTATION_REF_NO");
                    sb.Append("   AND BKG.BOOKING_AIR_PK = BTN.BOOKING_AIR_FK");
                    sb.Append("   AND BKG.BOOKING_AIR_PK = JOB.BOOKING_AIR_FK");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = JFD.JOB_CARD_AIR_EXP_FK");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK=" + JOBPK + "");
                    sb.Append("   AND BKG.BOOKING_AIR_PK=" + BKGPK + "");

                    //'for Spot Rate
                }
                else if (RefFrom == 1)
                {
                    sb.Append("SELECT DISTINCT");
                    //'General Quotation
                    if (Quot_Type == 0)
                    {
                        sb.Append("  QGT.REF_NO REFNR, ");
                    }
                    else
                    {
                        sb.Append(" QTN.TRANS_REF_NO REFNR,");
                    }
                    sb.Append("                RFQ.RFQ_DATE REFDATE,");
                    sb.Append("                RFQ.VALID_FROM,");
                    sb.Append("                RFQ.VALID_TO");
                    sb.Append("  FROM QUOTATION_AIR_TBL    QAT,");
                    //'General Quotation
                    if (Quot_Type == 0)
                    {
                        sb.Append("       QUOT_GEN_TRN_AIR_TBL QGT,");
                    }
                    else
                    {
                        sb.Append("       QUOTATION_TRN_AIR QTN,");
                    }
                    sb.Append("       RFQ_SPOT_RATE_AIR_TBL RFQ,");
                    sb.Append("       BOOKING_AIR_TBL      BKG,");
                    sb.Append("       BOOKING_TRN_AIR      BTN,");
                    sb.Append("       JOB_CARD_AIR_EXP_TBL JOB,");
                    sb.Append("       JOB_TRN_AIR_EXP_FD   JFD");
                    sb.Append(" WHERE ");
                    //'General Quotation
                    if (Quot_Type == 0)
                    {
                        sb.Append("   QAT.QUOTATION_AIR_PK = QGT.QUOT_GEN_AIR_FK");
                        sb.Append("   AND RFQ.RFQ_REF_NO = QGT.REF_NO");
                    }
                    else
                    {
                        sb.Append("    QAT.QUOTATION_AIR_PK=QTN.QUOTATION_AIR_FK");
                        sb.Append("    AND RFQ.RFQ_REF_NO = QTN.TRANS_REF_NO");
                    }
                    sb.Append("   AND BTN.TRANS_REF_NO=QAT.QUOTATION_REF_NO");
                    sb.Append("   AND BKG.BOOKING_AIR_PK = BTN.BOOKING_AIR_FK");
                    sb.Append("   AND BKG.BOOKING_AIR_PK = JOB.BOOKING_AIR_FK");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = JFD.JOB_CARD_AIR_EXP_FK");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK=" + JOBPK + "");
                    sb.Append("   AND BKG.BOOKING_AIR_PK=" + BKGPK + "");

                    //'for Customer Contract
                }
                else if (RefFrom == 2)
                {
                    sb.Append("SELECT DISTINCT");
                    //'General Quotation
                    if (Quot_Type == 0)
                    {
                        sb.Append("   QGT.REF_NO REFNR, ");
                    }
                    else
                    {
                        sb.Append("   QTN.TRANS_REF_NO REFNR,");
                    }
                    sb.Append("                CAT.CONT_DATE REFDATE,");
                    sb.Append("                CAT.VALID_FROM,");
                    sb.Append("                CAT.VALID_TO");
                    sb.Append("  FROM QUOTATION_AIR_TBL    QAT,");
                    if (Quot_Type == 0)
                    {
                        sb.Append("       QUOT_GEN_TRN_AIR_TBL QGT,");
                    }
                    else
                    {
                        sb.Append("       QUOTATION_TRN_AIR QTN,");
                    }
                    sb.Append("       CONT_CUST_AIR_TBL     CAT,");
                    sb.Append("       BOOKING_AIR_TBL      BKG,");
                    sb.Append("       BOOKING_TRN_AIR      BTN,");
                    sb.Append("       JOB_CARD_AIR_EXP_TBL JOB,");
                    sb.Append("       JOB_TRN_AIR_EXP_FD   JFD");
                    sb.Append(" WHERE ");
                    if (Quot_Type == 0)
                    {
                        sb.Append("   QAT.QUOTATION_AIR_PK = QGT.QUOT_GEN_AIR_FK");
                        sb.Append("   AND CAT.CONT_REF_NO = QGT.REF_NO");
                    }
                    else
                    {
                        sb.Append("    QAT.QUOTATION_AIR_PK=QTN.QUOTATION_AIR_FK");
                        sb.Append("    AND CAT.CONT_REF_NO = QTN.TRANS_REF_NO");
                    }

                    sb.Append("   AND BTN.TRANS_REF_NO=QAT.QUOTATION_REF_NO");
                    sb.Append("   AND BKG.BOOKING_AIR_PK = BTN.BOOKING_AIR_FK");
                    sb.Append("   AND BKG.BOOKING_AIR_PK = JOB.BOOKING_AIR_FK");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = JFD.JOB_CARD_AIR_EXP_FK");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK=" + JOBPK + "");
                    sb.Append("   AND BKG.BOOKING_AIR_PK=" + BKGPK + "");
                    //'Getting Freight Amount from Gen.Tariff
                }
                else
                {
                    sb.Append("SELECT DISTINCT TMS.TARIFF_REF_NO REFNR,");
                    sb.Append("                TMS.TARIFF_DATE REFDATE,");
                    sb.Append("                TMS.VALID_FROM,");
                    sb.Append("                TMS.VALID_TO");
                    sb.Append("   FROM TARIFF_MAIN_AIR_TBL TMS,TARIFF_TRN_AIR_TBL TNS");
                    sb.Append("   WHERE   TMS.TARIFF_MAIN_AIR_PK=TNS.TARIFF_MAIN_AIR_FK");
                    sb.Append("   AND TMS.TARIFF_TYPE=2");
                    sb.Append("   AND TMS.ACTIVE=1");
                    sb.Append("   AND (TMS.VALID_TO >= TO_DATE(SYSDATE,'" + dateFormat + "') OR");
                    sb.Append("       TMS.VALID_TO IS NULL)");
                    sb.Append("   AND TNS.PORT_MST_POL_FK=" + PolFK + "");
                    sb.Append("   AND TNS.PORT_MST_POD_FK=" + PodFK + "");
                }
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

        /// <summary>
        /// Fetches the air freight amount.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BKGPK">The BKGPK.</param>
        /// <param name="RefFrom">The reference from.</param>
        /// <param name="PolFK">The pol fk.</param>
        /// <param name="PodFK">The pod fk.</param>
        /// <param name="Quot_Type">Type of the quot_.</param>
        /// <returns></returns>
        public DataSet FetchAirFreightAmount(string JOBPK, string BKGPK, int RefFrom, int PolFK, int PodFK, int Quot_Type)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                //'For Airline Tariff and Gen.Tariff
                if ((RefFrom == 5 | RefFrom == 6))
                {
                    sb.Append("SELECT DISTINCT '' SLNO,JOB.JOB_CARD_AIR_EXP_PK,");
                    sb.Append("                TNF.FREIGHT_ELEMENT_MST_FK,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_ID,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_NAME,");
                    sb.Append("                TNF.CURRENCY_MST_FK,");
                    sb.Append("                CMT.CURRENCY_ID,");
                    sb.Append("                TNF.MIN_AMOUNT,");
                    sb.Append("                '' SEL");
                    sb.Append("  FROM QUOTATION_AIR_TBL          QAT,");
                    //'General Quotation
                    if (Quot_Type == 0)
                    {
                        sb.Append("       QUOT_GEN_TRN_AIR_TBL       QGT,");
                        sb.Append("       QUOT_AIR_TRN_FREIGHT_TBL   QTF,");
                        //'Specific Quotation
                    }
                    else
                    {
                        sb.Append("       QUOTATION_TRN_AIR     QTN,");
                        sb.Append("       QUOTATION_TRN_AIR_FRT_DTLS QFT,");
                    }
                    sb.Append("       TARIFF_MAIN_AIR_TBL        TMS,");
                    sb.Append("       TARIFF_TRN_AIR_TBL         TNS,");
                    sb.Append("       TARIFF_TRN_AIR_FREIGHT_TBL TNF,");
                    sb.Append("       BOOKING_AIR_TBL            BKG,");
                    sb.Append("       BOOKING_TRN_AIR            BTN,");
                    sb.Append("       JOB_CARD_AIR_EXP_TBL       JOB,");
                    sb.Append("       JOB_TRN_AIR_EXP_FD         JFD,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL    FMT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL      CMT");
                    sb.Append("  WHERE ");
                    //'General Quotation
                    if (Quot_Type == 0)
                    {
                        sb.Append("   QAT.QUOTATION_AIR_PK = QGT.QUOT_GEN_AIR_FK");
                        sb.Append("   AND QGT.QUOT_GEN_AIR_TRN_PK = QTF.QUOT_GEN_AIR_TRN_FK");
                        sb.Append("   AND QGT.REF_NO = TMS.TARIFF_REF_NO");
                    }
                    else
                    {
                        sb.Append("   QAT.QUOTATION_AIR_PK=QTN.QUOTATION_AIR_FK");
                        sb.Append("   AND QTN.QUOTE_TRN_AIR_PK=QFT.QUOTE_TRN_AIR_FK");
                        sb.Append("   AND QTN.TRANS_REF_NO = TMS.TARIFF_REF_NO");
                    }
                    sb.Append("   AND BKG.BOOKING_AIR_PK = BTN.BOOKING_AIR_FK");
                    sb.Append("   AND BKG.BOOKING_AIR_PK = JOB.BOOKING_AIR_FK");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = JFD.JOB_CARD_AIR_EXP_FK");
                    sb.Append("   AND BTN.TRANS_REF_NO = QAT.QUOTATION_REF_NO");
                    sb.Append("   AND TMS.TARIFF_MAIN_AIR_PK = TNS.TARIFF_MAIN_AIR_FK");
                    sb.Append("   AND TNF.TARIFF_TRN_AIR_FK = TNS.TARIFF_TRN_AIR_PK");
                    sb.Append("   AND TNF.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                    sb.Append("   AND TNF.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK=" + JOBPK + "");
                    sb.Append("   AND BKG.BOOKING_AIR_PK=" + BKGPK + "");

                    //'For Spot Rate
                }
                else if (RefFrom == 1)
                {
                    sb.Append("SELECT DISTINCT '' SLNO,");
                    sb.Append("                JOB.JOB_CARD_AIR_EXP_PK,");
                    sb.Append("                RFT.FREIGHT_ELEMENT_MST_FK,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_ID,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_NAME,");
                    sb.Append("                RFT.CURRENCY_MST_FK,");
                    sb.Append("                CMT.CURRENCY_ID,");
                    sb.Append("                RFT.MIN_AMOUNT,");
                    sb.Append("                '' SEL");
                    sb.Append("  FROM QUOTATION_AIR_TBL            QAT,");
                    //'General Quotation
                    if (Quot_Type == 0)
                    {
                        sb.Append("       QUOT_GEN_TRN_AIR_TBL         QGT,");
                        ///Specific Quotation
                    }
                    else
                    {
                        sb.Append("    QUOTATION_TRN_AIR     QTN,");
                    }
                    sb.Append("       RFQ_SPOT_RATE_AIR_TBL        RFQ,");
                    sb.Append("       RFQ_SPOT_TRN_AIR_TBL         RFA,");
                    sb.Append("       RFQ_SPOT_AIR_TRN_FREIGHT_TBL RFT,");
                    sb.Append("       BOOKING_AIR_TBL              BKG,");
                    sb.Append("       BOOKING_TRN_AIR              BTN,");
                    sb.Append("       JOB_CARD_AIR_EXP_TBL         JOB,");
                    sb.Append("       JOB_TRN_AIR_EXP_FD           JFD,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL      FMT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL        CMT");
                    sb.Append(" WHERE");
                    //'General Quotation
                    if (Quot_Type == 0)
                    {
                        sb.Append("   QAT.QUOTATION_AIR_PK = QGT.QUOT_GEN_AIR_FK");
                        sb.Append("   AND QGT.REF_NO = RFQ.RFQ_REF_NO");
                    }
                    else
                    {
                        sb.Append("   QAT.QUOTATION_AIR_PK = QTN.QUOTATION_AIR_FK");
                        sb.Append("   AND QTN.TRANS_REF_NO = RFQ.RFQ_REF_NO");
                    }
                    sb.Append("   AND BKG.BOOKING_AIR_PK = BTN.BOOKING_AIR_FK");
                    sb.Append("   AND BKG.BOOKING_AIR_PK = JOB.BOOKING_AIR_FK");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = JFD.JOB_CARD_AIR_EXP_FK");
                    sb.Append("   AND BTN.TRANS_REF_NO = QAT.QUOTATION_REF_NO");
                    sb.Append("   AND RFA.RFQ_SPOT_AIR_FK = RFQ.RFQ_SPOT_AIR_PK");
                    sb.Append("   AND RFT.RFQ_SPOT_TRN_AIR_FK = RFA.RFQ_SPOT_AIR_TRN_PK");
                    sb.Append("   AND RFT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                    sb.Append("   AND RFT.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK=" + JOBPK + "");
                    sb.Append("   AND BKG.BOOKING_AIR_PK=" + BKGPK + "");

                    //'for Customer Contract
                }
                else if (RefFrom == 2)
                {
                    sb.Append("SELECT DISTINCT '' SLNO,");
                    sb.Append("                JOB.JOB_CARD_AIR_EXP_PK,");
                    sb.Append("                CAF.FREIGHT_ELEMENT_MST_FK,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_ID,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_NAME,");
                    sb.Append("                CAF.CURRENCY_MST_FK,");
                    sb.Append("                CMT.CURRENCY_ID,");
                    sb.Append("                CAF.MIN_AMOUNT,");
                    sb.Append("                '' SEL");
                    sb.Append("  FROM QUOTATION_AIR_TBL            QAT,");
                    //'General Quotation
                    if (Quot_Type == 0)
                    {
                        sb.Append("       QUOT_GEN_TRN_AIR_TBL         QGT,");
                    }
                    else
                    {
                        sb.Append("    QUOTATION_TRN_AIR     QTN,");
                    }
                    sb.Append("       CONT_CUST_AIR_TBL     CAT,");
                    sb.Append("       CONT_CUST_TRN_AIR_TBL CTN,");
                    sb.Append("       CONT_CUST_AIR_FREIGHT_TBL CAF,");
                    sb.Append("       RFQ_SPOT_TRN_AIR_TBL         RFA,");
                    sb.Append("       RFQ_SPOT_AIR_TRN_FREIGHT_TBL RFT,");
                    sb.Append("       BOOKING_AIR_TBL              BKG,");
                    sb.Append("       BOOKING_TRN_AIR              BTN,");
                    sb.Append("       JOB_CARD_AIR_EXP_TBL         JOB,");
                    sb.Append("       JOB_TRN_AIR_EXP_FD           JFD,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL      FMT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL        CMT");
                    sb.Append("  WHERE ");
                    //'General Quotation
                    if (Quot_Type == 0)
                    {
                        sb.Append("    QAT.QUOTATION_AIR_PK = QGT.QUOT_GEN_AIR_FK");
                        sb.Append("   AND QGT.REF_NO = CAT.CONT_REF_NO");
                    }
                    else
                    {
                        sb.Append("   QAT.QUOTATION_AIR_PK = QTN.QUOTATION_AIR_FK");
                        sb.Append("   AND QTN.TRANS_REF_NO = CAT.CONT_REF_NO");
                    }
                    sb.Append("   AND BKG.BOOKING_AIR_PK = BTN.BOOKING_AIR_FK");
                    sb.Append("   AND BKG.BOOKING_AIR_PK = JOB.BOOKING_AIR_FK");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = JFD.JOB_CARD_AIR_EXP_FK");
                    sb.Append("   AND BTN.TRANS_REF_NO = QAT.QUOTATION_REF_NO");
                    sb.Append("   AND CAT.CONT_CUST_AIR_PK=CTN.CONT_CUST_AIR_FK");
                    sb.Append("   AND CTN.CONT_CUST_AIR_FK=CAF.CONT_CUST_TRN_AIR_FK");
                    sb.Append("    AND CAF.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                    sb.Append("   AND CAF.CURRENCY_MST_FK = CMT.CURRENCY_MST_PK");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK=" + JOBPK + "");
                    sb.Append("   AND BKG.BOOKING_AIR_PK=" + BKGPK + "");

                    //'For Fetching Freight Amount from Gen.Tariff
                }
                else
                {
                    sb.Append("SELECT DISTINCT '' SLNO,");
                    sb.Append("                JOB.JOB_CARD_AIR_EXP_PK,");
                    sb.Append("                TNF.FREIGHT_ELEMENT_MST_FK,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_ID,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_NAME,");
                    sb.Append("                TNF.CURRENCY_MST_FK,");
                    sb.Append("                CMT.CURRENCY_ID,");
                    sb.Append("                TNF.MIN_AMOUNT,");
                    sb.Append("                '' SEL");
                    sb.Append("  FROM TARIFF_MAIN_AIR_TBL        TMS,");
                    sb.Append("       TARIFF_TRN_AIR_TBL         TNS,");
                    sb.Append("       TARIFF_TRN_AIR_FREIGHT_TBL TNF,");
                    sb.Append("       BOOKING_AIR_TBL            BKG,");
                    sb.Append("       JOB_CARD_AIR_EXP_TBL       JOB,");
                    sb.Append("       JOB_TRN_AIR_EXP_FD         JFD,");
                    sb.Append("       FREIGHT_ELEMENT_MST_TBL    FMT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL      CMT");
                    sb.Append(" WHERE TMS.TARIFF_MAIN_AIR_PK = TNS.TARIFF_MAIN_AIR_FK");
                    sb.Append("   AND TNF.TARIFF_TRN_AIR_FK = TNS.TARIFF_TRN_AIR_PK");
                    sb.Append("   AND BKG.BOOKING_AIR_PK = JOB.BOOKING_AIR_FK");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = JFD.JOB_CARD_AIR_EXP_FK");
                    sb.Append("   AND TNF.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                    sb.Append("   AND CMT.CURRENCY_MST_PK = TNF.CURRENCY_MST_FK");
                    sb.Append("   AND TMS.TARIFF_TYPE = 2");
                    sb.Append("   AND TMS.ACTIVE = 1");
                    sb.Append("   AND (TMS.VALID_TO >= TO_DATE(SYSDATE, 'DD/MM/YYYY') OR");
                    sb.Append("       TMS.VALID_TO IS NULL)");
                    sb.Append("   AND TNS.PORT_MST_POL_FK=" + PolFK + "");
                    sb.Append("   AND TNS.PORT_MST_POD_FK=" + PodFK + "");
                    sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = " + JOBPK + "");
                    sb.Append("   AND BKG.BOOKING_AIR_PK = " + BKGPK + "");
                }
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

        /// <summary>
        /// Gets the type of the quot.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <param name="bkgpk">The BKGPK.</param>
        /// <returns></returns>
        public int GetQuotType(string Jobpk, string bkgpk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT DISTINCT QAT.QUOTATION_TYPE");
            sb.Append("  FROM QUOTATION_AIR_TBL QAT,");
            sb.Append("       BOOKING_AIR_TBL      BKG,");
            sb.Append("       BOOKING_TRN_AIR      BTN,");
            sb.Append("       JOB_CARD_AIR_EXP_TBL JOB,");
            sb.Append("       JOB_TRN_AIR_EXP_FD   JFD");
            sb.Append(" WHERE BTN.TRANS_REF_NO = QAT.QUOTATION_REF_NO");
            sb.Append("   AND BKG.BOOKING_AIR_PK = BTN.BOOKING_AIR_FK");
            sb.Append("   AND BKG.BOOKING_AIR_PK = JOB.BOOKING_AIR_FK");
            sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = JFD.JOB_CARD_AIR_EXP_FK");
            sb.Append("   AND BKG.BOOKING_AIR_PK = " + bkgpk + "");
            sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = " + Jobpk + "");
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "FETCH FREIGHT AMOUNT"

        /// <summary>
        /// Fetches the quot status.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <returns></returns>
        public int FetchQuotStatus(string Jobpk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT CASE");
            sb.Append("         WHEN QTN_DATE < SYSDATE THEN");
            sb.Append("          1");
            sb.Append("         ELSE");
            sb.Append("          0");
            sb.Append("       END QUOTFLAG");
            sb.Append("  FROM (SELECT JOB.JOB_CARD_AIR_EXP_PK,");
            sb.Append("               BTN.TRANS_REFERED_FROM,");
            sb.Append("               NVL((SELECT Q.EXPECTED_SHIPMENT + Q.VALID_FOR");
            sb.Append("                     FROM QUOTATION_AIR_TBL Q");
            sb.Append("                    WHERE Q.QUOTATION_REF_NO = BTN.TRANS_REF_NO),");
            sb.Append("                   '') QTN_DATE");
            sb.Append("          FROM JOB_CARD_AIR_EXP_TBL JOB,");
            sb.Append("               BOOKING_AIR_TBL      BKG,");
            sb.Append("               BOOKING_TRN_AIR      BTN");
            sb.Append("         WHERE JOB.BOOKING_AIR_FK = BKG.BOOKING_AIR_PK");
            sb.Append("           AND BKG.BOOKING_AIR_PK = BTN.BOOKING_AIR_FK");
            sb.Append("           AND JOB.JOB_CARD_AIR_EXP_PK = " + Jobpk + ")");
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region "Calculate_TAX"

        /// <summary>
        /// Calculate_s the tax.
        /// </summary>
        /// <param name="jobCardID">The job card identifier.</param>
        /// <returns></returns>
        public object Calculate_TAX(string jobCardID = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append(" SELECT  NVL(SUM(JP.TAX_AMT), 0) AS COST_TAX");
                sb.Append("   FROM JOB_CARD_AIR_EXP_TBL   JC,");
                sb.Append("        JOB_TRN_AIR_EXP_PIA    JP");
                sb.Append("  WHERE ");
                sb.Append("     JC.JOB_CARD_AIR_EXP_PK = " + jobCardID + "");
                sb.Append("    AND JC.JOB_CARD_AIR_EXP_PK = JP.JOB_CARD_AIR_EXP_FK(+)");

                return (objWF.GetDataSet(sb.ToString()));
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

        /// <summary>
        /// Calculate_s the ta x_ cost.
        /// </summary>
        /// <param name="jobCardID">The job card identifier.</param>
        /// <returns></returns>
        public object Calculate_TAX_Cost(string jobCardID = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append(" SELECT NVL(SUM(CI.TAX_AMT), 0) AS REVENUE_TAX");
                sb.Append("   FROM JOB_CARD_AIR_EXP_TBL   JC,");
                sb.Append("        CONSOL_INVOICE_TRN_TBL CI,");
                sb.Append("        CONSOL_INVOICE_TBL     CON");
                sb.Append("  WHERE JC.JOB_CARD_AIR_EXP_PK = CI.JOB_CARD_FK(+)");
                sb.Append("    AND CON.CONSOL_INVOICE_PK = CI.CONSOL_INVOICE_FK");
                sb.Append("    AND JC.JOB_CARD_AIR_EXP_PK = " + jobCardID + "");
                sb.Append("    AND CON.BUSINESS_TYPE = 1");
                sb.Append("    AND CON.PROCESS_TYPE = 1");
                return (objWF.GetDataSet(sb.ToString()));
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

        #endregion "Calculate_TAX"

        #region "FETCH TARIFF FOR VATOS"

        /// <summary>
        /// Fetches the air line tariff.
        /// </summary>
        /// <param name="AirlinePk">The airline pk.</param>
        /// <param name="CommdityPk">The commdity pk.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <returns></returns>
        public string FetchAirLineTariff(Int32 AirlinePk, Int32 CommdityPk, string strCondition)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            string ValidFrom = null;
            string ValidTo = null;
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.GETAIRLINETARIFF_AIR_VATOS";
                var _with61 = selectCommand.Parameters;
                _with61.Add("AIRLINE_PK_IN", AirlinePk).Direction = ParameterDirection.Input;
                _with61.Add("COMMODITY_GROUP_PK_IN", CommdityPk).Direction = ParameterDirection.Input;
                _with61.Add("LOOKUP_VALUE_IN", "E").Direction = ParameterDirection.Input;
                _with61.Add("CONDITION_IN", strCondition).Direction = ParameterDirection.Input;
                _with61.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                return "~" + ex.Message;
            }
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        /// <summary>
        /// Fetches the airline tariff rates.
        /// </summary>
        /// <param name="TariffPK">The tariff pk.</param>
        /// <param name="strCondition">The string condition.</param>
        /// <returns></returns>
        public DataSet FetchAirlineTariffRates(Int64 TariffPK, string strCondition)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append(" SELECT FRT.FREIGHT_ELEMENT_MST_PK,");
            sb.Append("        FRT.FREIGHT_ELEMENT_ID,");
            sb.Append("        CURR.CURRENCY_ID,");
            sb.Append("        NVL(TFT.MIN_AMOUNT, 0.00) AS APPROVED_RATE,");
            sb.Append("        CURR.CURRENCY_MST_PK,");
            sb.Append("        GET_EX_RATE(CURR.CURRENCY_MST_PK,  " + HttpContext.Current.Session["currency_mst_pk"] + ", SYSDATE) ROE,");
            sb.Append("       (NVL(TFT.MIN_AMOUNT, 0.00) *");
            sb.Append("        GET_EX_RATE(CURR.CURRENCY_MST_PK,  " + HttpContext.Current.Session["currency_mst_pk"] + ", SYSDATE)) FINAL_RATE");
            sb.Append("        FROM TARIFF_TRN_AIR_TBL  TRN,");
            sb.Append("        TARIFF_MAIN_AIR_TBL     TMT,");
            sb.Append("        TARIFF_TRN_AIR_FREIGHT_TBL TFT,");
            sb.Append("        CURRENCY_TYPE_MST_TBL   CURR,");
            sb.Append("        FREIGHT_ELEMENT_MST_TBL FRT");
            sb.Append("        WHERE TRN.TARIFF_MAIN_AIR_FK = " + TariffPK + "");
            sb.Append("        AND TMT.TARIFF_MAIN_AIR_PK = TRN.TARIFF_MAIN_AIR_FK");
            sb.Append("        AND TFT.CURRENCY_MST_FK= CURR.CURRENCY_MST_PK");
            sb.Append("        AND TRN.TARIFF_TRN_AIR_PK = TFT.TARIFF_TRN_AIR_FK");
            sb.Append("        AND TFT.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("        AND FRT.FREIGHT_ELEMENT_ID <> 'AFC'");
            sb.Append("        AND TFT.MIN_AMOUNT > 0");
            sb.Append("        AND (TRN.PORT_MST_POL_FK, TRN.PORT_MST_POD_FK) IN");
            sb.Append("        (" + strCondition + " ) ");
            sb.Append("        ORDER BY FRT.PREFERENCE");
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

        #endregion "FETCH TARIFF FOR VATOS"

        #region "Fetch Income and Expense Details"

        /// <summary>
        /// Fetches the sec ser income details.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <param name="CurFK">The current fk.</param>
        /// <returns></returns>
        public DataSet FetchSecSerIncomeDetails(string Jobpk, int CurFK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtTotalAmt = new DataTable();
            DataTable dtChargeDet = new DataTable();
            DataSet dsIncomeDet = new DataSet();
            int CurrencyPK = 0;
            if (CurFK > 0)
            {
                CurrencyPK = CurFK;
            }
            else
            {
                CurrencyPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            }
            //Parent Details
            try
            {
                var _with62 = objWF.MyCommand.Parameters;
                _with62.Clear();
                _with62.Add("JOB_CARD_AIR_EXP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with62.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with62.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTotalAmt = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "INCOME_MAIN_AIR_EXP");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }

            //Child Details
            try
            {
                var _with63 = objWF.MyCommand.Parameters;
                _with63.Clear();
                _with63.Add("JOB_CARD_AIR_EXP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with63.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with63.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtChargeDet = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "INCOME_CHILD_AIR_EXP");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }

            try
            {
                dsIncomeDet.Tables.Add(dtTotalAmt);
                dsIncomeDet.Tables.Add(dtChargeDet);
                dsIncomeDet.Tables[0].TableName = "TOTAL_AMOUNT";
                dsIncomeDet.Tables[1].TableName = "CHARGE_DETAILS";
                var rel_TotAmtAndCharge = new DataRelation("rel1", dsIncomeDet.Tables["TOTAL_AMOUNT"].Columns["FLAG"], dsIncomeDet.Tables["CHARGE_DETAILS"].Columns["FLAG"]);
                dsIncomeDet.Relations.Add(rel_TotAmtAndCharge);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return dsIncomeDet;
        }

        /// <summary>
        /// Fetches the sec ser expense details.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <param name="CurFK">The current fk.</param>
        /// <returns></returns>
        public DataSet FetchSecSerExpenseDetails(string Jobpk, int CurFK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtTotalAmt = new DataTable();
            DataTable dtChargeDet = new DataTable();
            DataSet dsExpenseDet = new DataSet();
            int CurrencyPK = 0;
            if (CurFK > 0)
            {
                CurrencyPK = CurFK;
            }
            else
            {
                CurrencyPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            }
            //Parent Details
            try
            {
                var _with64 = objWF.MyCommand.Parameters;
                _with64.Add("JOB_CARD_AIR_EXP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with64.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with64.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTotalAmt = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "EXPENSE_MAIN_AIR_EXP");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }

            //Child Details
            try
            {
                var _with65 = objWF.MyCommand.Parameters;
                _with65.Clear();
                _with65.Add("JOB_CARD_AIR_EXP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with65.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with65.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtChargeDet = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "EXPENSE_CHILD_AIR_EXP");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }

            try
            {
                dsExpenseDet.Tables.Add(dtTotalAmt);
                dsExpenseDet.Tables.Add(dtChargeDet);
                dsExpenseDet.Tables[0].TableName = "TOTAL_AMOUNT";
                dsExpenseDet.Tables[1].TableName = "CHARGE_DETAILS";
                var rel_TotAmtAndCharge = new DataRelation("rel1", dsExpenseDet.Tables["TOTAL_AMOUNT"].Columns["FLAG"], dsExpenseDet.Tables["CHARGE_DETAILS"].Columns["FLAG"]);
                dsExpenseDet.Relations.Add(rel_TotAmtAndCharge);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return dsExpenseDet;
        }

        #endregion "Fetch Income and Expense Details"

        #region "HAWB status"

        /// <summary>
        /// Fetches the hawb status.
        /// </summary>
        /// <param name="hawbpk">The hawbpk.</param>
        /// <returns></returns>
        public int FetchHAWBStatus(Int32 hawbpk = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            sb.Append("SELECT NVL(HAWB_STATUS,0) AS STATUS ");
            sb.Append("          FROM HAWB_EXP_TBL HET");
            sb.Append("         WHERE HET.HAWB_EXP_TBL_PK=" + hawbpk);
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "HAWB status"
    }
}