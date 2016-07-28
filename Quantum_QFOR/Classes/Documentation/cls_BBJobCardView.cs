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
    public class cls_BBJobCardView : CommonFeatures
    {
        private long _PkValueTrans;
        cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();

        cls_SeaBookingEntry objVesselVoyage = new cls_SeaBookingEntry();
        #region "GetMainBookingData"
        //Function gets the Booking data for particular BookingPK 
        public DataSet GetMainBookingData(string bookingPK)
        {

            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("      DISTINCT bst.booking_sea_pk,");
            SQL.Append("      bst.booking_ref_no,");
            SQL.Append("      bst.cargo_type,cust.customer_id,");
            SQL.Append("      cust.customer_name, cust.del_address,");
            SQL.Append("      bst.cust_customer_mst_fk,");
            SQL.Append("      col_place.place_name AS \"CollectionPlace\",");
            SQL.Append("      bst.col_place_mst_fk,");
            SQL.Append("      del_place.place_name AS \"DeliveryPlace\",");
            SQL.Append("      bst.del_place_mst_fk,");
            SQL.Append("      POL.port_name as \"POL\",");
            SQL.Append("      bst.port_mst_pol_fk AS \"POLPK\",");
            SQL.Append("      POD.port_name as \"POD\",");
            SQL.Append("      bst.port_mst_pod_fk AS \"PODPK\",");
            SQL.Append("      oprator.operator_id,");
            SQL.Append("      oprator.operator_name,");
            SQL.Append("      bst.operator_mst_fk,");
            //SQL.Append(vbCrLf & "      bst.vessel_name,")
            //SQL.Append(vbCrLf & "      bst.voyage,")
            SQL.Append("      VVT.VOYAGE_TRN_PK \"VoyagePK\",");
            SQL.Append("      V.VESSEL_NAME,");
            SQL.Append("      VVT.VOYAGE,");
            SQL.Append("      bst.eta_date,");
            SQL.Append("      bst.etd_date,");

            SQL.Append("      (   CASE WHEN bst.cust_customer_mst_fk IN ");
            SQL.Append("                                         (SELECT");
            SQL.Append("                                                 C.CUSTOMER_MST_PK ");
            SQL.Append("                                          FROM");
            SQL.Append("                                                 CUSTOMER_MST_TBL C,");
            SQL.Append("                                                 CUSTOMER_CATEGORY_MST_TBL CC,CUSTOMER_CATEGORY_TRN CCT ");
            SQL.Append("                                          WHERE");
            SQL.Append("                                                 C.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK ");
            SQL.Append("                                                 AND CC.CUSTOMER_CATEGORY_MST_PK = CCT.CUSTOMER_CATEGORY_MST_FK ");
            SQL.Append("                                                 AND UPPER(CC.CUSTOMER_CATEGORY_ID) LIKE 'SHIPPER'");
            SQL.Append("                                         )");
            SQL.Append("          THEN cust.customer_id ELSE '' END");
            SQL.Append("       ) \"Shipper\",");

            SQL.Append("      (   CASE WHEN bst.cust_customer_mst_fk IN ");
            SQL.Append("                                         (SELECT");
            SQL.Append("                                                 C.CUSTOMER_MST_PK ");
            SQL.Append("                                          FROM");
            SQL.Append("                                                 CUSTOMER_MST_TBL C,");
            SQL.Append("                                                 CUSTOMER_CATEGORY_MST_TBL CC,CUSTOMER_CATEGORY_TRN CCT ");
            SQL.Append("                                          WHERE");
            SQL.Append("                                                 C.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK ");
            SQL.Append("                                                 AND CC.CUSTOMER_CATEGORY_MST_PK = CCT.CUSTOMER_CATEGORY_MST_FK ");
            SQL.Append("                                                 AND UPPER(CC.CUSTOMER_CATEGORY_ID) LIKE 'SHIPPER'");
            SQL.Append("                                         )");
            SQL.Append("          THEN cust.customer_name ELSE '' END");
            SQL.Append("       ) \"ShipperName\",");

            SQL.Append("      (   CASE WHEN bst.cust_customer_mst_fk IN ");
            SQL.Append("                                         (SELECT");
            SQL.Append("                                                 C.CUSTOMER_MST_PK ");
            SQL.Append("                                          FROM");
            SQL.Append("                                                 CUSTOMER_MST_TBL C,");
            SQL.Append("                                                 CUSTOMER_CATEGORY_MST_TBL CC,CUSTOMER_CATEGORY_TRN CCT ");
            SQL.Append("                                          WHERE");
            SQL.Append("                                                 C.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK ");
            SQL.Append("                                                 AND CC.CUSTOMER_CATEGORY_MST_PK = CCT.CUSTOMER_CATEGORY_MST_FK ");
            SQL.Append("                                                 AND UPPER(CC.CUSTOMER_CATEGORY_ID) LIKE 'SHIPPER'");
            SQL.Append("                                         )");
            SQL.Append("          THEN TO_CHAR(cust.customer_mst_pk) ELSE ''END");
            SQL.Append("       ) \"shipper_cust_mst_fk\",");

            SQL.Append("      consignee.customer_id AS \"Consignee\",");
            SQL.Append("      consignee.customer_name AS \"ConsigneeName\",");
            SQL.Append("      bst.cons_customer_mst_fk consignee_cust_mst_fk,");
            SQL.Append("      bst.cb_agent_mst_fk,");
            SQL.Append("      cbagnt.agent_id \"cbAgent\",");
            SQL.Append("      cbagnt.agent_name \"cbAgentName\",");
            SQL.Append("      bst.cl_agent_mst_fk,");
            SQL.Append("      clagnt.agent_id \"clAgent\",");
            SQL.Append("      clagnt.agent_name \"clAgentName\",");
            SQL.Append("      bst.cargo_move_fk,");
            SQL.Append("      bst.pymt_type,");
            SQL.Append("      bst.commodity_group_fk,");
            SQL.Append("      comm.commodity_group_desc,");


            SQL.Append("      '' GOODS_DESCRIPTION,");
            SQL.Append("      '' MARKS_NUMBERS");

            SQL.Append("FROM");
            SQL.Append("      booking_sea_tbl bst,");
            SQL.Append("      booking_trn_sea_fcl_lcl btrn,");
            SQL.Append("      port_mst_tbl POD,");
            SQL.Append("      port_mst_tbl POL,");
            SQL.Append("      customer_mst_tbl cust,");
            SQL.Append("      customer_mst_tbl consignee,");
            SQL.Append("      place_mst_tbl col_place,");
            SQL.Append("      place_mst_tbl del_place,");
            SQL.Append("      operator_mst_tbl oprator,");
            SQL.Append("      agent_mst_tbl clagnt,");
            SQL.Append("      agent_mst_tbl cbagnt,");
            SQL.Append("      commodity_group_mst_tbl comm,");
            SQL.Append("      VESSEL_VOYAGE_TBL V,");
            SQL.Append("      VESSEL_VOYAGE_TRN VVT");

            SQL.Append("WHERE ");
            SQL.Append("      bst.cust_customer_mst_fk = cust.customer_mst_pk");
            SQL.Append("      AND bst.col_place_mst_fk = col_place.place_pk (+)");
            SQL.Append("      AND bst.del_place_mst_fk = del_place.place_pk (+)");
            SQL.Append("      AND bst.cb_agent_mst_fk = cbagnt.agent_mst_pk(+)");
            SQL.Append("      AND bst.cl_agent_mst_fk = clagnt.agent_mst_pk(+)");
            SQL.Append("      AND bst.port_mst_pol_fk = POL.port_mst_pk");
            SQL.Append("      AND bst.port_mst_pod_fk = POD.port_mst_pk");
            SQL.Append("      AND bst.operator_mst_fk = oprator.operator_mst_pk(+)");
            SQL.Append("      AND bst.booking_sea_pk =" + bookingPK);
            SQL.Append("      AND bst.cons_customer_mst_fk = consignee.customer_mst_pk(+)");
            SQL.Append("      AND bst.status = 2");
            // only for the confirmed booking.
            SQL.Append("      AND bst.commodity_group_fk = comm.commodity_group_pk(+)");
            SQL.Append("      AND VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK");
            SQL.Append("      AND BST.VESSEL_VOYAGE_FK = VVT.VOYAGE_TRN_PK");

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

        public DataSet GetBookingContainerData(string bookingPK)
        {

            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("      btrn.container_type_mst_fk,");
            SQL.Append("      cont.container_type_mst_id,");
            SQL.Append("      btrn.no_of_boxes,");
            SQL.Append("      com.commodity_name,");
            SQL.Append("      btrn.commodity_mst_fk");
            SQL.Append("FROM");
            SQL.Append("      booking_sea_tbl bst,");
            SQL.Append("      booking_trn_sea_fcl_lcl btrn,");
            SQL.Append("      container_type_mst_tbl cont,");
            SQL.Append("      commodity_mst_tbl com");
            SQL.Append("WHERE");
            SQL.Append("      btrn.booking_sea_fk =  bst.booking_sea_pk");
            SQL.Append("      AND btrn.container_type_mst_fk = cont.container_type_mst_pk");
            SQL.Append("      AND btrn.booking_sea_fk =" + bookingPK);
            SQL.Append("      AND btrn.commodity_mst_fk =com.commodity_mst_pk(+) ");
            SQL.Append("      AND bst.status = 2");
            // only for the confirmed booking.
            SQL.Append("ORDER BY container_type_mst_id ");

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

        public DataSet GetBookingContainerLCLData(string bookingPK)
        {

            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("select");
            SQL.Append("0 job_trn_sea_exp_cont_pk,");
            //modified by latha
            SQL.Append("bkg_trn.container_type_mst_fk,");
            SQL.Append("'' container_id,");
            SQL.Append("'' container_number,");
            SQL.Append("'' seal_number,");
            SQL.Append("bkg.volume_in_cbm volume_in_cbm,");
            SQL.Append("bkg.gross_weight gross_weight,");
            SQL.Append("bkg.net_weight net_weight,");
            SQL.Append("bkg.chargeable_weight,");
            SQL.Append("bkg.pack_typ_mst_fk pack_type_mst_fk,");
            SQL.Append("bkg.pack_count pack_count,");
            SQL.Append("bkg_trn.commodity_mst_fk,");
            SQL.Append("'' load_date");
            SQL.Append(", '0' CONTAINER_PK");
            SQL.Append("from");
            SQL.Append("booking_sea_tbl bkg,");
            SQL.Append("booking_trn_sea_fcl_lcl bkg_trn,");
            SQL.Append("commodity_mst_tbl com ");
            SQL.Append("where");
            SQL.Append("bkg_trn.booking_sea_fk = bkg.booking_sea_pk");
            SQL.Append("AND bkg_trn.booking_sea_fk =" + bookingPK);
            SQL.Append("AND bkg_trn.commodity_mst_fk =com.commodity_mst_pk(+) ");
            SQL.Append("AND bkg.status = 2");

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

        public DataSet GetBookingFreightData(string bookingPK, Int64 baseCurrency)
        {

            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("     '0' job_trn_sea_exp_fd_pk,");
            SQL.Append("     btrn.container_type_mst_fk,");
            SQL.Append("     frt.freight_element_id,");
            SQL.Append("     frt.freight_element_name,");
            SQL.Append("     frt.freight_element_mst_pk,");
            SQL.Append("     basis,");
            SQL.Append("     quantity,");
            SQL.Append("     DECODE(bfrt.pymt_type,1,'Prepaid',2,'Collect') freight_type,");
            SQL.Append("     nvl(bfrt.tariff_rate,1)*nvl(btrn.no_of_boxes,1) freight_amt,");
            SQL.Append("     bfrt.currency_mst_fk,");
            SQL.Append("     ROUND(GET_EX_RATE(bfrt.currency_mst_fk," + baseCurrency + ",round(sysdate - .5)),4) AS ROE ,");
            SQL.Append("     'false' \"Delete\", 1 \"Print\" ");
            SQL.Append("FROM");
            SQL.Append("     booking_sea_tbl bst,");
            SQL.Append("     booking_trn_sea_fcl_lcl btrn,");
            SQL.Append("     booking_trn_sea_frt_dtls bfrt,");
            SQL.Append("     container_type_mst_tbl cont,");
            SQL.Append("     freight_element_mst_tbl frt,");
            SQL.Append("     currency_type_mst_tbl curr");
            SQL.Append("WHERE");
            SQL.Append("     bfrt.freight_element_mst_fk = frt.freight_element_mst_pk");
            SQL.Append("     AND bfrt.booking_trn_sea_fk = btrn.booking_trn_sea_pk");
            SQL.Append("     AND btrn.booking_sea_fk = bst.booking_sea_pk");
            SQL.Append("     AND btrn.container_type_mst_fk = cont.container_type_mst_pk");
            SQL.Append("     AND bst.booking_sea_pk = " + bookingPK);
            SQL.Append("     AND bfrt.currency_mst_fk = curr.currency_mst_pk");
            SQL.Append("     AND bst.status = 2");
            // only for the confirmed booking.
            SQL.Append("     ORDER BY cont.container_type_mst_id,frt.freight_element_id ");

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

        public DataSet GetBookingFreightLCLData(string bookingPK, Int64 baseCurrency)
        {

            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("     '0' job_trn_sea_exp_fd_pk,");
            SQL.Append("     btrn.container_type_mst_fk,");
            SQL.Append("     frt.freight_element_id,");
            SQL.Append("     frt.freight_element_name,");
            SQL.Append("     frt.freight_element_mst_pk,");
            SQL.Append("     DECODE(btrn.basis,0,' ',1,'%',2,'Flat rate',3,'Kgs',4,'Unit') basis,");
            SQL.Append("     btrn.quantity,");
            SQL.Append("     DECODE(bfrt.pymt_type,1,'Prepaid',2,'Collect') freight_type,");
            SQL.Append("     nvl(bfrt.tariff_rate,1)*nvl(btrn.quantity,1) freight_amt,");
            SQL.Append("     bfrt.currency_mst_fk,");
            SQL.Append("     ROUND(GET_EX_RATE(bfrt.currency_mst_fk," + baseCurrency + ",round(sysdate - .5)),4) AS ROE ,");
            SQL.Append("     'false' \"Delete\", 1 \"Print\" ");
            SQL.Append("     FROM");
            SQL.Append("     booking_sea_tbl bst,");
            SQL.Append("     booking_trn_sea_fcl_lcl btrn,");
            SQL.Append("     booking_trn_sea_frt_dtls bfrt,");
            SQL.Append("     freight_element_mst_tbl frt,");
            SQL.Append("     currency_type_mst_tbl curr");
            SQL.Append("     WHERE");
            SQL.Append("     bfrt.freight_element_mst_fk = frt.freight_element_mst_pk");
            SQL.Append("     AND bfrt.booking_trn_sea_fk = btrn.booking_trn_sea_pk");
            SQL.Append("     AND btrn.booking_sea_fk = bst.booking_sea_pk");
            SQL.Append("     AND bst.booking_sea_pk =" + bookingPK);
            SQL.Append("     AND bfrt.currency_mst_fk = curr.currency_mst_pk");
            SQL.Append("     AND bst.status = 2");
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
        #endregion

        #region "Property"
        int ComGrp;
        public int CommodityGroup
        {
            get { return ComGrp; }
            set { ComGrp = value; }
        }
        #endregion

        #region "Commodity"
        public DataSet FetchCommodity()
        {

            string strSQL = null;
            strSQL = "SELECT c.commodity_mst_pk,c.commodity_name  FROM commodity_mst_tbl c WHERE c.active_flag =1 ORDER BY c.commodity_name";
            WorkFlow objWF = new WorkFlow();
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
        #endregion

        #region "Spacial Request"

        private ArrayList SaveTransactionHZSpcl(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with1 = SCM;
                    _with1.CommandType = CommandType.StoredProcedure;
                    _with1.CommandText = UserName + ".JOB_CARD_SPCL_REQ_PKG.JOB_TRN_SEA_HAZ_SPL_REQ_INS";
                    var _with2 = _with1.Parameters;
                    _with2.Clear();
                    //BKG_TRN_SEA_FK_IN()
                    _with2.Add("JOB_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //OUTER_PACK_TYPE_MST_FK_IN()
                    _with2.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    //INNER_PACK_TYPE_MST_FK_IN()
                    _with2.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with2.Add("MIN_TEMP_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with2.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? 0 : Convert.ToInt32(strParam[3])), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with2.Add("MAX_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with2.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_IN()
                    _with2.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_UOM_IN()
                    _with2.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? 0 : Convert.ToInt32(strParam[7])), 0)).Direction = ParameterDirection.Input;
                    //IMDG_CLASS_CODE_IN()
                    _with2.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //UN_NO_IN()
                    _with2.Add("UN_NO_IN", getDefault(strParam[9], DBNull.Value)).Direction = ParameterDirection.Input;
                    //IMO_SURCHARGE_IN()
                    _with2.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //SURCHARGE_AMT_IN()
                    _with2.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                    //IS_MARINE_POLLUTANT_IN()
                    _with2.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //EMS_NUMBER_IN()
                    _with2.Add("EMS_NUMBER_IN", getDefault(strParam[13], DBNull.Value)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with2.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with1.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
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
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }

        public DataTable fetchSpclReq(string strPK)
        {
            if (!string.IsNullOrEmpty(strPK))
            {
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                strQuery.Append("SELECT JOB_TRN_SEA_SPL_PK,");
                strQuery.Append("JOB_TRN_SEA_EXP_CONT_FK,");
                strQuery.Append("OUTER_PACK_TYPE_MST_FK,");
                strQuery.Append("INNER_PACK_TYPE_MST_FK,");
                strQuery.Append("MIN_TEMP,");
                strQuery.Append("MIN_TEMP_UOM,");
                strQuery.Append("MAX_TEMP,");
                strQuery.Append("MAX_TEMP_UOM,");
                strQuery.Append("FLASH_PNT_TEMP,");
                strQuery.Append("FLASH_PNT_TEMP_UOM,");
                strQuery.Append("IMDG_CLASS_CODE,");
                strQuery.Append("UN_NO,");
                strQuery.Append("IMO_SURCHARGE,");
                strQuery.Append("SURCHARGE_AMT,");
                strQuery.Append("IS_MARINE_POLLUTANT,");
                strQuery.Append("EMS_NUMBER FROM JOB_TRN_SEA_EXP_SPL_REQ Q");
                strQuery.Append("WHERE ");
                strQuery.Append("Q.JOB_TRN_SEA_EXP_CONT_FK=" + strPK);
                return (new WorkFlow()).GetDataTable(strQuery.ToString());
            }
            else
            {
                return null;
            }
        }

        private ArrayList SaveTransactionReefer(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {

            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with3 = SCM;
                    _with3.CommandType = CommandType.StoredProcedure;
                    _with3.CommandText = UserName + ".JOB_CARD_SPCL_REQ_PKG.JOB_TRN_SEA_REF_SPL_REQ_INS";
                    var _with4 = _with3.Parameters;
                    _with4.Clear();
                    //BOOKING_TRN_SEA_FK_IN()
                    _with4.Add("JOB_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //VENTILATION_IN()
                    _with4.Add("VENTILATION_IN", strParam[0]).Direction = ParameterDirection.Input;
                    //AIR_COOL_METHOD_IN()
                    _with4.Add("AIR_COOL_METHOD_IN", strParam[1]).Direction = ParameterDirection.Input;
                    //HUMIDITY_FACTOR_IN()
                    _with4.Add("HUMIDITY_FACTOR_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //IS_PERSHIABLE_GOODS_IN()
                    _with4.Add("IS_PERSHIABLE_GOODS_IN", strParam[3]).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with4.Add("MIN_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with4.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with4.Add("MAX_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with4.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? 0 : Convert.ToInt32(strParam[7])), 0)).Direction = ParameterDirection.Input;
                    //PACK_TYPE_MST_FK_IN()
                    _with4.Add("PACK_TYPE_MST_FK_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //PACK_COUNT_IN()
                    _with4.Add("PACK_COUNT_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with3.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
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
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }

        public DataTable fetchSpclReqReefer(string strPK)
        {
            if (!string.IsNullOrEmpty(strPK))
            {
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                strQuery.Append("SELECT JOB_TRN_SEA_SPL_PK,");
                strQuery.Append("JOB_TRN_SEA_EXP_CONT_FK,");
                strQuery.Append("VENTILATION,");
                strQuery.Append("AIR_COOL_METHOD,");
                strQuery.Append("HUMIDITY_FACTOR,");
                strQuery.Append("IS_PERSHIABLE_GOODS,");
                strQuery.Append("MIN_TEMP,");
                strQuery.Append("MIN_TEMP_UOM,");
                strQuery.Append("MAX_TEMP,");
                strQuery.Append("MAX_TEMP_UOM,");
                strQuery.Append("PACK_TYPE_MST_FK,");
                strQuery.Append("Q.PACK_COUNT ");
                strQuery.Append("FROM JOB_TRN_SEA_EXP_SPL_REQ Q");
                strQuery.Append("WHERE ");
                strQuery.Append("Q.JOB_TRN_SEA_EXP_CONT_FK=" + strPK);
                return (new WorkFlow()).GetDataTable(strQuery.ToString());
            }
            else
            {
                return null;
            }
        }

        private ArrayList SaveTransactionODC(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with5 = SCM;
                    _with5.CommandType = CommandType.StoredProcedure;
                    _with5.CommandText = UserName + ".JOB_CARD_SPCL_REQ_PKG.JOB_TRN_SEA_ODC_SPL_REQ_INS";
                    var _with6 = _with5.Parameters;
                    _with6.Clear();
                    //BKG_TRN_SEA_FK_IN()
                    _with6.Add("JOB_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //LENGTH_IN()
                    _with6.Add("LENGTH_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    //LENGTH_UOM_MST_FK_IN()
                    _with6.Add("LENGTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[0]) ? 0 : Convert.ToInt32(strParam[1])), 0)).Direction = ParameterDirection.Input;
                    //HEIGHT_IN()
                    _with6.Add("HEIGHT_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //HEIGHT_UOM_MST_FK_IN()
                    _with6.Add("HEIGHT_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? 0 : Convert.ToInt32(strParam[3])), 0)).Direction = ParameterDirection.Input;
                    //WIDTH_IN()
                    _with6.Add("WIDTH_IN", getDefault(strParam[4], 0)).Direction = ParameterDirection.Input;
                    //WIDTH_UOM_MST_FK_IN()
                    _with6.Add("WIDTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //WEIGHT_IN()
                    _with6.Add("WEIGHT_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //WEIGHT_UOM_MST_FK_IN()
                    _with6.Add("WEIGHT_UOM_MST_FK_IN", getDefault(strParam[7], 0)).Direction = ParameterDirection.Input;
                    //VOLUME_IN()
                    _with6.Add("VOLUME_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //VOLUME_UOM_MST_FK_IN()
                    _with6.Add("VOLUME_UOM_MST_FK_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //SLOT_LOSS_IN()
                    _with6.Add("SLOT_LOSS_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //LOSS_QUANTITY_IN()
                    _with6.Add("LOSS_QUANTITY_IN", getDefault(strParam[11], DBNull.Value)).Direction = ParameterDirection.Input;
                    //APPR_REQ_IN()
                    _with6.Add("APPR_REQ_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with6.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with5.ExecuteNonQuery();
                    arrMessage.Add("All data saved successfully");
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
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }

        public DataTable fetchSpclReqODC(string strPK)
        {
            if (!string.IsNullOrEmpty(strPK))
            {
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                strQuery.Append("SELECT ");
                strQuery.Append("JOB_TRN_SEA_SPL_PK,");
                strQuery.Append("JOB_TRN_SEA_EXP_CONT_FK,");
                strQuery.Append("LENGTH,");
                strQuery.Append("LENGTH_UOM_MST_FK,");
                strQuery.Append("HEIGHT,");
                strQuery.Append("HEIGHT_UOM_MST_FK,");
                strQuery.Append("WIDTH,");
                strQuery.Append("WIDTH_UOM_MST_FK,");
                strQuery.Append("WEIGHT,");
                strQuery.Append("WEIGHT_UOM_MST_FK,");
                strQuery.Append("VOLUME,");
                strQuery.Append("VOLUME_UOM_MST_FK,");
                strQuery.Append("SLOT_LOSS,");
                strQuery.Append("LOSS_QUANTITY,");
                strQuery.Append("APPR_REQ ");
                strQuery.Append("FROM JOB_TRN_SEA_EXP_SPL_REQ Q");
                strQuery.Append("WHERE ");
                strQuery.Append("Q.JOB_TRN_SEA_EXP_CONT_FK=" + strPK);
                return (new WorkFlow()).GetDataTable(strQuery.ToString());
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region "Save Function"

        public ArrayList Save(DataSet M_DataSet, DataSet dsContainerData, DataSet dsTPDetails, DataSet dsFreightDetails, DataSet dsPurchaseInventory, DataSet dsCostDetails, DataSet dsPickUpDetails, DataSet dsDropDetails, bool Update, bool isEdting,
        object ucrNo, string jobCardRefNumber, string userLocation, string employeeID, long JobCardPK, DataSet dsOtherCharges, string strBookingRefNo, string strOperatorPk, Int16 intIsUpdate, string hdColPlace = "",
        string hdDelPlace = "", DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null, DataSet dsDoc = null)
        {

            int strVoyagepk = Convert.ToInt32(getDefault(M_DataSet.Tables[0].Rows[0]["VOYAGEPK"], 0));
            // By Amit
            objVesselVoyage.ConfigurationPK = M_Configuration_PK;
            objVesselVoyage.CREATED_BY = M_CREATED_BY_FK;
            ///'
            WorkFlow objWK = new WorkFlow();

            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            OracleTransaction TRAN1 = null;

            // To save in Vessel/Voyage
            //
            if (Update == true)
            {
                strVoyagepk = 0;
            }
            TRAN = objWK.MyConnection.BeginTransaction();
            TRAN1 = TRAN;
            //If strVoyagepk = "0" Then

            if (strVoyagepk.ToString() == "0" & !string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["VESSEL_ID"].ToString()))
            {
                objWK.MyCommand.Transaction = TRAN1;
                objWK.MyCommand.Connection = objWK.MyConnection;

                arrMessage = objVesselVoyage.SaveVesselMaster(strVoyagepk, Convert.ToString(getDefault(M_DataSet.Tables[0].Rows[0]["VESSEL_NAME"], "")),
                    Convert.ToInt32(getDefault(M_DataSet.Tables[0].Rows[0]["OPERATOR_MST_FK"], 0)),
                    Convert.ToString(getDefault(M_DataSet.Tables[0].Rows[0]["VESSEL_ID"], "")),
                    Convert.ToString(getDefault(M_DataSet.Tables[0].Rows[0]["VOYAGE"], "")), 
                    objWK.MyCommand,
                    Convert.ToInt32(getDefault(M_DataSet.Tables[0].Rows[0]["PORT_MST_POL_FK"], 0)),
                    Convert.ToString(M_DataSet.Tables[0].Rows[0]["PORT_MST_POD_FK"]), 
                    DateTime.Now,
                    Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["ETD_DATE"], null)),
                    DateTime.Now,
                    Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["ETA_DATE"], null)),
                    Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["departure_date"], null)),
                    Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["arrival_date"], null)));
                M_DataSet.Tables[0].Rows[0]["VOYAGEPK"] = strVoyagepk;
                if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                {
                    TRAN1.Rollback();
                    return arrMessage;
                }
                else
                {
                    //TRAN1.Commit()
                    arrMessage.Clear();
                }
            }

            Int32 nRowCnt = default(Int32);
            
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            OracleCommand insContainerDetails = new OracleCommand();
            OracleCommand updContainerDetails = new OracleCommand();

            OracleCommand insTPDetails = new OracleCommand();
            OracleCommand updTPDetails = new OracleCommand();
            OracleCommand delTPDetails = new OracleCommand();

            OracleCommand insFreightDetails = new OracleCommand();
            OracleCommand updFreightDetails = new OracleCommand();
            OracleCommand delFreightDetails = new OracleCommand();

            OracleCommand insPickUpDetails = new OracleCommand();
            OracleCommand updPickUpDetails = new OracleCommand();

            OracleCommand insDropDetails = new OracleCommand();
            OracleCommand updDropDetails = new OracleCommand();

            OracleCommand insPurchaseInvDetails = new OracleCommand();
            OracleCommand updPurchaseInvDetails = new OracleCommand();
            OracleCommand delPurchaseInvDetails = new OracleCommand();

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
            //    jobCardRefNumber = GenerateProtocolKey("JOB CARD EXP (SEA)", userLocation, employeeID, Date.Now, , , , M_Last_Modified_By_Fk)
            //End If

            ucrNo = ucrNo + jobCardRefNumber;

            try
            {

                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;

                dsTrackNTrace = dsContainerData.Copy();
                var _with7 = insCommand;
                _with7.Connection = objWK.MyConnection;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_CARD_SEA_EXP_TBL_INS";
                var _with8 = _with7.Parameters;

                insCommand.Parameters.Add("BOOKING_SEA_FK_IN", OracleDbType.Int32, 10, "booking_sea_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["BOOKING_SEA_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JOBCARD_REF_NO_IN", jobCardRefNumber).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("UCR_NO_IN", ucrNo).Direction = ParameterDirection.Input;
                //insCommand.Parameters["UCR_NO_IN"].SourceVersion = DataRowVersion.Current

                insCommand.Parameters.Add("JOBCARD_DATE_IN", OracleDbType.Date, 20, "jobcard_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOBCARD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JOB_CARD_STATUS_IN", OracleDbType.Int32, 1, "job_card_status").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOB_CARD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JOB_CARD_CLOSED_ON_IN", OracleDbType.Date, 20, "job_card_closed_on").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOB_CARD_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_XML_STATUS_IN", OracleDbType.Int32, 1, "WIN_XML_STATUS").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_XML_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "remarks").Direction = ParameterDirection.Input;
                insCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "vessel_name").Direction = ParameterDirection.Input;
                insCommand.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VOYAGE_IN", OracleDbType.Varchar2, 10, "voyage").Direction = ParameterDirection.Input;
                insCommand.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters.Add("VOYAGE_FK_IN", OracleDbType.Varchar2, 10, "VoyagePK").Direction = ParameterDirection.Input
                insCommand.Parameters.Add("VOYAGE_FK_IN", getDefault(strVoyagepk, DBNull.Value)).Direction = ParameterDirection.Input;
                insCommand.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 25, "eta_date").Direction = ParameterDirection.Input
                //insCommand.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["ETA_DATE"].ToString()))
                {
                    insCommand.Parameters.Add("ETA_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("ETA_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["ETA_DATE"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;
                //Added By Sivachandran on 12-08-2009

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_Date"].ToString()))
                {
                    insCommand.Parameters.Add("SURVEY_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("SURVEY_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["Survey_Date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["SURVEY_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SHIPPING_INST_DT"].ToString()))
                {
                    insCommand.Parameters.Add("SI_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("SI_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["SHIPPING_INST_DT"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["SI_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["RFS_Date"].ToString()))
                {
                    insCommand.Parameters.Add("RFS_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("RFS_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["RFS_Date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["RFS_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CRQ_Date"].ToString()))
                {
                    insCommand.Parameters.Add("CRQ_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("CRQ_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["CRQ_Date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["CRQ_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["STF_Date"].ToString()))
                {
                    insCommand.Parameters.Add("STF_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("STF_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["STF_Date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["STF_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SURVEY_REF_NR_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_Ref_Nr"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[0]["Survey_Ref_Nr"])).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("SURVEY_REMARKS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_Remarks"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[0]["Survey_Remarks"])).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("SURVEYOR_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_PK"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[0]["Survey_PK"])).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("SI_IN", OracleDbType.Int32, 1, "SHIPPING_INST_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("RFS_IN", OracleDbType.Int32, 1, "RFS").Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("CRQ_IN", OracleDbType.Int32, 1, "CRQ").Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("STF_IN", OracleDbType.Int32, 1, "STF").Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("SURVEY_COMPLETED_IN", OracleDbType.Int32, 1, "SURVEY_COMPLETED").Direction = ParameterDirection.Input;

                //end

                //insCommand.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 25, "etd_date").Direction = ParameterDirection.Input
                //insCommand.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["ETD_DATE"].ToString()))
                {
                    insCommand.Parameters.Add("ETD_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("ETD_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["ETD_DATE"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;



                //insCommand.Parameters.Add("ARRIVAL_DATE_IN", OracleDbType.Date, 25, "arrival_date").Direction = ParameterDirection.Input
                //insCommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["arrival_date"].ToString()))
                {
                    insCommand.Parameters.Add("ARRIVAL_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("ARRIVAL_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["arrival_date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters.Add("DEPARTURE_DATE_IN", OracleDbType.Date, 25, "departure_date").Direction = ParameterDirection.Input
                //insCommand.Parameters["DEPARTURE_DATE_IN"].SourceVersion = DataRowVersion.Current

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["departure_date"].ToString()))
                {
                    insCommand.Parameters.Add("DEPARTURE_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("DEPARTURE_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["departure_date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["DEPARTURE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SEC_VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "sec_vessel_name").Direction = ParameterDirection.Input;
                insCommand.Parameters["SEC_VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SEC_VOYAGE_IN", OracleDbType.Varchar2, 10, "sec_voyage").Direction = ParameterDirection.Input;
                insCommand.Parameters["SEC_VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SEC_ETA_DATE_IN", OracleDbType.Date, 20, "sec_eta_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["SEC_ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SEC_ETD_DATE_IN", OracleDbType.Date, 20, "sec_etd_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["SEC_ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

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

                insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_PK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                //Added By Rijesh To Incorporate Cargo Details On March -30 2006
                //*******************
                insCommand.Parameters.Add("GOODS_DESCRIPTION_IN", OracleDbType.Varchar2, 4000, "GOODS_DESCRIPTION").Direction = ParameterDirection.Input;
                insCommand.Parameters["GOODS_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MARKS_NUMBERS_IN", OracleDbType.Varchar2, 2000, "MARKS_NUMBERS").Direction = ParameterDirection.Input;
                insCommand.Parameters["MARKS_NUMBERS_IN"].SourceVersion = DataRowVersion.Current;
                //***********************

                insCommand.Parameters.Add("master_jc_sea_exp_fk_in", OracleDbType.Int32, 10, "MASTER_JC_SEA_EXP_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["master_jc_sea_exp_fk_in"].SourceVersion = DataRowVersion.Current;

                //Code Added By Anil on 17 Aug 09
                insCommand.Parameters.Add("SB_DATE_IN", OracleDbType.Date, 20, "sb_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["SB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SB_NO_IN", OracleDbType.Varchar2, 20, "sb_number").Direction = ParameterDirection.Input;
                insCommand.Parameters["SB_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("STUFF_LOC_IN", OracleDbType.Varchar2, 40, "stuff_loc").Direction = ParameterDirection.Input;
                insCommand.Parameters["STUFF_LOC_IN"].SourceVersion = DataRowVersion.Current;
                //End By Anil

                insCommand.Parameters.Add("LC_SHIPMENT_IN", OracleDbType.Int32, 1, "LC_SHIPMENT").Direction = ParameterDirection.Input;
                insCommand.Parameters["LC_SHIPMENT_IN"].SourceVersion = DataRowVersion.Current;

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


                //Raghavenra added
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

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_CARD_SEA_EXP_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with9 = updCommand;
                _with9.Connection = objWK.MyConnection;
                _with9.CommandType = CommandType.StoredProcedure;
                _with9.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_CARD_SEA_EXP_TBL_UPD";
                var _with10 = _with9.Parameters;

                updCommand.Parameters.Add("JOB_CARD_SEA_EXP_PK_IN", OracleDbType.Int32, 10, "job_card_sea_exp_pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_SEA_EXP_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BOOKING_SEA_FK_IN", OracleDbType.Int32, 10, "booking_sea_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["BOOKING_SEA_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("UCR_NO_IN", OracleDbType.Varchar2, 40, "ucr_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["UCR_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOBCARD_REF_NO_IN", jobCardRefNumber).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("JOBCARD_DATE_IN", OracleDbType.Date, 20, "jobcard_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOBCARD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOB_CARD_STATUS_IN", OracleDbType.Int32, 1, "job_card_status").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOB_CARD_CLOSED_ON_IN", OracleDbType.Date, 20, "job_card_closed_on").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_XML_STATUS_IN", OracleDbType.Int32, 1, "WIN_XML_STATUS").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_XML_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "remarks").Direction = ParameterDirection.Input;
                updCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "vessel_name").Direction = ParameterDirection.Input;
                updCommand.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VOYAGE_IN", OracleDbType.Varchar2, 10, "voyage").Direction = ParameterDirection.Input;
                updCommand.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                //updCommand.Parameters.Add("VOYAGE_FK_IN", OracleDbType.Varchar2, 10, "VoyagePK").Direction = ParameterDirection.Input
                updCommand.Parameters.Add("VOYAGE_FK_IN", getDefault(strVoyagepk, DBNull.Value)).Direction = ParameterDirection.Input;
                updCommand.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ARRIVAL_DATE_IN", OracleDbType.Date, 20, "arrival_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DEPARTURE_DATE_IN", OracleDbType.Date, 20, "departure_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEPARTURE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SEC_VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "sec_vessel_name").Direction = ParameterDirection.Input;
                updCommand.Parameters["SEC_VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SEC_VOYAGE_IN", OracleDbType.Varchar2, 10, "sec_voyage").Direction = ParameterDirection.Input;
                updCommand.Parameters["SEC_VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SEC_ETA_DATE_IN", OracleDbType.Date, 20, "sec_eta_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["SEC_ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SEC_ETD_DATE_IN", OracleDbType.Date, 20, "sec_etd_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["SEC_ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

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

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200, "del_address").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                //<<<<<<<<<<<-----------------Jagadeesh on 15-Dec-06-----------------
                //updCommand.Parameters.Add("COL_PLACE_MST_FK_IN", OracleDbType.Int32, 10, hdColPlace).Direction = ParameterDirection.Input
                //updCommand.Parameters["COL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current

                //updCommand.Parameters.Add("DEL_PLACE_MST_FK_IN", OracleDbType.Int32, 10, hdDelPlace).Direction = ParameterDirection.Input
                //updCommand.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                //<<<<<<<<<<<--------------------------------------------------------
                //Added By Rijesh To Incorporate Cargo Details  on March 30  2006
                //****************************************************************
                updCommand.Parameters.Add("GOODS_DESCRIPTION_IN", OracleDbType.Varchar2, 4000, "GOODS_DESCRIPTION").Direction = ParameterDirection.Input;
                updCommand.Parameters["GOODS_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MARKS_NUMBERS_IN", OracleDbType.Varchar2, 2000, "MARKS_NUMBERS").Direction = ParameterDirection.Input;
                updCommand.Parameters["MARKS_NUMBERS_IN"].SourceVersion = DataRowVersion.Current;
                //*****************************************************************

                updCommand.Parameters.Add("MASTER_JC_SEA_EXP_FK_IN", OracleDbType.Int32, 10, "MASTER_JC_SEA_EXP_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["MASTER_JC_SEA_EXP_FK_IN"].SourceVersion = DataRowVersion.Current;

                //Added By Sivachandran on 12-08-2009

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_Date"].ToString()))
                {
                    updCommand.Parameters.Add("SURVEY_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("SURVEY_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["Survey_Date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["SURVEY_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["SHIPPING_INST_DT"].ToString()))
                {
                    updCommand.Parameters.Add("SI_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("SI_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["SHIPPING_INST_DT"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["SI_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["RFS_Date"].ToString()))
                {
                    updCommand.Parameters.Add("RFS_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("RFS_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["RFS_Date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["RFS_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CRQ_Date"].ToString()))
                {
                    updCommand.Parameters.Add("CRQ_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("CRQ_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["CRQ_Date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["CRQ_DATE_IN"].SourceVersion = DataRowVersion.Current;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["STF_Date"].ToString()))
                {
                    updCommand.Parameters.Add("STF_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("STF_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["STF_Date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["STF_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SURVEY_REF_NR_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_Ref_Nr"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[0]["Survey_Ref_Nr"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("SURVEY_REMARKS_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_Remarks"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[0]["Survey_Remarks"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("SURVEYOR_FK_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["Survey_PK"].ToString()) ? DBNull.Value : M_DataSet.Tables[0].Rows[0]["Survey_PK"])).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("SI_IN", OracleDbType.Int32, 1, "SHIPPING_INST_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("RFS_IN", OracleDbType.Int32, 1, "RFS").Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CRQ_IN", OracleDbType.Int32, 1, "CRQ").Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("STF_IN", OracleDbType.Int32, 1, "STF").Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("SURVEY_COMPLETED_IN", OracleDbType.Int32, 1, "SURVEY_COMPLETED").Direction = ParameterDirection.Input;

                //end

                //Code Added By Anil on 17 Aug 09
                updCommand.Parameters.Add("SB_DATE_IN", OracleDbType.Date, 20, "sb_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["SB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SB_NO_IN", OracleDbType.Varchar2, 20, "sb_number").Direction = ParameterDirection.Input;
                updCommand.Parameters["SB_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("STUFF_LOC_IN", OracleDbType.Varchar2, 40, "stuff_loc").Direction = ParameterDirection.Input;
                updCommand.Parameters["STUFF_LOC_IN"].SourceVersion = DataRowVersion.Current;
                //End By Anil

                updCommand.Parameters.Add("LC_SHIPMENT_IN", OracleDbType.Int32, 1, "LC_SHIPMENT").Direction = ParameterDirection.Input;
                updCommand.Parameters["LC_SHIPMENT_IN"].SourceVersion = DataRowVersion.Current;

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

                //Added by raghavendra

                updCommand.Parameters.Add("PRC_FK_IN", OracleDbType.Int32, 1, "PRC_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["PRC_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ONC_FK_IN", OracleDbType.Int32, 1, "ONC_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["ONC_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PRC_MODE_FK_IN", OracleDbType.Int32, 1, "PRC_MODE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["PRC_MODE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ONC_MODE_FK_IN", OracleDbType.Int32, 1, "ONC_MODE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["ONC_MODE_FK_IN"].SourceVersion = DataRowVersion.Current;

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
                //End
                

                var _with11 = objWK.MyDataAdapter;

                _with11.InsertCommand = insCommand;
                _with11.InsertCommand.Transaction = TRAN;

                _with11.UpdateCommand = updCommand;
                _with11.UpdateCommand.Transaction = TRAN;


                RecAfct = _with11.Update(M_DataSet);

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


                var _with12 = insContainerDetails;
                _with12.Connection = objWK.MyConnection;
                _with12.CommandType = CommandType.StoredProcedure;
                _with12.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_CONT_INS";
                var _with13 = _with12.Parameters;

                insContainerDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                //insContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 16, "container_number").Direction = ParameterDirection.Input
                //insContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current
                insContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                //insContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input
                //insContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                insContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                //insContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "seal_number").Direction = ParameterDirection.Input
                //insContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current
                insContainerDetails.Parameters.Add("SEAL_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                insContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "Volume").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                //insContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input
                //insContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current
                insContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;

                //insContainerDetails.Parameters.Add("NET_WEIGHT_IN", OracleDbType.Int32, 10, "net_weight").Direction = ParameterDirection.Input
                //insContainerDetails.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current
                insContainerDetails.Parameters.Add("NET_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;

                insContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "ChargeableWeight").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "PackTypePK").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "Nos").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "CommodityPK").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //Added By Prakash chandra on 5/1/2009 for pts: multiple commodity selection 
                //insContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Varchar2, 200, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input
                //insContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current
                insContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", DBNull.Value).Direction = ParameterDirection.Input;

                insContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "GLD").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                //Snigdharani - 21/08/2009
                insContainerDetails.Parameters.Add("CONTAINER_PK_IN", OracleDbType.Int32, 20, "Container_No_pk").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CONTAINER_PK_IN"].SourceVersion = DataRowVersion.Current;
                //insContainerDetails.Parameters.Add("LOAD_DATE_IN", loaddate).Direction = ParameterDirection.Input

                insContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PK").Direction = ParameterDirection.Output;
                insContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with14 = updContainerDetails;
                _with14.Connection = objWK.MyConnection;
                _with14.CommandType = CommandType.StoredProcedure;
                _with14.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_CONT_UPD";
                var _with15 = _with14.Parameters;

                updContainerDetails.Parameters.Add("JOB_TRN_SEA_EXP_CONT_PK_IN", OracleDbType.Int32, 10, "PK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["JOB_TRN_SEA_EXP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                //updContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 16, "container_number").Direction = ParameterDirection.Input
                //updContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current
                updContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                //updContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input
                //updContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                updContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                //updContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "seal_number").Direction = ParameterDirection.Input
                //updContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current
                updContainerDetails.Parameters.Add("SEAL_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                updContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "Volume").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                //updContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input
                //updContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current
                updContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;

                //updContainerDetails.Parameters.Add("NET_WEIGHT_IN", OracleDbType.Int32, 10, "net_weight").Direction = ParameterDirection.Input
                //updContainerDetails.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current
                updContainerDetails.Parameters.Add("NET_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;

                updContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "ChargeableWeight").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "PackTypePK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "Nos").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "CommodityPK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //Added By prakash chandra on 6/1/2009 for pts:Docs - Job Card – Provision to capture Multiple Commodities 
                //updContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Varchar2, 200, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input
                //updContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current
                updContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", DBNull.Value).Direction = ParameterDirection.Input;

                updContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "GLD").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;
                //Snigdharani - 21/08/2009
                updContainerDetails.Parameters.Add("CONTAINER_PK_IN", OracleDbType.Int32, 20, "Container_No_pk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CONTAINER_PK_IN"].SourceVersion = DataRowVersion.Current;

                //updContainerDetails.Parameters.Add("LOAD_DATE_IN", loaddate).Direction = ParameterDirection.Input

                updContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with16 = objWK.MyDataAdapter;

                _with16.InsertCommand = insContainerDetails;
                _with16.InsertCommand.Transaction = TRAN;

                _with16.UpdateCommand = updContainerDetails;
                _with16.UpdateCommand.Transaction = TRAN;
                RecAfct = _with16.Update(dsContainerData.Tables[0]);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                objWK.MyCommand.Transaction = TRAN;
                // Amit 22-Dec-06 TaskID "DOC-DEC-001"
                int rowCnt = 0;
                if (dsContainerData.Tables[0].Rows.Count > 0)
                {
                    for (rowCnt = 0; rowCnt <= dsContainerData.Tables[0].Rows.Count - 1; rowCnt++)
                    {
                        if (CommodityGroup == HAZARDOUS)
                        {
                            arrMessage = SaveTransactionHZSpcl(objWK.MyCommand, objWK.MyUserName,
                                Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")),
                                Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
                        }
                        else if (CommodityGroup == REEFER)
                        {
                            arrMessage = SaveTransactionReefer(objWK.MyCommand, objWK.MyUserName,
                                Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")),
                                Convert.ToInt32(dsContainerData.Tables[0].Rows[rowCnt][0]));
                        }
                        else if (CommodityGroup == ODC)
                        {
                            arrMessage = SaveTransactionODC(objWK.MyCommand, objWK.MyUserName,
                                Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")),
                                Convert.ToInt32(dsContainerData.Tables[0].Rows[rowCnt][0]));
                        }
                    }
                }
                // End

                var _with17 = insTPDetails;
                _with17.Connection = objWK.MyConnection;
                _with17.CommandType = CommandType.StoredProcedure;
                _with17.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_TP_INS";
                var _with18 = _with17.Parameters;

                insTPDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insTPDetails.Parameters.Add("PORT_MST_FK_IN", OracleDbType.Int32, 10, "port_mst_fk").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["PORT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("TRANSHIPMENT_NO_IN", OracleDbType.Int32, 10, "transhipment_no").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["TRANSHIPMENT_NO_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "vessel_name").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("VOYAGE_IN", OracleDbType.Varchar2, 10, "voyage").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("VOYAGE_FK_IN", OracleDbType.Varchar2, 10, "GridVoyagePK").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("AGENT_FK_IN", OracleDbType.Varchar2, 10, "AgentPK").Direction = ParameterDirection.Input;
                insTPDetails.Parameters["AGENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                insTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_TP_PK").Direction = ParameterDirection.Output;
                insTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with19 = updTPDetails;
                _with19.Connection = objWK.MyConnection;
                _with19.CommandType = CommandType.StoredProcedure;
                _with19.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_TP_UPD";
                var _with20 = _with19.Parameters;

                updTPDetails.Parameters.Add("JOB_TRN_SEA_EXP_TP_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_tp_pk").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["JOB_TRN_SEA_EXP_TP_PK_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updTPDetails.Parameters.Add("PORT_MST_FK_IN", OracleDbType.Int32, 10, "port_mst_fk").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["PORT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("TRANSHIPMENT_NO_IN", OracleDbType.Int32, 10, "transhipment_no").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["TRANSHIPMENT_NO_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "vessel_name").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("VOYAGE_IN", OracleDbType.Varchar2, 10, "voyage").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("VOYAGE_FK_IN", OracleDbType.Varchar2, 10, "GridVoyagePK").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("AGENT_FK_IN", OracleDbType.Varchar2, 10, "AgentPK").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["AGENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input;
                updTPDetails.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with21 = delTPDetails;
                _with21.Connection = objWK.MyConnection;
                _with21.CommandType = CommandType.StoredProcedure;
                _with21.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_TP_DEL";

                _with21.Parameters.Add("JOB_TRN_SEA_EXP_TP_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_tp_pk").Direction = ParameterDirection.Input;
                _with21.Parameters["JOB_TRN_SEA_EXP_TP_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with21.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with21.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with22 = objWK.MyDataAdapter;

                _with22.InsertCommand = insTPDetails;
                _with22.InsertCommand.Transaction = TRAN;

                _with22.UpdateCommand = updTPDetails;
                _with22.UpdateCommand.Transaction = TRAN;

                _with22.DeleteCommand = delTPDetails;
                _with22.DeleteCommand.Transaction = TRAN;
                RecAfct = _with22.Update(dsTPDetails.Tables[0]);
                // Amit 22-Dec-06 TaskID "DOC-DEC-001"
                if (arrMessage.Count == 0)
                {
                    //goto 20;
                }

                if (string.Compare(Convert.ToString(arrMessage[0]).ToLower(), "saved") > 0)
                {
                    arrMessage.Clear();
                }
                //20:

				if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                // End

                var _with23 = insFreightDetails;
                _with23.Connection = objWK.MyConnection;
                _with23.CommandType = CommandType.StoredProcedure;
                _with23.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_INS";

                _with23.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                //.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input
                //.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                _with23.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                _with23.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                _with23.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with23.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                _with23.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                // By Amit Singh on 23-May-07
                _with23.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                _with23.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with23.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                _with23.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                // End
                _with23.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                _with23.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with23.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURR_FK").Direction = ParameterDirection.Input;
                _with23.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //'changed

                _with23.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                _with23.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                //'surcharge
                _with23.Parameters.Add("surcharge_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                _with23.Parameters["surcharge_IN"].SourceVersion = DataRowVersion.Current;
                ///surcharge


                _with23.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                _with23.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                _with23.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                _with23.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                // Added Suresh Kumar 30.03.2006 - Print Check box for MBL Print
                _with23.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                _with23.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;
                //end
                _with23.Parameters.Add("JOB_TRN_SEA_EXP_CONT_FK_IN", OracleDbType.Int32, 1, "JOB_TRN_SEA_EXP_CONT_PK").Direction = ParameterDirection.Input;
                _with23.Parameters["JOB_TRN_SEA_EXP_CONT_FK_IN"].SourceVersion = DataRowVersion.Current;
                //Added by Faheem
                _with23.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "RATEPERBASIS").Direction = ParameterDirection.Input;
                _with23.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;
                //End
                _with23.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_FD_PK").Direction = ParameterDirection.Output;
                _with23.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with24 = updFreightDetails;
                _with24.Connection = objWK.MyConnection;
                _with24.CommandType = CommandType.StoredProcedure;
                _with24.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_UPD";

                _with24.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_fd_pk").Direction = ParameterDirection.Input;
                _with24.Parameters["JOB_TRN_SEA_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with24.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                //.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input
                //.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current

                _with24.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                _with24.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with24.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                _with24.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                // By Amit Singh on 23-May-07
                _with24.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                _with24.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with24.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                _with24.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                // End

                //Added by Faheem
                _with24.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "RATEPERBASIS").Direction = ParameterDirection.Input;
                _with24.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;
                //End
                _with24.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                _with24.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                //'surcharge
                _with24.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                _with24.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;
                ///surcharge

                _with24.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURR_FK").Direction = ParameterDirection.Input;
                _with24.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //'changed

                _with24.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                _with24.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                _with24.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                _with24.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                _with24.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                _with24.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                //' Added Suresh Kumar 30.03.2006 - Print Check box for MBL Print
                _with24.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                _with24.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;
                //end

                _with24.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with24.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with25 = delFreightDetails;
                _with25.Connection = objWK.MyConnection;
                _with25.CommandType = CommandType.StoredProcedure;
                _with25.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_DEL";

                _with25.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_fd_pk").Direction = ParameterDirection.Input;
                _with25.Parameters["JOB_TRN_SEA_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with25.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with25.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with26 = objWK.MyDataAdapter;

                _with26.InsertCommand = insFreightDetails;
                _with26.InsertCommand.Transaction = TRAN;

                _with26.UpdateCommand = updFreightDetails;
                _with26.UpdateCommand.Transaction = TRAN;

                _with26.DeleteCommand = delFreightDetails;
                _with26.DeleteCommand.Transaction = TRAN;

                RecAfct = _with26.Update(dsFreightDetails);

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

                //Manjunath for Cargo Pick up & Drop Address
                if ((dsPickUpDetails != null))
                {
                    var _with27 = insPickUpDetails;
                    _with27.Connection = objWK.MyConnection;
                    _with27.CommandType = CommandType.StoredProcedure;
                    _with27.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_INS";

                    _with27.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with27.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with27.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with27.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with27.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with27.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with27.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with27.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with27.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with27.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with27.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with27.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with27.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with27.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with27.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with27.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with27.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with27.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with27.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with27.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with27.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Output;
                    _with27.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                    var _with28 = updPickUpDetails;
                    _with28.Connection = objWK.MyConnection;
                    _with28.CommandType = CommandType.StoredProcedure;
                    _with28.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_UPD";

                    _with28.Parameters.Add("PICK_DROP_MST_PK_IN", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Input;
                    _with28.Parameters["PICK_DROP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with28.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with28.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with28.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with28.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with28.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with28.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with28.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with28.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with28.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with28.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with28.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with28.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with28.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with28.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with28.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with28.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with28.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with28.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with28.Parameters.Add("MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with28.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with28.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with29 = objWK.MyDataAdapter;

                    _with29.InsertCommand = insPickUpDetails;
                    _with29.InsertCommand.Transaction = TRAN;

                    _with29.UpdateCommand = updPickUpDetails;
                    _with29.UpdateCommand.Transaction = TRAN;

                    RecAfct = _with29.Update(dsPickUpDetails);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }

                if ((dsDropDetails != null))
                {
                    var _with30 = insDropDetails;
                    _with30.Connection = objWK.MyConnection;
                    _with30.CommandType = CommandType.StoredProcedure;
                    _with30.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_INS";

                    _with30.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with30.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with30.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with30.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with30.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with30.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with30.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with30.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with30.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with30.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with30.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with30.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with30.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with30.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with30.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with30.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with30.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with30.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with30.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with30.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with30.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Output;
                    _with30.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                    var _with31 = updDropDetails;
                    _with31.Connection = objWK.MyConnection;
                    _with31.CommandType = CommandType.StoredProcedure;
                    _with31.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_UPD";

                    _with31.Parameters.Add("PICK_DROP_MST_PK_IN", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Input;
                    _with31.Parameters["PICK_DROP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with31.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with31.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with31.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with31.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with31.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with31.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with31.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with31.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with31.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with31.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with31.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with31.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with31.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with31.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with31.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with31.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with31.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with31.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with31.Parameters.Add("MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with31.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with31.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with32 = objWK.MyDataAdapter;

                    _with32.InsertCommand = insDropDetails;
                    _with32.InsertCommand.Transaction = TRAN;

                    _with32.UpdateCommand = updDropDetails;
                    _with32.UpdateCommand.Transaction = TRAN;

                    RecAfct = _with32.Update(dsDropDetails);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                //End Manjunath

                var _with33 = insPurchaseInvDetails;
                _with33.Connection = objWK.MyConnection;
                _with33.CommandType = CommandType.StoredProcedure;
                _with33.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_PIA_INS";

                _with33.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                _with33.Parameters.Add("COST_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "cost_element_mst_fk").Direction = ParameterDirection.Input;
                _with33.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with33.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "vendor_key").Direction = ParameterDirection.Input;
                _with33.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                _with33.Parameters.Add("INVOICE_NUMBER_IN", OracleDbType.Varchar2, 20, "invoice_number").Direction = ParameterDirection.Input;
                _with33.Parameters["INVOICE_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                _with33.Parameters.Add("INVOICE_DATE_IN", OracleDbType.Date, 20, "invoice_date").Direction = ParameterDirection.Input;
                _with33.Parameters["INVOICE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                _with33.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                _with33.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with33.Parameters.Add("INVOICE_AMT_IN", OracleDbType.Int32, 10, "invoice_amt").Direction = ParameterDirection.Input;
                _with33.Parameters["INVOICE_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with33.Parameters.Add("TAX_PERCENTAGE_IN", OracleDbType.Int32, 5, "tax_percentage").Direction = ParameterDirection.Input;
                _with33.Parameters["TAX_PERCENTAGE_IN"].SourceVersion = DataRowVersion.Current;

                _with33.Parameters.Add("TAX_AMT_IN", OracleDbType.Int32, 10, "tax_amt").Direction = ParameterDirection.Input;
                _with33.Parameters["TAX_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with33.Parameters.Add("ESTIMATED_AMT_IN", OracleDbType.Int32, 10, "estimated_amt").Direction = ParameterDirection.Input;
                _with33.Parameters["ESTIMATED_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with33.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "vendor_mst_fk").Direction = ParameterDirection.Input;
                _with33.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with33.Parameters.Add("ATTACHED_FILE_NAME_IN", OracleDbType.Varchar2, 50, "attached_file_name").Direction = ParameterDirection.Input;
                _with33.Parameters["ATTACHED_FILE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                _with33.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_PIA_PK").Direction = ParameterDirection.Output;
                _with33.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with34 = updPurchaseInvDetails;
                _with34.Connection = objWK.MyConnection;
                _with34.CommandType = CommandType.StoredProcedure;
                _with34.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_PIA_UPD";

                _with34.Parameters.Add("JOB_TRN_SEA_EXP_PIA_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_pia_pk").Direction = ParameterDirection.Input;
                _with34.Parameters["JOB_TRN_SEA_EXP_PIA_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with34.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                _with34.Parameters.Add("COST_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "cost_element_mst_fk").Direction = ParameterDirection.Input;
                _with34.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with34.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "vendor_key").Direction = ParameterDirection.Input;
                _with34.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                _with34.Parameters.Add("INVOICE_NUMBER_IN", OracleDbType.Varchar2, 20, "invoice_number").Direction = ParameterDirection.Input;
                _with34.Parameters["INVOICE_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                _with34.Parameters.Add("INVOICE_DATE_IN", OracleDbType.Date, 20, "invoice_date").Direction = ParameterDirection.Input;
                _with34.Parameters["INVOICE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                _with34.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                _with34.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with34.Parameters.Add("INVOICE_AMT_IN", OracleDbType.Int32, 10, "invoice_amt").Direction = ParameterDirection.Input;
                _with34.Parameters["INVOICE_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with34.Parameters.Add("TAX_PERCENTAGE_IN", OracleDbType.Int32, 5, "tax_percentage").Direction = ParameterDirection.Input;
                _with34.Parameters["TAX_PERCENTAGE_IN"].SourceVersion = DataRowVersion.Current;

                _with34.Parameters.Add("TAX_AMT_IN", OracleDbType.Int32, 10, "tax_amt").Direction = ParameterDirection.Input;
                _with34.Parameters["TAX_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with34.Parameters.Add("ESTIMATED_AMT_IN", OracleDbType.Int32, 10, "estimated_amt").Direction = ParameterDirection.Input;
                _with34.Parameters["ESTIMATED_AMT_IN"].SourceVersion = DataRowVersion.Current;

                _with34.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "vendor_mst_fk").Direction = ParameterDirection.Input;
                _with34.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with34.Parameters.Add("ATTACHED_FILE_NAME_IN", OracleDbType.Varchar2, 50, "attached_file_name").Direction = ParameterDirection.Input;
                _with34.Parameters["ATTACHED_FILE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                _with34.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with34.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with35 = delPurchaseInvDetails;
                _with35.Connection = objWK.MyConnection;
                _with35.CommandType = CommandType.StoredProcedure;
                _with35.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_PIA_DEL";

                _with35.Parameters.Add("JOB_TRN_SEA_EXP_PIA_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_pia_pk").Direction = ParameterDirection.Input;
                _with35.Parameters["JOB_TRN_SEA_EXP_PIA_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with35.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with35.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with36 = objWK.MyDataAdapter;

                _with36.InsertCommand = insPurchaseInvDetails;
                _with36.InsertCommand.Transaction = TRAN;

                _with36.UpdateCommand = updPurchaseInvDetails;
                _with36.UpdateCommand.Transaction = TRAN;

                _with36.DeleteCommand = delPurchaseInvDetails;
                _with36.DeleteCommand.Transaction = TRAN;

                RecAfct = _with36.Update(dsPurchaseInventory.Tables[0]);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                //'Added By Koteshwari on 22/4/2011
                var _with37 = insCostDetails;
                _with37.Connection = objWK.MyConnection;
                _with37.CommandType = CommandType.StoredProcedure;
                _with37.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_INS";

                _with37.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                _with37.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                _with37.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with37.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                _with37.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with37.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                _with37.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with37.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                _with37.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                _with37.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                _with37.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                _with37.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                _with37.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with37.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                _with37.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                _with37.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                _with37.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                //'surcharge
                _with37.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                _with37.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                _with37.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Output;
                _with37.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with38 = updCostDetails;
                _with38.Connection = objWK.MyConnection;
                _with38.CommandType = CommandType.StoredProcedure;
                _with38.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_UPD";

                _with38.Parameters.Add("JOB_TRN_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Input;
                _with38.Parameters["JOB_TRN_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with38.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                _with38.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                _with38.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with38.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 50, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                _with38.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with38.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                _with38.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with38.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                _with38.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                _with38.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                _with38.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                _with38.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                _with38.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with38.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                _with38.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                _with38.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                _with38.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                //'surcharge
                _with38.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                _with38.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                _with38.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with38.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with39 = delCostDetails;
                _with39.Connection = objWK.MyConnection;
                _with39.CommandType = CommandType.StoredProcedure;
                _with39.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_DEL";

                _with39.Parameters.Add("JOB_TRN_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Input;
                _with39.Parameters["JOB_TRN_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with39.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "Clng(RETURN_VALUE)").Direction = ParameterDirection.Output;
                _with39.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with40 = objWK.MyDataAdapter;

                _with40.InsertCommand = insCostDetails;
                _with40.InsertCommand.Transaction = TRAN;

                _with40.UpdateCommand = updCostDetails;
                _with40.UpdateCommand.Transaction = TRAN;

                _with40.DeleteCommand = delCostDetails;
                _with40.DeleteCommand.Transaction = TRAN;

                RecAfct = _with40.Update(dsCostDetails.Tables[0]);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                //'End Koteshwari

                var _with41 = insOtherChargesDetails;
                _with41.Connection = objWK.MyConnection;
                _with41.CommandType = CommandType.StoredProcedure;
                _with41.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_OTH_CHRG_INS";

                _with41.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                _with41.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                _with41.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with41.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "Payment_Type").Direction = ParameterDirection.Input;
                _with41.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                _with41.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                _with41.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with41.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                _with41.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with41.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                _with41.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with41.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                _with41.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                _with41.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                _with41.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                _with41.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                _with41.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                _with41.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_OTH_PK").Direction = ParameterDirection.Output;
                _with41.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with42 = updOtherChargesDetails;
                _with42.Connection = objWK.MyConnection;
                _with42.CommandType = CommandType.StoredProcedure;
                _with42.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_OTH_CHRG_UPD";

                _with42.Parameters.Add("JOB_TRN_SEA_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_oth_pk").Direction = ParameterDirection.Input;
                _with42.Parameters["JOB_TRN_SEA_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with42.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                _with42.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                _with42.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with42.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "Payment_Type").Direction = ParameterDirection.Input;
                _with42.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                _with42.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                _with42.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with42.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                _with42.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with42.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                _with42.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with42.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                _with42.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                _with42.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                _with42.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                _with42.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                _with42.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                _with42.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with42.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with43 = delOtherChargesDetails;
                _with43.Connection = objWK.MyConnection;
                _with43.CommandType = CommandType.StoredProcedure;
                _with43.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_OTH_CHRG_DEL";

                _with43.Parameters.Add("JOB_TRN_SEA_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_oth_pk").Direction = ParameterDirection.Input;
                _with43.Parameters["JOB_TRN_SEA_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with43.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with43.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with44 = objWK.MyDataAdapter;

                _with44.InsertCommand = insOtherChargesDetails;
                _with44.InsertCommand.Transaction = TRAN;

                _with44.UpdateCommand = updOtherChargesDetails;
                _with44.UpdateCommand.Transaction = TRAN;

                _with44.DeleteCommand = delOtherChargesDetails;
                _with44.DeleteCommand.Transaction = TRAN;

                RecAfct = _with44.Update(dsOtherCharges);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    if (intIsUpdate == 1)
                    {
                        if (!string.IsNullOrEmpty(getDefault(strOperatorPk, "").ToString()))
                        {
                            arrMessage = (ArrayList)funUpStreamUpdationBookingOpr(strBookingRefNo, strOperatorPk, TRAN);

                            if (!(string.Compare(arrMessage[0].ToString(), "Saved") > 0))
                            {
                                TRAN.Rollback();
                                return arrMessage;
                            }
                        }
                    }
                    cls_JobCardView objJCFclLcl = new cls_JobCardView();
                    objJCFclLcl.CREATED_BY = Convert.ToInt64(M_CREATED_BY_FK);
                    objJCFclLcl.LAST_MODIFIED_BY = Convert.ToInt64(M_CREATED_BY_FK);
                    objJCFclLcl.ConfigurationPK = Convert.ToInt64(M_Configuration_PK);
                    arrMessage = (ArrayList)objJCFclLcl.SaveJobCardDoc(Convert.ToString(JobCardPK), TRAN, dsDoc, 2, 1);
                    //Biztype -2(Sea),Process Type -1(Export)
                    if (arrMessage.Count > 0)
                    {
                        if (!(string.Compare(arrMessage[0].ToString(), "Saved") > 0))
                        {
                            TRAN.Rollback();
                            return arrMessage;
                        }
                    }
                    arrMessage = (ArrayList)SaveTrackAndTrace(Convert.ToInt32(JobCardPK), TRAN, M_DataSet, Convert.ToInt32(userLocation), isEdting, dsTrackNTrace);
                    if (arrMessage.Count > 0)
                    {
                        TRAN.Commit();
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
                TRAN.Rollback();
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }
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
                        Frt_Pk = Convert.ToInt32(ri["JOB_TRN_SEA_EXP_FD_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Frt_Pk = 0;
                    }
                    var _with45 = objWK.MyCommand;
                    _with45.Parameters.Clear();
                    _with45.Transaction = TRAN;
                    _with45.CommandType = CommandType.StoredProcedure;
                    if (Frt_Pk > 0)
                    {
                        _with45.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_UPD";
                        _with45.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", ri["JOB_TRN_SEA_EXP_FD_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with45.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_INS";
                        _with45.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                        _with45.Parameters.Add("JOB_TRN_SEA_EXP_CONT_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with45.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    _with45.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("BASIS_IN", getDefault(ri["BASIS"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("PRINT_ON_MBL_IN", 1).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("BASIS_FK_IN", getDefault(ri["BASIS_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("EXCHANGE_RATE_IN", getDefault(ri["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("RATE_PERBASIS_IN", getDefault(ri["RATEPERBASIS"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("SURCHARGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("QUANTITY_IN", getDefault(ri["VOLUME"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("SERVICE_MST_FK_IN", ri["SERVICE_MST_PK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with45.ExecuteNonQuery();
                        if (Frt_Pk == 0)
                        {
                            _with45.Parameters.Clear();
                            _with45.CommandType = CommandType.StoredProcedure;
                            _with45.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_FRT_SEQ_CURRVAL";
                            _with45.Parameters.Add("BIZ_IN", 2).Direction = ParameterDirection.Input;
                            _with45.Parameters.Add("PROCESS_IN", 1).Direction = ParameterDirection.Input;
                            _with45.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with45.ExecuteNonQuery();
                            Frt_Pk = Convert.ToInt32(_with45.Parameters["RETURN_VALUE"].Value);
                            ri["JOB_TRN_SEA_EXP_FD_PK"] = Frt_Pk;
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
                        Cost_Pk = Convert.ToInt32(re["JOB_TRN_SEA_EXP_COST_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Cost_Pk = 0;
                    }
                    var _with46 = objWK.MyCommand;
                    _with46.Parameters.Clear();
                    _with46.Transaction = TRAN;
                    _with46.CommandType = CommandType.StoredProcedure;
                    if (Cost_Pk > 0)
                    {
                        _with46.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_UPD";
                        _with46.Parameters.Add("JOB_TRN_EST_PK_IN", re["JOB_TRN_SEA_EXP_COST_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with46.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_INS";
                        _with46.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }
                    _with46.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("VENDOR_MST_FK_IN", re["SUPPLIER_MST_PK"]).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("COST_ELEMENT_FK_IN", re["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("LOCATION_FK_IN", re["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("VENDOR_KEY_IN", re["SUPPLIER_MST_ID"]).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("PTMT_TYPE_IN", re["PTMT_TYPE"]).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("CURRENCY_MST_FK_IN", re["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("ESTIMATED_COST_IN", re["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("TOTAL_COST_IN", re["TOTAL_COST"]).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("BASIS_FK_IN", re["DD_VALUE"]).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("RATEPERBASIS_IN", re["RATEPERBASIS"]).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("QUANTITY_IN", getDefault(re["VOLUME"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("EXCHANGE_RATE_IN", getDefault(re["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("EXT_INT_FLAG_IN", getDefault(re["EXT_INT_FLAG"], 2)).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("SERVICE_MST_FK_IN", re["SERVICE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with46.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with46.ExecuteNonQuery();
                        if (Cost_Pk == 0)
                        {
                            _with46.Parameters.Clear();
                            _with46.CommandType = CommandType.StoredProcedure;
                            _with46.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_COST_SEQ_CURRVAL";
                            _with46.Parameters.Add("BIZ_IN", 2).Direction = ParameterDirection.Input;
                            _with46.Parameters.Add("PROCESS_IN", 1).Direction = ParameterDirection.Input;
                            _with46.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with46.ExecuteNonQuery();
                            Cost_Pk = Convert.ToInt32(_with46.Parameters["RETURN_VALUE"].Value);
                            re["JOB_TRN_SEA_EXP_COST_PK"] = Cost_Pk;
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
                            SelectedFrtPks = Convert.ToString(getDefault(ri["JOB_TRN_SEA_EXP_FD_PK"], 0));
                        }
                        else
                        {
                            SelectedFrtPks += "," + getDefault(ri["JOB_TRN_SEA_EXP_FD_PK"], 0);
                        }
                    }
                    foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedCostPks))
                        {
                            SelectedCostPks = Convert.ToString(getDefault(re["JOB_TRN_SEA_EXP_COST_PK"], 0));
                        }
                        else
                        {
                            SelectedCostPks += "," + getDefault(re["JOB_TRN_SEA_EXP_COST_PK"], 0);
                        }
                    }

                    var _with47 = objWK.MyCommand;
                    _with47.Transaction = TRAN;
                    _with47.CommandType = CommandType.StoredProcedure;
                    _with47.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.DELETE_SEA_EXP_SEC_CHG_EXCEPT";
                    _with47.Parameters.Clear();
                    _with47.Parameters.Add("JOB_CARD_SEA_EXP_PK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with47.Parameters.Add("JOB_TRN_SEA_EXP_FD_PKS", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
                    _with47.Parameters.Add("JOB_TRN_SEA_EXP_COST_PKS", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
                    _with47.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    _with47.ExecuteNonQuery();
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
        #endregion

        #region "Fetch Vessel/Voyage Detail"
        public DataSet FetchVoyageDetail(string VoyagePk)
        {

            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT");
            strSQL.Append("      VVT.VESSEL_ID,");
            strSQL.Append("      VVT.VESSEL_NAME,");
            strSQL.Append("      VTR.VOYAGE");
            strSQL.Append(" FROM");
            strSQL.Append("      VESSEL_VOYAGE_TBL VVT,");
            strSQL.Append("      VESSEL_VOYAGE_TRN VTR");
            strSQL.Append(" WHERE");
            strSQL.Append("      VVT.VESSEL_VOYAGE_TBL_PK = VTR.VESSEL_VOYAGE_TBL_FK");
            strSQL.Append(" AND  VTR.VOYAGE_TRN_PK=" + VoyagePk);

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
        #endregion

        #region "CustomerID"
        //To Get Customer ID
        //By Amit on 23-May-07
        public DataSet GetCustomerID(string CustomerPK)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("SELECT CMT.CUSTOMER_ID");
                strQuery.Append("   FROM CUSTOMER_MST_TBL CMT");
                strQuery.Append("  WHERE CMT.CUSTOMER_MST_PK= '" + CustomerPK + "'");
                strQuery.Append("");
                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }
        //End
        #endregion

        #region " Save Track And Trace "
        public object SaveTrackAndTrace(int jobPk, OracleTransaction TRAN, DataSet dsMain, int nlocationfk, bool IsEditing, DataSet dsContainer)
        {
            try
            {
                int Cnt = 0;
                int a = 0;
                bool UpdATD = false;
                bool UpdLDdate = false;
                string strContData = null;
                if (!string.IsNullOrEmpty((dsMain.Tables[0].Rows[Cnt]["departure_date"].ToString())))
                {
                    UpdATD = true;
                }
                objTrackNTrace.DeleteOnSaveTraceExportOnATDLDUpd(jobPk, 2, 1, "Vessel Voyage", "LD-DT-DATA-DEL-SEA-EXP", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                "Null");


                for (Cnt = 0; Cnt <= dsContainer.Tables[0].Rows.Count - 1; Cnt++)
                {
                    if (!string.IsNullOrEmpty((dsContainer.Tables[0].Rows[Cnt]["GLD"].ToString())))
                    {
                        UpdLDdate = true;
                        var _with48 = dsContainer.Tables[0].Rows[Cnt];
                        // Updated by Amit on 05-Jan-07 For Task DTS-1833
                        if (string.IsNullOrEmpty((dsMain.Tables[0].Rows[0]["vessel_name"].ToString())))
                        {
                            strContData = "Loaded Container " + _with48["ContainerNo"] + "~" + _with48["GLD"];
                        }
                        else
                        {
                            strContData = "Loaded Container " + _with48["ContainerNo"] + " On " + dsMain.Tables[0].Rows[0]["vessel_name"] + "/" + dsMain.Tables[0].Rows[0]["voyage"] + "~" + _with48["GLD"];
                        }
                        // End
                        arrMessage = objTrackNTrace.SaveBBTrackAndTraceExportOnLDUpd(jobPk, 2, 1, "Vessel Voyage", "LD-DT-UPD-JOB-SEA-EXP", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                        strContData);
                    }
                }


                //If UpdLDdate = True Then
                //    arrMessage = objTrackNTrace.SaveTrackAndTraceExportOnATDLDUpd(jobPk, 2, 1, "Vessel Voyage", "LD-DT-UPD-JOB-SEA-EXP", nlocationfk, TRAN, "INS", "O")
                //End If

                if (UpdATD == true & IsEditing == true)
                {
                    //Added by Venkata to get status like "sailed from XXX POL"
                    for (Cnt = 0; Cnt <= dsMain.Tables[0].Rows.Count - 1; Cnt++)
                    {
                        if (!string.IsNullOrEmpty((dsMain.Tables[0].Rows[Cnt]["departure_date"].ToString())))
                        {
                            //UpdATD = True
                            var _with49 = dsMain.Tables[0].Rows[Cnt];
                            strContData = "Sailed from " + _with49["POL"] + "~" + _with49["departure_date"];
                            arrMessage = objTrackNTrace.SaveBBTrackAndTraceExportOnATDUpd(jobPk, 2, 1, "Sail", "ATD-UPD-JC-SEA-EXP", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                            strContData);
                        }
                    }
                    //end Added
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
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Main Jobcard for export"
        public DataSet FetchMainJobCardDataExp(string jobCardPK = "0")
        {

            StringBuilder strSQL = new StringBuilder();
            //Changed by Snigdharani - for customer id, bkg date, mbl date, hbl date
            strSQL.Append("SELECT ");
            strSQL.Append("    job_exp.job_card_sea_exp_pk, ");
            strSQL.Append("    job_exp.booking_sea_fk,  ");
            strSQL.Append("    bst.BOOKING_DATE,  ");
            strSQL.Append("    job_exp.jobcard_ref_no, ");
            strSQL.Append("    bst.booking_ref_no, ");
            strSQL.Append("    bst.cargo_type, cust.customer_id,");
            strSQL.Append("    bst.cust_customer_mst_fk,");
            strSQL.Append("    cust.customer_name, job_exp.del_address,");
            strSQL.Append("    bst.col_place_mst_fk, ");
            strSQL.Append("    col_place.place_name \"CollectionPlace\",");
            strSQL.Append("    bst.port_mst_pol_fk, ");
            strSQL.Append("    pol.port_name \"POL\",");
            strSQL.Append("    bst.port_mst_pod_fk, ");
            strSQL.Append("    pod.port_name \"POD\",");
            strSQL.Append("    bst.del_place_mst_fk, ");
            strSQL.Append("    del_place.place_name \"DeliveryPlace\",");
            strSQL.Append("    job_card_status,     ");
            strSQL.Append("    job_card_closed_on,  ");
            strSQL.Append("    job_exp.WIN_XML_STATUS,");
            strSQL.Append("    job_exp.WIN_TOTAL_QTY,");
            strSQL.Append("    job_exp.WIN_REC_QTY,");
            strSQL.Append("    job_exp.WIN_BALANCE_QTY,");
            strSQL.Append("    job_exp.WIN_TOTAL_WT,");
            strSQL.Append("    job_exp.WIN_REC_WT,");
            strSQL.Append("    job_exp.WIN_BALANCE_WT,");
            strSQL.Append("    bst.operator_mst_fk, ");
            strSQL.Append("    oprator.operator_id \"operator_id\",");
            strSQL.Append("    oprator.operator_name \"operator_name\",");
            //strSQL.Append(vbCrLf & "    job_exp.vessel_name ""vessel_name"",  ")
            //strSQL.Append(vbCrLf & "    job_exp.voyage ""voyage"",   ")
            strSQL.Append("    VVT.VOYAGE_TRN_PK \"VoyagePK\",  ");
            strSQL.Append("    V.VESSEL_ID \"vessel_id\",   ");
            strSQL.Append("    V.VESSEL_NAME \"vessel_name\",  ");
            strSQL.Append("    VVT.VOYAGE \"voyage\",   ");
            strSQL.Append("    TO_CHAR(job_exp.eta_date,DATETIMEFORMAT24)eta_date , ");
            strSQL.Append("    TO_CHAR(job_exp.etd_date,DATETIMEFORMAT24) etd_date, ");
            strSQL.Append("    TO_CHAR(job_exp.arrival_date,DATETIMEFORMAT24) arrival_date, ");
            strSQL.Append("    TO_CHAR(job_exp.departure_date,DATETIMEFORMAT24) departure_date, ");
            strSQL.Append("    job_exp.sec_vessel_name, ");
            strSQL.Append("    job_exp.sec_voyage,  ");
            strSQL.Append("    TO_CHAR(job_exp.sec_eta_date,DATEFORMAT) sec_eta_date, ");
            strSQL.Append("    TO_CHAR(job_exp.sec_etd_date,DATEFORMAT) sec_etd_date, ");
            strSQL.Append("    job_exp.shipper_cust_mst_fk, ");
            strSQL.Append("    shipper.customer_id \"Shipper\", ");
            strSQL.Append("    shipper.customer_name \"ShipperName\", ");
            strSQL.Append("    job_exp.consignee_cust_mst_fk, ");
            strSQL.Append("    consignee.customer_id \"Consignee\",");
            strSQL.Append("    consignee.customer_name \"ConsigneeName\",");
            strSQL.Append("    job_exp.notify1_cust_mst_fk, ");
            strSQL.Append("    notify1.customer_id \"Notify1\" ,");
            strSQL.Append("    notify1.customer_name \"Notify1Name\" ,");
            strSQL.Append("    job_exp.notify2_cust_mst_fk, ");
            strSQL.Append("    notify2.customer_id \"Notify2\",");
            strSQL.Append("    notify2.customer_name \"Notify2Name\",");
            strSQL.Append("    job_exp.cb_agent_mst_fk, ");
            strSQL.Append("    cbagnt.agent_id \"cbAgent\",");
            strSQL.Append("    cbagnt.agent_name \"cbAgentName\",");
            strSQL.Append("    job_exp.dp_agent_mst_fk, ");
            strSQL.Append("    dpagnt.agent_id \"dpAgent\",");
            strSQL.Append("    dpagnt.agent_name \"dpAgentName\",");
            strSQL.Append("    job_exp.cl_agent_mst_fk, ");
            strSQL.Append("    clagnt.agent_id \"clAgent\",");
            strSQL.Append("    clagnt.agent_name \"clAgentName\",");
            strSQL.Append("    job_exp.remarks,  ");
            strSQL.Append("    job_exp.version_no,  ");
            strSQL.Append("    job_exp.jobcard_date,");
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
            strSQL.Append("    job_exp.hbl_exp_tbl_fk, ");
            strSQL.Append("    job_exp.mbl_exp_tbl_fk, ");
            strSQL.Append("    job_exp.master_jc_sea_exp_fk, ");
            strSQL.Append("    mst.master_jc_ref_no, ");
            strSQL.Append("    mst.MASTER_JC_DATE, ");
            //Added On March 30 - Rijesh
            strSQL.Append("    job_exp.GOODS_DESCRIPTION,");
            strSQL.Append("    job_exp.MARKS_NUMBERS,");
            //End

            //Added by Manoharan for display HBL&MBL no.
            strSQL.Append("    hbl.hbl_ref_no hbl_no,");
            strSQL.Append("    hbl.hbl_date,");
            strSQL.Append("    mbl.mbl_ref_no mbl_no,");
            strSQL.Append("    mbl.mbl_date,");

            strSQL.Append("    job_exp.SHIPPING_INST_FLAG,");
            strSQL.Append("    job_exp.RFS,");
            strSQL.Append("    job_exp.CRQ,");
            strSQL.Append("    job_exp.STF,");
            strSQL.Append("    job_exp.SHIPPING_INST_DT,");
            strSQL.Append("    job_exp.RFS_DATE,");
            strSQL.Append("    job_exp.CRQ_DATE,");
            strSQL.Append("    job_exp.STF_DATE,");
            strSQL.Append("    job_exp.SURVEY_COMPLETED,");
            strSQL.Append("    job_exp.SURVEY_REF_NR,");
            strSQL.Append("    job_exp.SURVEY_DATE,");
            strSQL.Append("    job_exp.SURVEYOR_FK \"Survey_PK\",");
            strSQL.Append("    job_exp.SURVEY_REMARKS,");
            strSQL.Append("    Surveyor.vendor_id,");
            strSQL.Append("    Surveyor.vendor_name,");
            //end

            //Code Added By Anil on 18 Aug 09
            strSQL.Append("    job_exp.sb_number,job_exp.sb_date, ");
            strSQL.Append("    job_exp.cha_agent_mst_fk, ");
            strSQL.Append("    chaagnt.VENDOR_ID \"CHAAgentID\",");
            strSQL.Append("    chaagnt.VENDOR_NAME \"CHAAgentName\",job_exp.stuff_loc,");
            strSQL.Append("   curr.currency_id,HBL.HBL_STATUS,job_exp.LC_SHIPMENT,");
            strSQL.Append("    NVL(JOB_EXP.CHK_NOMINATED,0) CHK_NOMINATED,");
            strSQL.Append("    NVL(JOB_EXP.CHK_CSR,1) CHK_CSR,");
            strSQL.Append("    NVL(EMP.EMPLOYEE_MST_PK,NVL(SHP_SE.EMPLOYEE_MST_PK,0)) SALES_EXEC_FK,");
            strSQL.Append("    NVL(EMP.EMPLOYEE_ID,SHP_SE.EMPLOYEE_ID) SALES_EXEC_ID,");
            strSQL.Append("    NVL(EMP.EMPLOYEE_NAME,SHP_SE.EMPLOYEE_NAME) SALES_EXEC_NAME , ");
            strSQL.Append("    job_exp.cc_req,job_exp.cc_ie,job_exp.PRC_FK,job_exp.ONC_FK,job_exp.PRC_MODE_FK,job_exp.ONC_MODE_FK ");
            strSQL.Append(" FROM ");
            strSQL.Append("    job_card_sea_exp_tbl job_exp,");
            strSQL.Append("    booking_sea_tbl bst,");
            strSQL.Append("    port_mst_tbl POD,");
            strSQL.Append("    port_mst_tbl POL,");
            strSQL.Append("    customer_mst_tbl cust,");
            strSQL.Append("    customer_mst_tbl consignee,");
            strSQL.Append("    customer_mst_tbl shipper,");
            strSQL.Append("    customer_mst_tbl notify1,");
            strSQL.Append("    customer_mst_tbl notify2,");
            strSQL.Append("    place_mst_tbl col_place,");
            strSQL.Append("    place_mst_tbl del_place,");
            strSQL.Append("    operator_mst_tbl oprator,");
            strSQL.Append("    agent_mst_tbl clagnt, ");
            strSQL.Append("    agent_mst_tbl dpagnt, ");
            strSQL.Append("    agent_mst_tbl cbagnt, ");
            strSQL.Append("    VENDOR_MST_TBL chaagnt, ");
            strSQL.Append("    commodity_group_mst_tbl comm, ");
            strSQL.Append("    VESSEL_VOYAGE_TBL V,  ");
            strSQL.Append("    VESSEL_VOYAGE_TRN VVT, ");
            strSQL.Append("    vendor_mst_tbl  depot,");
            strSQL.Append("    vendor_mst_tbl  carrier,");
            strSQL.Append("    vendor_mst_tbl  Surveyor,");
            strSQL.Append("    country_mst_tbl country,");
            strSQL.Append("    master_jc_sea_exp_tbl mst,");
            strSQL.Append("    hbl_exp_tbl hbl,");
            strSQL.Append("    mbl_exp_tbl mbl,");
            strSQL.Append("    currency_type_mst_tbl   curr,");
            strSQL.Append("    EMPLOYEE_MST_TBL        EMP, ");
            strSQL.Append("    EMPLOYEE_MST_TBL        SHP_SE ");
            //SHIPPER SALES PERSON
            strSQL.Append(" WHERE ");
            strSQL.Append("    job_exp.job_card_sea_exp_pk = " + jobCardPK);
            strSQL.Append("    AND job_exp.booking_sea_fk           =  bst.booking_sea_pk");
            strSQL.Append("    AND bst.port_mst_pol_fk              =  pol.port_mst_pk");
            strSQL.Append("    AND bst.port_mst_pod_fk              =  pod.port_mst_pk");
            strSQL.Append("    AND bst.col_place_mst_fk             =  col_place.place_pk(+)");
            strSQL.Append("    AND bst.del_place_mst_fk             =  del_place.place_pk(+)");
            strSQL.Append("    AND bst.cust_customer_mst_fk         =  cust.customer_mst_pk(+) ");
            strSQL.Append("    AND bst.operator_mst_fk              =  oprator.operator_mst_pk(+)");
            strSQL.Append("    AND job_exp.shipper_cust_mst_fk      =  shipper.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.consignee_cust_mst_fk    =  consignee.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.notify1_cust_mst_fk      =  notify1.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.Notify2_Cust_Mst_Fk      =  notify2.customer_mst_pk(+)");
            strSQL.Append("    AND job_exp.cl_agent_mst_fk          =  clagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.cha_agent_mst_fk         =  chaagnt.VENDOR_MST_PK(+)");
            //Added By Anil
            strSQL.Append("    AND job_exp.cb_agent_mst_fk          =  cbagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.dp_agent_mst_fk          =  dpagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_exp.commodity_group_fk       =  comm.commodity_group_pk(+)");

            //conditions are added after the UAT..
            strSQL.Append("    AND job_exp.transporter_depot_fk     =  depot.vendor_mst_pk(+)");
            strSQL.Append("    AND job_exp.transporter_carrier_fk   =  carrier.vendor_mst_pk(+)");
            strSQL.Append("    AND job_exp.surveyor_fk              =  Surveyor.vendor_mst_pk(+)");
            strSQL.Append("    AND job_exp.country_origin_fk        =  country.country_mst_pk(+)");
            strSQL.Append("    AND VVT.VESSEL_VOYAGE_TBL_FK         =  V.VESSEL_VOYAGE_TBL_PK(+)  ");
            strSQL.Append("    AND JOB_EXP.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
            strSQL.Append("    AND job_exp.master_jc_sea_exp_fk     =  mst.master_jc_sea_exp_pk(+)");

            strSQL.Append("    AND shipper.REP_EMP_MST_FK=SHP_SE.EMPLOYEE_MST_PK(+) ");
            strSQL.Append("    AND JOB_EXP.EXECUTIVE_MST_FK=EMP.EMPLOYEE_MST_PK(+) ");
            //added by manoharan for display HBL&MBL no.
            strSQL.Append("    and hbl.hbl_exp_tbl_pk(+) = job_exp.hbl_exp_tbl_fk");
            strSQL.Append("    and mbl.mbl_exp_tbl_pk(+) = job_exp.mbl_exp_tbl_fk");
            //end
            //added by surya prasad for introducing base currency.
            strSQL.Append("     and curr.currency_mst_pk(+) = job_exp.base_currency_mst_fk");
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
        #endregion

        #region " Fetch Container data export"
        public DataSet FetchContainerDataExport(string jobCardPK = "0", string MJCPK = "")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                //by thiyagarajan for displaying container details in frmcargodetails.aspx which has link of booking sea.
                //26/2/08 
                strSQL.Append("SELECT");
                strSQL.Append("    job_trn_sea_exp_cont_pk,");
                strSQL.Append("    container_number,");
                strSQL.Append("    cont.container_type_mst_id,");
                strSQL.Append("    container_type_mst_fk,");
                strSQL.Append("    seal_number,");
                strSQL.Append("    volume_in_cbm,");
                strSQL.Append("    gross_weight,");
                strSQL.Append("    net_weight,");
                strSQL.Append("    chargeable_weight,");
                strSQL.Append("    pack_type_mst_fk,");
                strSQL.Append("    pack_count,");
                strSQL.Append("    commodity_mst_fk,");
                if (string.IsNullOrEmpty(MJCPK))
                {
                    strSQL.Append("    TO_CHAR(job_trn_cont.load_date ,DATETIMEFORMAT24) load_date ");
                    // for now..
                }
                else
                {
                    strSQL.Append("    TO_CHAR(job_exp.departure_date ,DATETIMEFORMAT24) load_date ");
                }
                //Snigdharani - 21/08/2009
                strSQL.Append("     , job_trn_cont.CONTAINER_PK CONTAINER_PK");
                //strSQL.Append(vbCrLf & "    job_trn_cont.load_date load_date ")
                strSQL.Append("FROM");
                strSQL.Append("    job_trn_sea_exp_cont job_trn_cont,");
                strSQL.Append("    pack_type_mst_tbl pack,");
                strSQL.Append("    commodity_mst_tbl comm,");
                strSQL.Append("    container_type_mst_tbl cont,");
                strSQL.Append("    job_card_sea_exp_tbl job_exp");
                strSQL.Append("WHERE ");
                strSQL.Append("    job_trn_cont.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
                strSQL.Append("    AND job_trn_cont.container_type_mst_fk = cont.container_type_mst_pk(+)");
                strSQL.Append("    AND job_trn_cont.commodity_mst_fk = comm.commodity_mst_pk(+)");
                strSQL.Append("    AND job_trn_cont.job_card_sea_exp_fk = job_exp.job_card_sea_exp_pk");
                strSQL.Append("    AND job_exp.job_card_sea_exp_pk =" + jobCardPK);

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
        #endregion

        #region " Fetch Container data export"
        public DataSet FetchContainerDataExp(string jobCardPK = "0", string MJCPK = "")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                //modified the order by latha for conainer type and number interchange

                strSQL.Append("SELECT");
                strSQL.Append("    job_trn_cont_pk PK,");
                strSQL.Append("    CG.COMMODITY_GROUP_CODE ContainerTypePK,");
                strSQL.Append("    CG.COMMODITY_GROUP_PK  ContainerTypeID,");
                strSQL.Append("   COMM.COMMODITY_NAME fetch_comm,pack_type_mst_fk PackTypePK,");
                strSQL.Append("   DM.DIMENTION_ID GrossWt,pack_count Nos, chargeable_weight ChargeableWeight ,volume_in_cbm Volume,");
                strSQL.Append("   container_number ContainerNo,seal_number SealNo, commodity_mst_fk CommodityPK,");
                strSQL.Append("    net_weight NetWt,");
                if (string.IsNullOrEmpty(MJCPK))
                {
                    strSQL.Append("    TO_CHAR(job_trn_cont.load_date ,DATETIMEFORMAT24) GLD, ");
                    // for now..
                }
                else
                {
                    strSQL.Append("    TO_CHAR(job_exp.departure_date ,DATETIMEFORMAT24) GLD, ");
                }
                strSQL.Append("    DM.DIMENTION_UNIT_MST_PK COMMODITY_MST_FKS");
                //added By prakash chandra on 6/1/2009 for implementing multiple commodities
                strSQL.Append("     , job_trn_cont.CONTAINER_PK Container_No_pk");
                //strSQL.Append(vbCrLf & "    job_trn_cont.load_date load_date ")
                strSQL.Append("FROM");
                strSQL.Append("    job_trn_cont job_trn_cont,");
                strSQL.Append("    pack_type_mst_tbl pack,DIMENTION_UNIT_MST_TBL DM,");
                strSQL.Append("    commodity_mst_tbl comm,");
                strSQL.Append("    container_type_mst_tbl cont,");
                strSQL.Append("    job_card_trn job_exp ,COMMODITY_GROUP_MST_TBL CG");
                strSQL.Append("WHERE ");
                strSQL.Append("    job_trn_cont.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
                strSQL.Append("    AND CG.COMMODITY_GROUP_PK=COMM.COMMODITY_GROUP_FK");
                strSQL.Append("    AND DM.DIMENTION_UNIT_MST_PK(+)=JOB_TRN_CONT.BASIS_FK");
                strSQL.Append("    AND job_trn_cont.container_type_mst_fk = cont.container_type_mst_pk(+)");
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
        //Added by surya prasad for implementing multiple commodity task
        public DataSet FetchContainerDataExpBooking(string jobCardPK = "0", string MJCPK = "")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    job_trn_sea_exp_cont_pk,");
                strSQL.Append("    container_number,");
                strSQL.Append("    cont.container_type_mst_id,");
                strSQL.Append("    container_type_mst_fk,");
                strSQL.Append("    seal_number,");
                strSQL.Append("    volume_in_cbm,");
                strSQL.Append("    gross_weight,");
                strSQL.Append("    net_weight,");
                strSQL.Append("    chargeable_weight,");
                strSQL.Append("    pack_type_mst_fk,");
                strSQL.Append("    pack_count,");
                strSQL.Append("    commodity_mst_fk,");
                strSQL.Append("    ' ' fetch_comm,");
                //added by surya rasad for implementing multiple commodities
                if (string.IsNullOrEmpty(MJCPK))
                {
                    strSQL.Append("    TO_CHAR(job_trn_cont.load_date ,DATETIMEFORMAT24) load_date, ");
                    // for now..
                }
                else
                {
                    strSQL.Append("    TO_CHAR(job_exp.departure_date ,DATETIMEFORMAT24) load_date, ");
                }
                strSQL.Append("     COMMODITY_MST_FKS ");
                strSQL.Append("     , job_trn_cont.CONTAINER_PK CONTAINER_PK");
                //strSQL.Append(vbCrLf & "    job_trn_cont.load_date load_date ")
                strSQL.Append("FROM");
                strSQL.Append("    job_trn_sea_exp_cont job_trn_cont,");
                strSQL.Append("    pack_type_mst_tbl pack,");
                strSQL.Append("    commodity_mst_tbl comm,");
                strSQL.Append("    container_type_mst_tbl cont,");
                strSQL.Append("    job_card_sea_exp_tbl job_exp");
                strSQL.Append("WHERE ");
                strSQL.Append("    job_trn_cont.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
                strSQL.Append("    AND job_trn_cont.container_type_mst_fk = cont.container_type_mst_pk(+)");
                strSQL.Append("    AND job_trn_cont.commodity_mst_fk = comm.commodity_mst_pk(+)");
                strSQL.Append("    AND job_trn_cont.job_card_sea_exp_fk = job_exp.job_card_sea_exp_pk");
                strSQL.Append("    AND job_exp.job_card_sea_exp_pk =" + jobCardPK);

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
        #endregion

        #region "Frieght Element"
        public DataSet FetchFret(int jobcardpk)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT CON.FRT_OTH_ELEMENT_FK, COMM.COMMODITY_MST_PK,COMM.COMMODITY_NAME");
                sb.Append("  FROM CONSOL_INVOICE_TRN_TBL CON,");
                sb.Append("       JOB_TRN_SEA_EXP_FD     FD_TRN,");
                sb.Append("       JOB_TRN_SEA_EXP_CONT   JOB_TRN,");
                sb.Append("       COMMODITY_MST_TBL      COMM");
                sb.Append(" WHERE CON.FRT_OTH_ELEMENT = 1");
                sb.Append("   AND JOB_TRN.JOB_CARD_SEA_EXP_FK = FD_TRN.JOB_CARD_SEA_EXP_FK");
                sb.Append("   AND JOB_TRN.COMMODITY_MST_FK = COMM.COMMODITY_MST_PK");
                sb.Append("   AND CON.FRT_OTH_ELEMENT_FK = FD_TRN.FREIGHT_ELEMENT_MST_FK");
                sb.Append("   AND CON.CONSOL_INVOICE_TRN_PK = FD_TRN.CONSOL_INVOICE_TRN_FK");
                sb.Append("   AND CON.JOB_CARD_FK = " + jobcardpk);
                sb.Append("   AND NVL(FD_TRN.SERVICE_TYPE_FLAG,0)<>1");
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
        public DataSet FetchAgentFret(int jobcardpk)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT INVTRN.COST_FRT_ELEMENT_FK, COMM.COMMODITY_MST_PK, COMM.COMMODITY_NAME");
                sb.Append("  FROM INV_AGENT_TBL     INV,");
                sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("       JOB_TRN_SEA_EXP_FD        JOB_TRN_FD,");
                sb.Append("       JOB_TRN_SEA_EXP_CONT      JOB_TRN,");
                sb.Append("       COMMODITY_MST_TBL         COMM");
                sb.Append(" WHERE INVTRN.COST_FRT_ELEMENT = 2");
                sb.Append("   AND COMM.COMMODITY_MST_PK = JOB_TRN.COMMODITY_MST_FK(+)");
                sb.Append("   AND INV.JOB_CARD_FK = JOB_TRN_FD.JOB_CARD_SEA_EXP_FK");
                sb.Append("   AND JOB_TRN.JOB_CARD_SEA_EXP_FK = INV.JOB_CARD_FK");
                sb.Append("   AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
                sb.Append("   AND INVTRN.COST_FRT_ELEMENT_FK = JOB_TRN_FD.FREIGHT_ELEMENT_MST_FK");
                sb.Append("   AND INV.JOB_CARD_FK = " + jobcardpk);
                sb.Append("   AND NVL(JOB_TRN_FD.SERVICE_TYPE_FLAG,0)<>1 ");
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
        #endregion

        #region " Fetch Freight data export"
        //added by manoharan 2/11/2006 for disable the entry in Fre. Data when already invoiced 
        public DataSet FetchFreDet(string jcpk)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    job_trn_fd.invoice_sea_tbl_fk,");
                strSQL.Append("    job_trn_fd.inv_agent_trn_sea_exp_fk,");
                strSQL.Append("    job_trn_fd.consol_invoice_trn_fk");
                strSQL.Append("    FROM");
                strSQL.Append("    job_trn_sea_exp_fd job_trn_fd,");
                strSQL.Append("    container_type_mst_tbl cont,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    freight_element_mst_tbl freight,");
                strSQL.Append("    job_card_sea_exp_tbl job_exp");
                strSQL.Append("    WHERE");
                strSQL.Append("    job_trn_fd.job_card_sea_exp_fk = job_exp.job_card_sea_exp_pk");
                strSQL.Append("    AND job_trn_fd.container_type_mst_fk = cont.container_type_mst_pk(+)");
                strSQL.Append("    AND job_trn_fd.Currency_Mst_Fk = curr.currency_mst_pk");
                strSQL.Append("    AND job_trn_fd.freight_element_mst_fk = freight.freight_element_mst_pk");
                strSQL.Append("    AND job_exp.job_card_sea_exp_pk =" + jcpk);
                strSQL.Append("    AND NVL(job_trn_fd.SERVICE_TYPE_FLAG,0)<>1");
                strSQL.Append("    ORDER BY cont.container_type_mst_id,freight.freight_element_id ");

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
        public DataSet FetchFreightDataExp(string jobCardPK = "0", string BaseCurrFk = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT QRY.job_trn_fd_pk,");
                strSQL.Append("       QRY.container_type_mst_fk,");
                strSQL.Append("       QRY.freight_element_id,");
                strSQL.Append("       QRY.freight_element_name,");
                strSQL.Append("       QRY.freight_element_mst_pk,");
                strSQL.Append("       QRY.basis,");
                strSQL.Append("       QRY.quantity,");
                strSQL.Append("       QRY.freight_type,");
                strSQL.Append("       QRY.location_mst_fk,");
                strSQL.Append("       QRY.location_id,");
                strSQL.Append("       QRY.frtpayer_cust_mst_fk,");
                strSQL.Append("       QRY.customer_id,");
                strSQL.Append("       QRY.currency_mst_fk,");
                strSQL.Append("       QRY.RATEPERBASIS,");
                strSQL.Append("       QRY.FREIGHT_AMT,");
                strSQL.Append("       QRY.ROE,");
                strSQL.Append("       QRY.TOTAL,");
                strSQL.Append("       \"Delete\",");
                strSQL.Append("       \"Print\",");
                strSQL.Append("       QRY.COMMODITY_MST_PK,");
                strSQL.Append("       QRY.job_trn_cont_pk,");
                strSQL.Append("       QRY.CURR_FK,");
                strSQL.Append("       QRY.CHARGEABLE_WEIGHT,");
                strSQL.Append("       QRY.VOLUME_IN_CBM,");
                strSQL.Append("       QRY.DIMENTION_ID,");
                strSQL.Append("       QRY.CREDIT");
                strSQL.Append(" FROM (");
                strSQL.Append("SELECT");
                strSQL.Append("    job_trn_fd_pk,\t");
                strSQL.Append("   COMM.COMMODITY_NAME container_type_mst_fk,");
                strSQL.Append("    freight.freight_element_id,");
                strSQL.Append("    freight.freight_element_name,");
                strSQL.Append("    freight.freight_element_mst_pk,\t");
                strSQL.Append("    job_trn_fd.basis,");
                strSQL.Append("    JCT.PACK_COUNT quantity,");
                strSQL.Append("    DECODE(job_trn_fd.freight_type,1,'Prepaid',2,'Collect') freight_type, ");
                strSQL.Append("    job_trn_fd.location_mst_fk, ");
                strSQL.Append("    lmt.location_id ,");
                strSQL.Append("    job_trn_fd.frtpayer_cust_mst_fk,");
                strSQL.Append("    cmt.customer_id, ");
                strSQL.Append("    CURR.CURRENCY_ID currency_mst_fk, ABS(JOB_TRN_FD.RATEPERBASIS)RATEPERBASIS,");
                strSQL.Append(" (ABS( CASE");
                strSQL.Append("                WHEN DU.DIMENTION_ID = 'W/M' THEN");
                strSQL.Append("                  CASE");
                strSQL.Append("                    WHEN JCT.VOLUME_IN_CBM > JCT.CHARGEABLE_WEIGHT THEN");
                strSQL.Append("                     ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.VOLUME_IN_CBM, 2)");
                strSQL.Append("                    WHEN JCT.CHARGEABLE_WEIGHT > JCT.VOLUME_IN_CBM THEN");
                strSQL.Append("                     ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.CHARGEABLE_WEIGHT, 2)");
                strSQL.Append("                    ELSE");
                strSQL.Append("                     ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.CHARGEABLE_WEIGHT, 2)");
                strSQL.Append("                  END");
                strSQL.Append("                 WHEN DU.DIMENTION_ID = 'CBM' THEN");
                strSQL.Append("                  ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.VOLUME_IN_CBM, 2)");
                strSQL.Append("                 WHEN DU.DIMENTION_ID = 'MT' THEN");
                strSQL.Append("                  ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.CHARGEABLE_WEIGHT, 2)");
                strSQL.Append("                 WHEN DU.DIMENTION_ID = 'UNIT' THEN");
                strSQL.Append("                  ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.PACK_COUNT, 2)");
                strSQL.Append("                 ELSE");
                strSQL.Append("                  ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.PACK_COUNT, 2)");
                strSQL.Append("               END)*DECODE(FREIGHT.Credit,NULL,1,0,-1,1,1)) FREIGHT_AMT,");
                if (Convert.ToInt32(BaseCurrFk) != 0)
                {
                    strSQL.Append("       ROUND(GET_EX_RATE(JOB_TRN_FD.CURRENCY_MST_FK, " + BaseCurrFk + ", round(TO_DATE(job_exp.JOBCARD_DATE,DATEFORMAT) - .5)), 4) AS ROE,");
                    strSQL.Append("    (JOB_TRN_FD.FREIGHT_AMT*ROUND(GET_EX_RATE(JOB_TRN_FD.CURRENCY_MST_FK, " + BaseCurrFk + ", round(TO_DATE(job_exp.JOBCARD_DATE,DATEFORMAT) - .5)), 4))TOTAL,");
                }
                else
                {
                    strSQL.Append("    job_trn_fd.exchange_rate AS ROE ,");
                    strSQL.Append("    (JOB_TRN_FD.FREIGHT_AMT*JOB_TRN_FD.EXCHANGE_RATE)TOTAL,");
                }

                strSQL.Append("    'false' as \"Delete\", job_trn_fd.PRINT_ON_MBL \"Print\" ,COMM.COMMODITY_MST_PK,JCT.job_trn_cont_pk");
                strSQL.Append("    ,CURR.CURRENCY_MST_PK CURR_FK,JCT.CHARGEABLE_WEIGHT, JCT.VOLUME_IN_CBM,DU.DIMENTION_ID,FREIGHT.CREDIT,");
                strSQL.Append("     freight.Preference");
                strSQL.Append("    FROM");
                strSQL.Append("    job_trn_fd job_trn_fd,job_trn_cont    JCT,");
                strSQL.Append("    container_type_mst_tbl cont,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    freight_element_mst_tbl freight,");
                strSQL.Append("    parameters_tbl prm,");
                strSQL.Append("    JOB_CARD_TRN job_exp,");
                strSQL.Append("    location_mst_tbl lmt,DIMENTION_UNIT_MST_TBL DU,");
                strSQL.Append("    customer_mst_tbl cmt,job_trn_cont CNT,COMMODITY_MST_TBL COMM");
                strSQL.Append("    WHERE");
                strSQL.Append("    job_trn_fd.job_card_trn_fk = job_exp.JOB_CARD_TRN_PK");
                strSQL.Append("    AND job_trn_fd.container_type_mst_fk = cont.container_type_mst_pk(+)");
                strSQL.Append("    AND DU.DIMENTION_UNIT_MST_PK(+)=JCT.BASIS_FK");
                strSQL.Append("    AND JOB_TRN_FD.job_trn_cont_fk = JCT.job_trn_cont_pk ");
                strSQL.Append("    AND CNT.job_trn_cont_pk =  JOB_TRN_FD.job_trn_cont_fk");
                strSQL.Append("    AND COMM.COMMODITY_MST_PK=CNT.COMMODITY_MST_FK");
                strSQL.Append("    AND job_trn_fd.Currency_Mst_Fk = curr.currency_mst_pk");
                strSQL.Append("    AND job_trn_fd.freight_element_mst_fk = freight.freight_element_mst_pk");
                strSQL.Append("    AND job_trn_fd.freight_element_mst_fk = prm.frt_bof_fk");
                strSQL.Append("   AND job_trn_fd.location_mst_fk = lmt.location_mst_pk (+)");
                strSQL.Append("   AND job_trn_fd.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+)");
                strSQL.Append("    AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK);
                strSQL.Append("    AND NVL(job_trn_fd.SERVICE_TYPE_FLAG,0)<>1");
                strSQL.Append(" union all ");
                strSQL.Append(" SELECT");
                strSQL.Append("    job_trn_fd_pk,\t");
                strSQL.Append("   COMM.COMMODITY_NAME container_type_mst_fk,");
                strSQL.Append("    freight.freight_element_id,");
                strSQL.Append("    freight.freight_element_name,");
                strSQL.Append("    freight.freight_element_mst_pk,\t");
                strSQL.Append("    job_trn_fd.basis,");
                strSQL.Append("    JCT.PACK_COUNT quantity,");
                strSQL.Append("    DECODE(job_trn_fd.freight_type,1,'Prepaid',2,'Collect') freight_type, ");
                strSQL.Append("    job_trn_fd.location_mst_fk, ");
                strSQL.Append("    lmt.location_id ,");
                strSQL.Append("    job_trn_fd.frtpayer_cust_mst_fk,");
                strSQL.Append("    cmt.customer_id, ");
                strSQL.Append("    CURR.CURRENCY_ID currency_mst_fk, ABS(JOB_TRN_FD.RATEPERBASIS)RATEPERBASIS,");
                strSQL.Append("   (ABS(CASE");
                strSQL.Append("              WHEN DU.DIMENTION_ID = 'W/M' THEN");
                strSQL.Append("                  CASE");
                strSQL.Append("                    WHEN JCT.VOLUME_IN_CBM > JCT.CHARGEABLE_WEIGHT THEN");
                strSQL.Append("                     ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.VOLUME_IN_CBM, 2)");
                strSQL.Append("                    WHEN JCT.CHARGEABLE_WEIGHT > JCT.VOLUME_IN_CBM THEN");
                strSQL.Append("                     ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.CHARGEABLE_WEIGHT, 2)");
                strSQL.Append("                    ELSE");
                strSQL.Append("                     ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.CHARGEABLE_WEIGHT, 2)");
                strSQL.Append("                  END");
                strSQL.Append("                 WHEN DU.DIMENTION_ID = 'CBM' THEN");
                strSQL.Append("                  ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.VOLUME_IN_CBM, 2)");
                strSQL.Append("                 WHEN DU.DIMENTION_ID = 'MT' THEN");
                strSQL.Append("                  ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.CHARGEABLE_WEIGHT, 2)");
                strSQL.Append("                 WHEN DU.DIMENTION_ID = 'UNIT' THEN");
                strSQL.Append("                  ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.PACK_COUNT, 2)");
                strSQL.Append("                 ELSE");
                strSQL.Append("                  ROUND(JOB_TRN_FD.RATEPERBASIS * JCT.PACK_COUNT, 2)");
                strSQL.Append("               END)*DECODE(FREIGHT.Credit,NULL,1,0,-1,1,1)) FREIGHT_AMT,");
                if (Convert.ToInt32(BaseCurrFk) != 0)
                {
                    strSQL.Append("       ROUND(GET_EX_RATE_BUY(JOB_TRN_FD.CURRENCY_MST_FK, " + BaseCurrFk + ", round(TO_DATE(job_exp.JOBCARD_DATE,DATEFORMAT) - .5)), 4) AS ROE,");
                    strSQL.Append("    (JOB_TRN_FD.FREIGHT_AMT*ROUND(GET_EX_RATE_BUY(JOB_TRN_FD.CURRENCY_MST_FK, " + BaseCurrFk + ", round(TO_DATE(job_exp.JOBCARD_DATE,DATEFORMAT) - .5)), 4))TOTAL,");
                }
                else
                {
                    strSQL.Append("    job_trn_fd.exchange_rate AS ROE ,");
                    strSQL.Append("    (JOB_TRN_FD.FREIGHT_AMT*JOB_TRN_FD.EXCHANGE_RATE)TOTAL,");
                }
                strSQL.Append("    'false' as \"Delete\", job_trn_fd.PRINT_ON_MBL \"Print\" ,COMM.COMMODITY_MST_PK, JCT.job_trn_cont_pk ");
                strSQL.Append("    ,CURR.CURRENCY_MST_PK CURR_FK,JCT.CHARGEABLE_WEIGHT, JCT.VOLUME_IN_CBM,DU.DIMENTION_ID,FREIGHT.CREDIT,");
                strSQL.Append("     freight.Preference");
                strSQL.Append("    FROM");
                strSQL.Append("    job_trn_fd job_trn_fd, job_trn_cont    JCT,");
                strSQL.Append("    container_type_mst_tbl cont,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    freight_element_mst_tbl freight,");
                strSQL.Append("    parameters_tbl prm,");
                strSQL.Append("    JOB_CARD_TRN job_exp,");
                strSQL.Append("    location_mst_tbl lmt,DIMENTION_UNIT_MST_TBL DU,");
                strSQL.Append("    customer_mst_tbl cmt,job_trn_cont CNT,COMMODITY_MST_TBL COMM");
                strSQL.Append("    WHERE");
                strSQL.Append("    job_trn_fd.job_card_trn_fk = job_exp.JOB_CARD_TRN_PK");
                strSQL.Append("    AND job_trn_fd.container_type_mst_fk = cont.container_type_mst_pk(+)");
                strSQL.Append("    AND JOB_TRN_FD.job_trn_cont_fk = JCT.job_trn_cont_pk ");
                strSQL.Append("    AND CNT.job_trn_cont_pk =  JOB_TRN_FD.job_trn_cont_fk");
                strSQL.Append("    AND COMM.COMMODITY_MST_PK=CNT.COMMODITY_MST_FK");
                strSQL.Append("    AND job_trn_fd.Currency_Mst_Fk = curr.currency_mst_pk");
                strSQL.Append("    AND job_trn_fd.freight_element_mst_fk = freight.freight_element_mst_pk");
                strSQL.Append("    AND job_trn_fd.freight_element_mst_fk not in  prm.frt_bof_fk");
                strSQL.Append("   AND job_trn_fd.location_mst_fk = lmt.location_mst_pk (+)");
                strSQL.Append("    AND DU.DIMENTION_UNIT_MST_PK=JCT.BASIS_FK");
                strSQL.Append("   AND job_trn_fd.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+)");
                strSQL.Append("    AND job_exp.JOB_CARD_TRN_PK =" + jobCardPK);
                strSQL.Append("    AND NVL(job_trn_fd.SERVICE_TYPE_FLAG,0)<>1");
                strSQL.Append("   ) QRY ");
                strSQL.Append("    ORDER BY Preference ");
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
        #endregion

        #region " Fetch Purchase Inventory data export"
        public DataSet FetchPurchaseInvDataExp(string jobCardPK = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    job_trn_sea_exp_pia_pk,");
                strSQL.Append("    vendor_key,\t");
                strSQL.Append("    cost_element_id,\t");
                strSQL.Append("    invoice_number,\t");
                strSQL.Append("    to_char(job_trn_pia.invoice_date, dateformat) invoice_date,");
                strSQL.Append("    currency_mst_fk,");
                strSQL.Append("    invoice_amt,\t");
                strSQL.Append("    tax_percentage,");
                strSQL.Append("    tax_amt,\t");
                strSQL.Append("    estimated_amt,");
                //strSQL.Append(vbCrLf & "    NVL(invoice_amt,0) - NVL(estimated_amt,0) diff_amt,")
                strSQL.Append("    invoice_amt - NVL(estimated_amt,0) diff_amt,");
                strSQL.Append("    vendor_mst_fk, ");
                strSQL.Append("    cost_element_mst_fk,");
                strSQL.Append("    job_trn_pia.attached_file_name,'false' as \"Delete\", MJC_TRN_SEA_EXP_PIA_FK ");

                strSQL.Append("FROM");
                strSQL.Append("    job_trn_sea_exp_pia  job_trn_pia,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    cost_element_mst_tbl cost_ele,");
                strSQL.Append("    job_card_sea_exp_tbl job_exp");
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_pia.job_card_sea_exp_fk = job_exp.job_card_sea_exp_pk");
                strSQL.Append("    AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk");
                strSQL.Append("    AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk");
                strSQL.Append("    AND job_exp.job_card_sea_exp_pk =" + jobCardPK);
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
        public DataSet FetchPIA(string jobCardPK = "0")
        {

            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    invoice_sea_tbl_fk,");
                strSQL.Append("    inv_agent_trn_sea_exp_fk,");
                strSQL.Append("    inv_supplier_fk");
                strSQL.Append("FROM");
                strSQL.Append("    job_trn_sea_exp_pia  job_trn_pia,");
                strSQL.Append("    currency_type_mst_tbl curr,");
                strSQL.Append("    cost_element_mst_tbl cost_ele,");
                strSQL.Append("    job_card_sea_exp_tbl job_exp");
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_pia.job_card_sea_exp_fk = job_exp.job_card_sea_exp_pk");
                strSQL.Append("    AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk");
                strSQL.Append("    AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk");
                strSQL.Append("    AND job_exp.job_card_sea_exp_pk =" + jobCardPK);
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

        #endregion

        #region " Fetch Cost details data export"
        public DataSet FetchCostDetailDataExp(string jobCardPK = "0", int basecurrency = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT JEC.JOB_TRN_SEA_EXP_COST_PK,");
                strSQL.Append("       JEC.JOB_CARD_SEA_EXP_FK,");
                strSQL.Append("       VMT.VENDOR_MST_PK,");
                strSQL.Append("       CMT.COST_ELEMENT_MST_PK,");
                strSQL.Append("       JEC.VENDOR_KEY,");
                strSQL.Append("       CMT.COST_ELEMENT_ID,");
                strSQL.Append("       CMT.COST_ELEMENT_NAME,");
                strSQL.Append("       DECODE(JEC.PTMT_TYPE,1,'Prepaid',2,'Collect')PTMT_TYPE,");
                strSQL.Append("       LMT.LOCATION_ID,");
                strSQL.Append("       CURR.CURRENCY_ID,");
                strSQL.Append("       JEC.ESTIMATED_COST,");
                strSQL.Append("       ROUND(GET_EX_RATE_BUY(JEC.CURRENCY_MST_FK, " + basecurrency + ", round(TO_DATE(JOB.JOBCARD_DATE,DATEFORMAT) - .5)), 4) AS ROE,");
                strSQL.Append("       JEC.TOTAL_COST,");
                strSQL.Append("       ''DEL_FLAG,");
                strSQL.Append("       'true'SEL_FLAG,");
                strSQL.Append("       JEC.LOCATION_MST_FK,");
                strSQL.Append("       JEC.CURRENCY_MST_FK");
                strSQL.Append("  FROM JOB_TRN_SEA_EXP_COST      JEC,");
                strSQL.Append("       JOB_CARD_SEA_EXP_TBL  JOB,");
                strSQL.Append("       VENDOR_MST_TBL        VMT,");
                strSQL.Append("       COST_ELEMENT_MST_TBL  CMT,");
                strSQL.Append("       CURRENCY_TYPE_MST_TBL CURR,");
                strSQL.Append("       LOCATION_MST_TBL      LMT");
                strSQL.Append(" WHERE JEC.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_SEA_EXP_PK");
                strSQL.Append("   AND JEC.COST_ELEMENT_MST_FK = CMT.COST_ELEMENT_MST_PK");
                strSQL.Append("   AND JEC.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
                strSQL.Append("   AND JEC.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                strSQL.Append("   AND JEC.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                strSQL.Append("   AND JOB.JOB_CARD_SEA_EXP_PK = " + jobCardPK);
                strSQL.Append("   AND NVL(JEC.SERVICE_TYPE_FLAG,0)<>1");
                strSQL.Append("   ORDER BY CMT.PREFERENCE");
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
        #endregion

        #region " Fetch TP data export"
        public DataSet FetchTPDataExp(string jobCardPK = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    job_trn_tp.job_trn_sea_exp_tp_pk,");
                strSQL.Append("    job_trn_tp.transhipment_no,");
                strSQL.Append("    job_trn_tp.port_mst_fk,");
                strSQL.Append("    port.port_id,");
                strSQL.Append("    port.port_name,");
                strSQL.Append("    job_trn_tp.vessel_name,");
                strSQL.Append("    job_trn_tp.voyage,");
                strSQL.Append("    TO_CHAR(job_trn_tp.eta_date,DATEFORMAT ) eta_date,");
                strSQL.Append("    TO_CHAR(job_trn_tp.etd_date,DATEFORMAT ) etd_date,");
                strSQL.Append("    agt.agent_id,");
                strSQL.Append("    agt.agent_name,");
                strSQL.Append("    'false' \"Delete\",");
                strSQL.Append("    job_trn_tp.voyage_trn_fk \"GridVoyagePK\", ");
                strSQL.Append("    job_trn_tp.agent_fk \"AgentPK\" ");
                strSQL.Append(" FROM");
                strSQL.Append("    job_trn_sea_exp_tp  job_trn_tp,");
                strSQL.Append("    port_mst_tbl port,");
                strSQL.Append("    agent_mst_tbl agt,");
                strSQL.Append("    location_mst_tbl lmt,");
                strSQL.Append("    vessel_voyage_trn vvt,");
                strSQL.Append("    job_card_sea_exp_tbl job_exp");
                strSQL.Append("WHERE");
                strSQL.Append("    job_trn_tp.job_card_sea_exp_fk = job_exp.job_card_sea_exp_pk");
                strSQL.Append("    AND job_trn_tp.port_mst_fk = port.port_mst_pk");
                strSQL.Append("    AND agt.location_mst_fk = lmt.location_mst_pk");
                strSQL.Append("    AND lmt.location_mst_pk = port.location_mst_fk");
                strSQL.Append("    AND JOB_TRN_TP.AGENT_FK = AGT.AGENT_MST_PK");
                strSQL.Append("    AND job_trn_tp.voyage_trn_fk = vvt.voyage_trn_pk(+)");
                strSQL.Append("    AND job_exp.job_card_sea_exp_pk =" + jobCardPK);
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
        #endregion

        #region "Calculate_TAX" ''Added by subhransu for tax calculation
        public object Calculate_TAX_Cost(string jobCardID = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append(" SELECT  NVL(SUM(JP.TAX_AMT), 0) AS COST_TAX");
                sb.Append("   FROM JOB_CARD_SEA_EXP_TBL   JC,");
                sb.Append("        JOB_TRN_SEA_EXP_PIA    JP");
                sb.Append("  WHERE ");
                sb.Append("     JC.JOB_CARD_SEA_EXP_PK = " + jobCardID + "");
                sb.Append("    AND JC.JOB_CARD_SEA_EXP_PK = JP.JOB_CARD_SEA_EXP_FK(+)");

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
        public object Calculate_TAX(string jobCardID = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {

                sb.Append("  SELECT NVL(SUM(CI.TAX_AMT), 0) AS REVENUE_TAX");
                sb.Append("  FROM JOB_CARD_SEA_EXP_TBL   JC,");
                sb.Append("   CONSOL_INVOICE_TRN_TBL CI");
                sb.Append("  WHERE JC.JOB_CARD_SEA_EXP_PK = CI.JOB_CARD_FK(+)");
                sb.Append("  AND JC.JOB_CARD_SEA_EXP_PK = " + jobCardID + "");
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
        #endregion

        #region "Fetch For Transhipment"
        public DataSet FetchAgentPK(string AgentPK = "0", decimal TEU = 0)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT '' JOB_TRN_SEA_EXP_PIA_PK,");
                sb.Append("       AMT.AGENT_ID VENDOR_KEY,");
                sb.Append("       CEM.COST_ELEMENT_ID COST_ELEMENT_ID,");
                sb.Append("       '' INVOICE_NUMBER,");
                sb.Append("       '' INVOICE_DATE,");
                sb.Append("       A.CURRENCY_TYPE_MST_FK CURRENCY_MST_FK,");
                sb.Append("       A.AMOUNT INVOICE_AMT,");
                sb.Append("       '' TAX_PERCENTAGE,");
                sb.Append("       '' TAX_AMT,");
                sb.Append("       NVL((A.AMOUNT * " + TEU + "), 0) ESTIMATED_AMT,");
                sb.Append("       NVL(A.AMOUNT - NVL((A.AMOUNT * " + TEU + "), 0), 0) DIFF_AMT,");
                sb.Append("       A.AGENT_MST_FK VENDOR_MST_FK,");
                sb.Append("       A.COST_ELEMENT_MST_FK COST_ELEMENT_MST_FK,");
                sb.Append("       '' ATTACHED_FILE_NAME,");
                sb.Append("       '0' AS \"Delete\",");
                sb.Append("       '' MJC_TRN_SEA_EXP_PIA_FK,");
                sb.Append("       '' UpdFlag");
                sb.Append("  FROM AGENT_TRANSHIP_TRN A, AGENT_MST_TBL AMT, COST_ELEMENT_MST_TBL CEM");
                sb.Append(" WHERE A.AGENT_MST_FK = " + AgentPK);
                sb.Append("   AND AMT.AGENT_MST_PK = A.AGENT_MST_FK");
                sb.Append("   AND A.COST_ELEMENT_MST_FK = CEM.COST_ELEMENT_MST_PK");

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

        #endregion

        #region "GetRevenueDetails"

        public DataSet GetRevenueDetails(decimal actualCost, decimal actualRevenue, decimal estimatedCost, decimal estimatedRevenue, string jobCardPK, int LocationPK = 0)
        {

            //Dim SQL As New System.Text.StringBuilder
            WorkFlow objWF = new WorkFlow();
            //Snigdharani - 10/11/2008 - making the values same as consolidation screen.
            try
            {
                DataSet DS = new DataSet();
                var _with50 = objWF.MyCommand.Parameters;
                _with50.Add("JCPK", jobCardPK).Direction = ParameterDirection.Input;
                _with50.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with50.Add("JOB_EXP_SEA", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_COST_REVENUE_PROFIT", "FETCH_JOBCARD_EXP_SEA");
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
        #endregion

        #region "Get Invoice PK"
        //0 - means no invoice exists.
        //-1 - means more then one invoice extists
        //pk value of invoice

        //invice Type: 1-Invoice to customer.
        //invice Type: 2-Invoice to CB Agent.
        //invice Type: 3-Invoice to DP Agent.
        public long GetCustInvoice(string jobCardPK, Int16 invoiceType = 1)
        {

            System.Text.StringBuilder SQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            OracleDataReader oraReader = null;
            int invoiceCount = 0;
            long invoicePK = 0;

            if (invoiceType == 1)
            {
                //SQL.Append(vbCrLf & "select i.inv_cust_sea_exp_pk from inv_cust_sea_exp_tbl i where i.job_card_sea_exp_fk = " & jobCardPK)
                SQL.Append("SELECT CON.CONSOL_INVOICE_PK");
                SQL.Append("  FROM CONSOL_INVOICE_TBL CON, CONSOL_INVOICE_TRN_TBL CONTRN");
                SQL.Append(" WHERE CON.CONSOL_INVOICE_PK = CONTRN.CONSOL_INVOICE_FK");
                SQL.Append("  AND CON.BUSINESS_TYPE = 2");
                SQL.Append("  AND CON.PROCESS_TYPE =1");
                SQL.Append("   AND CONTRN.JOB_CARD_FK = " + jobCardPK);
            }
            else if (invoiceType == 2)
            {
                SQL.Append("select i.inv_agent_sea_exp_pk from inv_agent_sea_exp_tbl i where i.cb_or_dp_agent=1 AND  i.job_card_sea_exp_fk in (" + jobCardPK + ")");
            }
            else if (invoiceType == 3)
            {
                SQL.Append("select i.inv_agent_sea_exp_pk from inv_agent_sea_exp_tbl i where  i.cb_or_dp_agent=2 AND i.job_card_sea_exp_fk = " + jobCardPK);
            }


            oraReader = objWF.GetDataReader(SQL.ToString());

            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], DBNull.Value)))
                {
                    invoicePK = Convert.ToInt64(oraReader[0].ToString());
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

        public void UpdatePlacesRefDel(string SQLQuery)
        {
            WorkFlow objWF = new WorkFlow();
            objWF.ExecuteScaler(SQLQuery);
        }
        #endregion

        #region "Fill Combo"
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
            strSQL.Append(" ORDER BY cargo_move_code");

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
            strSQL.Append(" ORDER BY inco_code");

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


        #endregion

        #region "Fetch Revenue data export"
        public DataSet FetchRevenueData(string jobCardPK = "0")
        {
            //Dim strSQL As StringBuilder = New StringBuilder
            WorkFlow objWF = new WorkFlow();

            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with51 = objWF.MyCommand.Parameters;
                _with51.Add("JOBCARD_PK", jobCardPK).Direction = ParameterDirection.Input;
                _with51.Add("JOB_SEA_EXP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_SEA_EXP");
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
        #endregion

        #region "GenerateUCRNumber"
        public string GenerateUCRNumber(string customerID)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strSQL.Append("SELECT (SELECT TO_CHAR(SYSDATE,'yy')  FROM dual)||country.country_id||cmt.vat_no");
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

        public DataSet FillContainerTypeDataSet(bool isBooking, string pk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            if (isBooking)
            {
                strSQL.Append(" SELECT  distinct");
                strSQL.Append(" cont.container_type_mst_pk,");
                strSQL.Append(" cont.container_type_mst_id");
                strSQL.Append(" FROM");
                strSQL.Append(" booking_sea_tbl book,");
                strSQL.Append(" booking_trn_sea_fcl_lcl booking_trn,");
                strSQL.Append(" container_type_mst_tbl cont");
                strSQL.Append(" WHERE");
                strSQL.Append(" booking_trn.booking_sea_fk = " + pk);
                strSQL.Append(" AND booking_trn.container_type_mst_fk = cont.container_type_mst_pk");
                strSQL.Append(" AND book.booking_sea_pk = booking_trn.booking_sea_fk");
                strSQL.Append(" ORDER BY container_type_mst_id");
            }
            else
            {
                strSQL.Append(" SELECT distinct");
                strSQL.Append(" cont.container_type_mst_pk,");
                strSQL.Append(" cont.container_type_mst_id");
                strSQL.Append(" FROM");
                strSQL.Append(" job_trn_sea_exp_cont job_trn,");
                strSQL.Append(" container_type_mst_tbl cont");
                strSQL.Append(" WHERE");
                strSQL.Append(" job_trn.container_type_mst_fk = cont.container_type_mst_pk");
                strSQL.Append(" and job_trn.job_card_sea_exp_fk =" + pk);
                strSQL.Append(" ORDER BY cont.container_type_mst_id");
            }


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

        public DataSet FillBookingOtherChargesDataSet(string pk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append("SELECT");
            strSQL.Append("         '' JOB_TRN_SEA_EXP_OTH_PK,");
            strSQL.Append("         frt.freight_element_mst_pk,");
            strSQL.Append("         frt.freight_element_id,");
            strSQL.Append("         frt.freight_element_name,");
            strSQL.Append("         curr.CURRENCY_ID CURRENCY,");
            strSQL.Append("         curr.currency_mst_pk, '' \"ROE\",");
            strSQL.Append("         oth_chrg.amount amount,");
            strSQL.Append("         'false' \"Delete\", 1 \"Print\" ");
            strSQL.Append("FROM");
            strSQL.Append("         booking_trn_sea_oth_chrg oth_chrg,");
            strSQL.Append("         booking_sea_tbl  booking_mst,");
            strSQL.Append("         freight_element_mst_tbl frt,");
            strSQL.Append("         currency_type_mst_tbl curr");
            strSQL.Append("WHERE");
            strSQL.Append("         oth_chrg.booking_sea_fk = booking_mst.booking_sea_pk");
            strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.booking_sea_fk         = " + pk);
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

        public DataSet FillJobCardOtherChargesDataSet(string pk = "0", Int64 baseCurrency = 1, Int16 CheckBkgJC = 0, string BKGPK = "")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();


            if (CheckBkgJC == 0)
            {
                strSQL.Append("         SELECT");
                strSQL.Append("         oth_chrg.job_trn_sea_exp_oth_pk,");
                strSQL.Append("         frt.freight_element_mst_pk,");
                strSQL.Append("         frt.freight_element_id,");
                strSQL.Append("         frt.freight_element_name,");
                strSQL.Append("         DECODE(oth_chrg.freight_type,1,'Prepaid',2,'Collect') Payment_Type, ");
                // By Amit on 26-April-2007
                //To introduced LOCATION & FREIGHT PAYER Column 
                //By Amit Singh on 23-May-07
                strSQL.Append("         oth_chrg.location_mst_fk,");
                //strSQL.Append(vbCrLf & "         (CASE WHEN  oth_chrg.freight_type=1 THEN lmt.location_id ELSE pmt.port_id END) ""location_id"",")
                strSQL.Append("         lmt.location_id ,");
                strSQL.Append("         oth_chrg.frtpayer_cust_mst_fk,");
                strSQL.Append("         cmt.customer_id,");
                //End
                strSQL.Append("         CURR.CURRENCY_ID currency_mst_pk, ");
                //strSQL.Append(vbCrLf & "        ROUND(GET_EX_RATE(oth_chrg.currency_mst_fk," & Session("currency_mst_pk") & ",round(sysdate - .5)),4) AS ROE ,") 'Code added by gopi for converting ROE into BaseCurrency Ref No:EQA 2045
                strSQL.Append("         oth_chrg.exchange_rate ROE, ");
                //adding by thiyagarajan on 12/12/08 
                strSQL.Append("         oth_chrg.amount amount, ROUND((OTH_CHRG.AMOUNT*OTH_CHRG.EXCHANGE_RATE),2)TOTAL,");
                strSQL.Append("         'false' \"Delete\", oth_chrg.PRINT_ON_MBL \"Print\" , CURR.CURRENCY_MST_PK CURRFK");
                strSQL.Append("FROM");
                strSQL.Append("         job_trn_sea_exp_oth_chrg oth_chrg,");
                strSQL.Append("         job_card_sea_exp_tbl jobcard_mst,");
                strSQL.Append("         freight_element_mst_tbl frt,");
                strSQL.Append("         currency_type_mst_tbl curr,");
                //To introduced LOCATION & FREIGHT PAYER Column 
                //By Amit Singh on 23-May-07
                strSQL.Append("         location_mst_tbl lmt,");
                //strSQL.Append(vbCrLf & "         port_mst_tbl pmt,")
                strSQL.Append("         customer_mst_tbl cmt");
                //End
                strSQL.Append("WHERE");
                strSQL.Append("         oth_chrg.job_card_sea_exp_fk = jobcard_mst.job_card_sea_exp_pk");
                strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
                strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
                //To introduced LOCATION & FREIGHT PAYER Column 
                //By Amit Singh on 23-May-07
                strSQL.Append("         AND oth_chrg.location_mst_fk = lmt.location_mst_pk (+)");
                //strSQL.Append(vbCrLf & "         AND oth_chrg.location_mst_fk = pmt.port_mst_pk (+)")
                strSQL.Append("         AND oth_chrg.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+)");
                //End
                strSQL.Append("         AND oth_chrg.job_card_sea_exp_fk    = " + pk);
                strSQL.Append("ORDER BY freight_element_id ");


            }
            else
            {
                strSQL.Append("         SELECT");
                strSQL.Append("         '' JOB_TRN_SEA_EXP_OTH_PK,");
                strSQL.Append("         frt.freight_element_mst_pk,");
                strSQL.Append("         frt.freight_element_id,");
                strSQL.Append("         frt.freight_element_name,");
                strSQL.Append("         DECODE(oth_chrg.freight_type,1,'Prepaid',2,'Collect') Payment_Type, ");
                // By Amit on 26-April-2007
                //To introduced LOCATION & FREIGHT PAYER Column 
                //By Amit Singh on 23-May-07
                strSQL.Append("         '' location_mst_fk,");
                //strSQL.Append(vbCrLf & "         (CASE WHEN  oth_chrg.freight_type=1 THEN lmt.location_id ELSE pmt.port_id END) ""location_id"",")
                strSQL.Append("         '' location_id ,");
                strSQL.Append("         booking_mst.cust_customer_mst_fk frtpayer_cust_mst_fk,");
                strSQL.Append("         cmt.customer_id,");
                //End
                //strSQL.Append(vbCrLf & "         curr.currency_mst_pk, ")
                strSQL.Append("         CURR.CURRENCY_ID currency_mst_pk, ");
                strSQL.Append("    ROUND(GET_EX_RATE(oth_chrg.currency_mst_fk," + baseCurrency + ",round(sysdate - .5)),4) AS ROE ,");
                //Code added by gopi for converting ROE into BaseCurrency Ref No:EQA 2045
                strSQL.Append("         oth_chrg.amount amount,0 TOTAL,");
                strSQL.Append("         'false' \"Delete\", 1 \"Print\" ,CURR.CURRENCY_MST_PK CURRFK ");
                strSQL.Append("FROM");
                strSQL.Append("         booking_trn_sea_oth_chrg oth_chrg,");
                strSQL.Append("         booking_sea_tbl booking_mst,");
                strSQL.Append("         freight_element_mst_tbl frt,");
                strSQL.Append("         currency_type_mst_tbl curr,");
                strSQL.Append("         customer_mst_tbl cmt");
                strSQL.Append("WHERE");
                strSQL.Append("         oth_chrg.booking_sea_fk = booking_mst.booking_sea_pk");
                strSQL.Append("         and booking_mst.cust_customer_mst_fk=cmt.customer_mst_pk");
                strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
                strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");

                strSQL.Append("         AND oth_chrg.booking_sea_fk    = " + BKGPK);
                strSQL.Append("ORDER BY freight_element_id ");

            }
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
        public DataSet fillJcOthChrg(string pk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append("         SELECT");
            strSQL.Append("         oth_chrg.inv_cust_trn_sea_exp_fk,");
            strSQL.Append("         oth_chrg.inv_agent_trn_sea_exp_fk,");
            strSQL.Append("         oth_chrg.consol_invoice_trn_fk");
            strSQL.Append("FROM");
            strSQL.Append("         job_trn_sea_exp_oth_chrg oth_chrg,");
            strSQL.Append("         job_card_sea_exp_tbl jobcard_mst,");
            strSQL.Append("         freight_element_mst_tbl frt,");
            strSQL.Append("         currency_type_mst_tbl curr");
            strSQL.Append("WHERE");
            strSQL.Append("         oth_chrg.job_card_sea_exp_fk = jobcard_mst.job_card_sea_exp_pk");
            strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.job_card_sea_exp_fk    = " + pk);
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
        //added by surya prasad for implementing multiple commodity task
        public string FetchBookingPk(string Jobpk)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSQL.Append(" select bkg.booking_sea_pk from ");
            strSQL.Append(" booking_sea_tbl bkg, ");
            strSQL.Append(" job_card_sea_exp_tbl  jsea ");
            strSQL.Append(" where jsea.booking_sea_fk = bkg.booking_sea_pk ");
            strSQL.Append(" and jsea.job_card_sea_exp_pk = " + Jobpk + " ");
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
        //end
        #endregion

        #region "Certificate of Insurance -- Exports Sea"
        public DataSet FetchCISExpSea(Int32 JobPk)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();

            Strsql = "  SELECT JS.JOB_CARD_SEA_EXP_PK AS JOBPK,";
            Strsql += " JS.JOBCARD_REF_NO AS JOBREFNO,";
            Strsql += " JS.VESSEL_NAME AS CONVEYANCE,";
            Strsql += " COLL.PLACE_NAME AS FRM,";
            Strsql += " DEL.PLACE_NAME AS VIATO,";
            Strsql += " NVL(JS.INSURANCE_AMT,0) AS INSUREDVALUE, ";
            Strsql += " C.CURRENCY_NAME,";
            Strsql += " H.MARKS_NUMBERS,";
            Strsql += " H.GOODS_DESCRIPTION AS INTEREST ,";
            Strsql += " SHP.CUSTOMER_NAME AS SHIPPER";
            Strsql += " FROM JOB_CARD_SEA_EXP_TBL JS,";
            Strsql += " BOOKING_SEA_TBL BS,";
            Strsql += " HBL_EXP_TBL H,";
            Strsql += " PLACE_MST_TBL COLL,";
            Strsql += " PLACE_MST_TBL DEL,";
            Strsql += " CURRENCY_TYPE_MST_TBL C,";
            Strsql += " CUSTOMER_MST_TBL SHP";
            Strsql += " WHERE JS.BOOKING_SEA_FK = BS.BOOKING_SEA_PK";
            Strsql += " AND H.JOB_CARD_SEA_EXP_FK(+)=JS.JOB_CARD_SEA_EXP_PK";
            Strsql += " AND COLL.PLACE_PK(+)=BS.COL_PLACE_MST_FK";
            Strsql += " AND DEL.PLACE_PK(+)=BS.DEL_PLACE_MST_FK";
            Strsql += " AND C.CURRENCY_MST_PK(+)=JS.INSURANCE_CURRENCY";
            Strsql += " AND SHP.CUSTOMER_MST_PK(+)=JS.SHIPPER_CUST_MST_FK";
            Strsql += " AND JS.JOB_CARD_SEA_EXP_PK=" + JobPk;

            try
            {
                return ObjWk.GetDataSet(Strsql);
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
        #endregion

        #region " Fetch base currency Exchange rate export"
        public DataSet FetchROE(Int64 baseCurrency, string ROE_Date = "")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");
                strSQL.Append(" CURR.CURRENCY_MST_PK,");
                strSQL.Append(" CURR.CURRENCY_ID,");
                if (string.IsNullOrEmpty(ROE_Date))
                {
                    strSQL.Append(" ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK," + baseCurrency + ",round(sysdate - .5)),6) AS ROE ");
                }
                else
                {
                    strSQL.Append(" ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK," + baseCurrency + ",round(TO_DATE(" + ROE_Date + ",DATEFORMAT) - .5)),6) AS ROE ");
                }
                strSQL.Append(" FROM CURRENCY_TYPE_MST_TBL CURR ");
                strSQL.Append(" WHERE CURR.ACTIVE_FLAG = 1 ");
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
        public DataSet FetchROE_BUY(Int64 baseCurrency, string ROE_Date = "")
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");
                strSQL.Append(" CURR.CURRENCY_MST_PK,");
                strSQL.Append(" CURR.CURRENCY_ID,");
                if (string.IsNullOrEmpty(ROE_Date))
                {
                    strSQL.Append("  ROUND(GET_EX_RATE_BUY(CURR.CURRENCY_MST_PK," + baseCurrency + ",round(sysdate - .5)),6) AS ROE");
                }
                else
                {
                    strSQL.Append("  ROUND(GET_EX_RATE_BUY(CURR.CURRENCY_MST_PK," + baseCurrency + ",round(TO_DATE(" + ROE_Date + ",DATEFORMAT) - .5)),6) AS ROE");
                }
                strSQL.Append(" FROM CURRENCY_TYPE_MST_TBL CURR");
                strSQL.Append("WHERE CURR.ACTIVE_FLAG = 1");
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
        #endregion

        #region " Fetch base currency Exchange rate export"
        public DataSet FetchVesselVoyageROE(Int64 voyage)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                //modified by thiyagarajan on 2/12/08 for location based currency task
                strSQL = " SELECT C.CURRENCY_MST_PK," + " 1  ROE" + " FROM CURRENCY_TYPE_MST_TBL C" + " WHERE C.CURRENCY_MST_PK =" + HttpContext.Current.Session["currency_mst_pk"] + " AND C.ACTIVE_FLAG = 1" + " UNION" + " SELECT CURR.CURRENCY_MST_PK," + " EXCHANGE_RATE ROE" + " FROM CURRENCY_TYPE_MST_TBL CURR, Exchange_Rate_Trn EXC" + " WHERE EXC.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK" + " AND CURR.ACTIVE_FLAG = 1" + " AND EXC.Voyage_Trn_Fk is not null" + " AND exc.voyage_trn_fk = " + voyage + " AND EXC.CURRENCY_MST_BASE_FK =" + HttpContext.Current.Session["currency_mst_pk"] + " AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK" + " UNION" + " SELECT T.CURRENCY_MST_PK," + " TO_NUMBER(NULL) ROE" + " FROM CURRENCY_TYPE_MST_TBL T " + " WHERE T.CURRENCY_MST_PK NOT IN" + " ( SELECT CURRENCY_MST_PK" + " FROM CURRENCY_TYPE_MST_TBL , Exchange_Rate_Trn " + " WHERE CURRENCY_MST_FK = CURRENCY_MST_PK" + " AND ACTIVE_FLAG = 1" + " AND voyage_trn_fk = " + voyage + " AND CURRENCY_MST_BASE_FK = " + HttpContext.Current.Session["currency_mst_pk"] + " AND CURRENCY_MST_BASE_FK <> CURRENCY_MST_FK )" + " AND T.CURRENCY_MST_PK <> " + HttpContext.Current.Session["currency_mst_pk"];
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
        #endregion

        #region "Fetch Freight Type"
        public DataSet FetchFrtType(Int64 baseFrt = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT");
                strSQL.Append("    STMT.SHIPPING_TERMS_MST_PK, STMT.FREIGHT_TYPE");
                strSQL.Append("FROM");
                strSQL.Append("    SHIPPING_TERMS_MST_TBL STMT");

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
        #endregion

        #region " Fetch Data for Standard Shipping Note"
        public DataSet FetchSSN(string JOBPK)
        {
            string Strsql = null;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = " SELECT ";
                Strsql += " JAE.JOB_CARD_SEA_EXP_PK JOBPK,                     ";
                Strsql += " JAE.JOBCARD_REF_NO JOBREFNO,                       ";
                Strsql += " BAT.BOOKING_SEA_PK BKGPK,                          ";
                Strsql += " BAT.BOOKING_REF_NO BKGREFNO,                       ";
                Strsql += " BAT.BOOKING_DATE BKGDATE, ";
                Strsql += " JAE.SHIPPER_CUST_MST_FK EXPORTERPK,";
                Strsql += " CUSTOMER.CUSTOMER_NAME EXPORTERNAME,";
                Strsql += " CUSTDTLS.ADM_ADDRESS_1 EXPORTERADD1,";
                Strsql += " CUSTDTLS.ADM_ADDRESS_2 EXPORTERADD2,";
                Strsql += " CUSTDTLS.ADM_ADDRESS_3 EXPORTERADD3,";
                Strsql += " CUSTDTLS.ADM_CITY EXPORTERCITY,";
                Strsql += " CUSTDTLS.ADM_LOCATION_MST_FK EXPORTERLOCFK,";
                Strsql += " EXPORTERLOC.LOCATION_NAME EXPORTERLOCNAME,";
                Strsql += " CUSTDTLS.ADM_COUNTRY_MST_FK EXPORTERCOUNTRYFK,";
                Strsql += " EXPORTERCOUNTRY.COUNTRY_NAME EXPORTERCOUNTRYNAME,";
                Strsql += " BAT.CUSTOMER_REF_NO EXPORTERSREF,";
                Strsql += " CUSTDTLS.ADM_ZIP_CODE EXPORTERZIP,";
                Strsql += " CUSTDTLS.ADM_PHONE_NO_1 EXPORTERPHONE1,";
                Strsql += " CUSTDTLS.ADM_PHONE_NO_2 EXPORTERPHONE2,";
                Strsql += " CUSTDTLS.ADM_FAX_NO     EXPORTERFAX,";
                Strsql += " CUSTDTLS.ADM_EMAIL_ID   EXPORTEREMAIL,";
                Strsql += " CUSTDTLS.ADM_URL        EXPORTERURL,";
                Strsql += " ' ' CUSTOMSFK,                 ";
                Strsql += " JAE.UCR_NO  CUSTOMSCODE,       ";
                Strsql += " BAT.CUSTOMER_REF_NO EXPORTERREF,                                   ";
                Strsql += " CORP.CORPORATE_NAME CORPNAME,                      ";
                Strsql += " CORP.ADDRESS_LINE1 CORPADD1,                       ";
                Strsql += " CORP.ADDRESS_LINE2 CORPADD2,                       ";
                Strsql += " CORP.ADDRESS_LINE3 CORPADD3,                       ";
                Strsql += " CORP.CITY          CORPCITY,  ";
                Strsql += " CORP.STATE_MST_FK CORPSTATEFK,";
                Strsql += " STATE.STATE_NAME CORPSTATENAME,                         ";
                Strsql += " CORP.COUNTRY_MST_FK CORPCOUNTRYFK,";
                Strsql += " COUNTRY.COUNTRY_NAME CORPCOUNTRY,                  ";
                Strsql += " CORP.POST_CODE       CORPZIP, ";
                Strsql += " CORP.PHONE           CORPPHONE,";
                Strsql += " CORP.FAX             CORPFAX,";
                Strsql += " CORP.EMAIL           CORPEMAIL,";
                Strsql += " CORP.HOME_PAGE       CORPURL,                     ";
                Strsql += " BAT.OPERATOR_MST_FK INTLCARRFK,                     ";
                Strsql += " OPERAT.OPERATOR_NAME INTLCARRNAME,                 ";
                Strsql += " ' ' OTHUKTRANS,                                    ";
                Strsql += " (JAE.VESSEL_NAME || '/' || JAE.VOYAGE) VSL_OR_FLIGHT_NO,                    ";
                Strsql += " TO_CHAR(JAE.ETD_DATE,'" + dateFormat + "') VSL_OR_FLIGHT_DATE ,                           ";
                Strsql += " BAT.PORT_MST_POL_FK PORTOFLANDFK ,                 ";
                Strsql += " PORTOFLANDING.PORT_NAME PORTOFLANDNAME,            ";
                Strsql += " BAT.PORT_MST_POD_FK PORTOFDISCHFK,                 ";
                Strsql += " PORTOFDISCHARGE.PORT_NAME PORTOFDISCHNAME,         ";
                Strsql += " BAT.DEL_PLACE_MST_FK DELPLACEFK,                   ";
                Strsql += " PLD.PLACE_NAME DELPLACENAME,                       ";
                Strsql += " (CASE                                              ";
                Strsql += " WHEN JAE.MARKS_NUMBERS IS NOT NULL THEN          ";
                Strsql += " jAE.MARKS_NUMBERS";
                Strsql += " ELSE                                               ";
                Strsql += " JAE.MARKS_NUMBERS";
                Strsql += " END) MARKS,                                        ";
                Strsql += " (CASE                                              ";
                Strsql += " WHEN JAE.GOODS_DESCRIPTION IS NOT NULL THEN          ";
                Strsql += " JAE.GOODS_DESCRIPTION";
                Strsql += " ELSE                                               ";
                Strsql += " JAE.GOODS_DESCRIPTION";
                Strsql += " END) GOODS,                                        ";
                Strsql += " SUM(JTAEC.GROSS_WEIGHT) GROSSWT,                   ";
                Strsql += " SUM(JTAEC.VOLUME_IN_CBM) VOL_IN_CBM,               ";
                Strsql += " SUM(JTAEC.GROSS_WEIGHT) TOTGROSSWT,                ";
                Strsql += " SUM(JTAEC.VOLUME_IN_CBM) TOT_VOL_IN_CBM,";
                Strsql += " JTAEC.CONTAINER_NUMBER CONTAINERNO,                    ";
                Strsql += " JTAEC.SEAL_NUMBER SEALNO,";
                Strsql += " COUNT(CONTAINER.CONTAINER_TYPE_MST_ID) || ' X ' || CONTAINER.CONTAINER_TYPE_MST_ID CONTAINERSIZE, ";
                Strsql += " CONTAINER.CONTAINER_TAREWEIGHT_TONE TAREWT,                                        ";
                Strsql += " JTAEC.PACK_COUNT TOTOFBOXES,                       ";
                Strsql += " CORP.CORPORATE_NAME NAMEOFCOMP,                    ";
                Strsql += " TO_CHAR(SYSDATE,'" + dateFormat + "') TODAYSDATE ,         ";
                Strsql += " JAE.TRANSPORTER_CARRIER_FK HAULIERFK,              ";
                Strsql += " TRANSPORTER.VENDOR_NAME HAULIERNAME,               ";
                Strsql += " ' ' VEHICLEREGNO                                   ";
                Strsql += " FROM JOB_CARD_SEA_EXP_TBL    JAE,                  ";
                Strsql += " JOB_TRN_SEA_EXP_CONT    JTAEC,                     ";
                Strsql += " BOOKING_SEA_TBL         BAT,                       ";
                Strsql += " PLACE_MST_TBL           PLD,                       ";
                Strsql += " COMMODITY_GROUP_MST_TBL CGMST,                     ";
                Strsql += " CORPORATE_MST_TBL       CORP,                      ";
                Strsql += " COUNTRY_MST_TBL         COUNTRY,                   ";
                Strsql += " PORT_MST_TBL            PORTOFLANDING,             ";
                Strsql += " PORT_MST_TBL            PORTOFDISCHARGE,           ";
                Strsql += " VENDOR_MST_TBL TRANSPORTER,";
                Strsql += " STATE_MST_TBL STATE,";
                Strsql += " CUSTOMER_MST_TBL CUSTOMER,";
                Strsql += " CUSTOMER_CONTACT_DTLS CUSTDTLS,";
                Strsql += " LOCATION_MST_TBL EXPORTERLOC,";
                Strsql += " COUNTRY_MST_TBL EXPORTERCOUNTRY,";
                Strsql += " OPERATOR_MST_TBL OPERAT,";
                Strsql += " CONTAINER_TYPE_MST_TBL CONTAINER";
                Strsql += " WHERE JAE.JOB_CARD_SEA_EXP_PK IN(" + JOBPK + " )   ";
                Strsql += " AND JTAEC.JOB_CARD_SEA_EXP_FK(+) = JAE.JOB_CARD_SEA_EXP_PK";
                Strsql += " AND JAE.BOOKING_SEA_FK = BAT.BOOKING_SEA_PK(+)";
                Strsql += " AND BAT.OPERATOR_MST_FK = OPERAT.OPERATOR_MST_PK(+)";
                Strsql += " AND JAE.COMMODITY_GROUP_FK = CGMST.COMMODITY_GROUP_PK(+)";
                Strsql += " AND CORP.COUNTRY_MST_FK     = COUNTRY.COUNTRY_MST_PK(+)";
                Strsql += " AND BAT.PORT_MST_POL_FK     = PORTOFLANDING.PORT_MST_PK(+)";
                Strsql += " AND BAT.PORT_MST_POD_FK     = PORTOFDISCHARGE.PORT_MST_PK(+)";
                Strsql += " AND BAT.DEL_PLACE_MST_FK    = PLD.PLACE_PK(+)";
                Strsql += " AND JAE.TRANSPORTER_CARRIER_FK = TRANSPORTER.VENDOR_MST_PK(+)";
                Strsql += " AND CORP.STATE_MST_FK          = STATE.STATE_MST_PK(+)";
                Strsql += " AND JAE.SHIPPER_CUST_MST_FK    = CUSTOMER.CUSTOMER_MST_PK";
                Strsql += " AND CUSTDTLS.CUSTOMER_MST_FK   = CUSTOMER.CUSTOMER_MST_PK(+)";
                Strsql += " AND CUSTDTLS.ADM_LOCATION_MST_FK = EXPORTERLOC.LOCATION_MST_PK(+)";
                Strsql += " AND CUSTDTLS.ADM_COUNTRY_MST_FK  = EXPORTERCOUNTRY.COUNTRY_MST_PK(+)";
                Strsql += " AND JTAEC.CONTAINER_TYPE_MST_FK  = CONTAINER.CONTAINER_TYPE_MST_PK(+)";
                Strsql += " and CGMST.COMMODITY_GROUP_CODE NOT LIKE 'DGS'   ";
                Strsql += " GROUP BY JAE.JOB_CARD_SEA_EXP_PK,               ";
                Strsql += " JAE.JOBCARD_REF_NO,                             ";
                Strsql += " BAT.BOOKING_SEA_PK,                             ";
                Strsql += " BAT.BOOKING_REF_NO,                             ";
                Strsql += " BAT.BOOKING_DATE,  ";
                Strsql += " JAE.SHIPPER_CUST_MST_FK ,      ";
                Strsql += " CUSTOMER.CUSTOMER_NAME ,       ";
                Strsql += " CUSTDTLS.ADM_ADDRESS_1 ,       ";
                Strsql += " CUSTDTLS.ADM_ADDRESS_2 ,       ";
                Strsql += " CUSTDTLS.ADM_ADDRESS_3 ,       ";
                Strsql += " CUSTDTLS.ADM_CITY ,            ";
                Strsql += " JAE.UCR_NO,                    ";
                Strsql += " CUSTDTLS.ADM_LOCATION_MST_FK , ";
                Strsql += " EXPORTERLOC.LOCATION_NAME ,    ";
                Strsql += " CUSTDTLS.ADM_COUNTRY_MST_FK ,  ";
                Strsql += " EXPORTERCOUNTRY.COUNTRY_NAME , ";
                Strsql += " BAT.CUSTOMER_REF_NO,          ";
                Strsql += " CUSTDTLS.ADM_ZIP_CODE ,    ";
                Strsql += " CUSTDTLS.ADM_PHONE_NO_1 ,  ";
                Strsql += " CUSTDTLS.ADM_PHONE_NO_2 ,  ";
                Strsql += " CUSTDTLS.ADM_FAX_NO     ,  ";
                Strsql += " CUSTDTLS.ADM_EMAIL_ID   ,  ";
                Strsql += " CUSTDTLS.ADM_URL,                              ";
                Strsql += " ' ',                                           ";
                Strsql += " ' ' ,                                          ";
                Strsql += " ' ' ,                                          ";
                Strsql += " CORP.CORPORATE_NAME ,                          ";
                Strsql += " CORP.ADDRESS_LINE1 ,                           ";
                Strsql += " CORP.ADDRESS_LINE2 ,                           ";
                Strsql += " CORP.ADDRESS_LINE3 ,                           ";
                Strsql += " CORP.CITY          ,                           ";
                Strsql += " CORP.STATE_MST_FK ,                            ";
                Strsql += " STATE.STATE_NAME ,                             ";
                Strsql += " CORP.COUNTRY_MST_FK ,                          ";
                Strsql += " COUNTRY.COUNTRY_NAME ,                         ";
                Strsql += " CORP.POST_CODE       ,                         ";
                Strsql += " CORP.PHONE           ,                         ";
                Strsql += " CORP.FAX             ,                         ";
                Strsql += " CORP.EMAIL           ,                         ";
                Strsql += " CORP.HOME_PAGE       ,                         ";
                Strsql += " BAT.OPERATOR_MST_FK ,                          ";
                Strsql += " OPERAT.OPERATOR_NAME ,                         ";
                Strsql += " ' ',                                           ";
                Strsql += " (JAE.VESSEL_NAME || '/'|| JAE.VOYAGE) ,        ";
                Strsql += " TO_CHAR(JAE.ETD_DATE,'" + dateFormat + "'),            ";
                Strsql += " BAT.PORT_MST_POL_FK,                           ";
                Strsql += " PORTOFLANDING.PORT_NAME,                       ";
                Strsql += " BAT.PORT_MST_POD_FK ,                          ";
                Strsql += " PORTOFDISCHARGE.PORT_NAME,                     ";
                Strsql += " BAT.DEL_PLACE_MST_FK,                          ";
                Strsql += " PLD.PLACE_NAME,                                ";
                Strsql += " (CASE                                          ";
                Strsql += " WHEN JAE.MARKS_NUMBERS IS NOT NULL THEN        ";
                Strsql += " JAE.MARKS_NUMBERS                              ";
                Strsql += " ELSE                                           ";
                Strsql += " JAE.MARKS_NUMBERS                              ";
                Strsql += " END),                                          ";
                Strsql += " (CASE                                          ";
                Strsql += " WHEN JAE.GOODS_DESCRIPTION IS NOT NULL THEN    ";
                Strsql += " JAE.GOODS_DESCRIPTION                          ";
                Strsql += " ELSE                                           ";
                Strsql += " JAE.GOODS_DESCRIPTION                          ";
                Strsql += " END),                                          ";
                Strsql += " JTAEC.CONTAINER_NUMBER,                        ";
                Strsql += " JTAEC.SEAL_NUMBER,                             ";
                Strsql += " CONTAINER.CONTAINER_TYPE_MST_ID,                ";
                Strsql += " CONTAINER.CONTAINER_TAREWEIGHT_TONE,            ";
                Strsql += " JTAEC.PACK_COUNT,                               ";
                Strsql += " CORP.CORPORATE_NAME,                            ";
                Strsql += " TO_CHAR(SYSDATE,'" + dateFormat + "') ,                 ";
                Strsql += " JAE.TRANSPORTER_CARRIER_FK ,                    ";
                Strsql += " TRANSPORTER.VENDOR_NAME ,                       ";
                Strsql += "  ' '                                            ";
                return ObjWF.GetDataSet(Strsql);
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
        #endregion

        #region "Fetch Volume and Gross Weight"
        public Int32 FetchVolume(string JOBPK)
        {
            string Strsql = null;
            Int32 TotVol = 0;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = " select sum(nvl(j.volume_in_cbm,0)) ";
                Strsql += " from JOB_TRN_SEA_EXP_CONT j where j.job_card_sea_exp_fk=" + JOBPK;
                TotVol = Convert.ToInt32(ObjWF.ExecuteScaler(Strsql));
                return TotVol;
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

        public Int32 FetchGrossWt(string JOBPK)
        {
            string Strsql = null;
            Int32 TotGrossWt = 0;
            WorkFlow ObjWF = new WorkFlow();
            try
            {
                Strsql = " select sum(nvl(j.gross_weight,0)) ";
                Strsql += " from JOB_TRN_SEA_EXP_CONT j where j.job_card_sea_exp_fk=" + JOBPK;
                TotGrossWt = Convert.ToInt32(ObjWF.ExecuteScaler(Strsql));
                return TotGrossWt;
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
        #endregion

        #region "Enhance Search Function"
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
            string operatorPK = null;


            var strNull = DBNull.Value;

            arr = strCond.Split('~');

            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));

            POL = Convert.ToString(arr.GetValue(2));
            POD = Convert.ToString(arr.GetValue(3));
            operatorPK = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBCARD_MASTER_PKG.GET_MASTERJOBCARDSEA";

                var _with52 = selectCommand.Parameters;
                _with52.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with52.Add("LOOKUP_VALUE_IN", (!string.IsNullOrEmpty(strReq) ? strReq : "")).Direction = ParameterDirection.Input;
                _with52.Add("POL_IN", (!string.IsNullOrEmpty(POL) ? POL : "")).Direction = ParameterDirection.Input;
                _with52.Add("POD_IN", (!string.IsNullOrEmpty(POD) ? POD : "")).Direction = ParameterDirection.Input;
                _with52.Add("OPERATOR_IN", (!string.IsNullOrEmpty(operatorPK) ? operatorPK : "")).Direction = ParameterDirection.Input;
                _with52.Add("LOC_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                //Snigdharani - 15/12/2008
                _with52.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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
            finally
            {
                selectCommand.Connection.Close();
            }
        }
        #endregion

        #region " Fetch Max Contract No."
        public string FetchJobCardReferenceNo(string strMasterJobCardNo)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                string strResult = null;

                strSQL = " SELECT NVL(MAX(T.jobcard_ref_no),0) FROM job_card_sea_exp_tbl T " + " WHERE t.master_jc_sea_exp_fk = " + strMasterJobCardNo + " ORDER BY T.jobcard_ref_no ";

                strResult = objWF.ExecuteScaler(strSQL);

                if (strResult == "0")
                {
                    strSQL = " select m.master_jc_ref_no from master_jc_sea_exp_tbl m " + " where m.master_jc_sea_exp_pk = " + strMasterJobCardNo;
                    strResult = objWF.ExecuteScaler(strSQL);
                }

                return strResult;
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
        #endregion

        #region " Fetch VAT percentage "
        public double FetchVATPercentage()
        {
            try
            {
                string strSQL = "select nvl(c.vat_percentage,0) from corporate_mst_tbl c";
                WorkFlow objWK = new WorkFlow();
                return Convert.ToDouble(objWK.ExecuteScaler(strSQL));
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
        #endregion

        #region "Fetch EnableDisableOprStatus & Save Operator Booking"

        public string funEDisableOprStatus(string strBookingRefNo)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                string strReturn = null;
                strBuilder.Append(" SELECT ");
                strBuilder.Append(" BST.BOOKING_SEA_PK");
                strBuilder.Append(" FROM");
                strBuilder.Append(" BOOKING_SEA_TBL BST ,");
                strBuilder.Append(" JOB_CARD_SEA_EXP_TBL JHDR");
                strBuilder.Append(" WHERE");
                strBuilder.Append(" BST.OPR_UPDATE_STATUS=1 ");
                strBuilder.Append(" AND JHDR.BOOKING_SEA_FK=BST.BOOKING_SEA_PK");
                strBuilder.Append(" AND (JHDR.HBL_EXP_TBL_FK IS NULL AND JHDR.MBL_EXP_TBL_FK IS NULL)");
                strBuilder.Append(" AND BST.BOOKING_REF_NO='" + strBookingRefNo + "'");

                strReturn = objWF.ExecuteScaler(strBuilder.ToString());
                return strReturn;
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

        private object funUpStreamUpdationBookingOpr(string strBookingRefNo, string strOperatorPk, OracleTransaction TRAN)
        {
            try
            {
                arrMessage.Clear();
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                OracleCommand OCUpdCmd = new OracleCommand();
                Int16 intReturn = default(Int16);
                var _with53 = OCUpdCmd;
                _with53.CommandType = CommandType.StoredProcedure;
                _with53.CommandText = objWF.MyUserName + ".BB_JOB_CARD_SEA_PKG.UPDATE_UPSTREAM_BOOKINGOPR";
                _with53.Connection = TRAN.Connection;
                _with53.Transaction = TRAN;

                _with53.Parameters.Clear();
                _with53.Parameters.Add("BOOKING_REFNO_IN", strBookingRefNo).Direction = ParameterDirection.Input;
                _with53.Parameters.Add("OPERATOR_FK_IN", strOperatorPk).Direction = ParameterDirection.Input;
                _with53.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                intReturn = Convert.ToInt16(_with53.ExecuteNonQuery());



                if (intReturn == 1)
                {
                    arrMessage.Add("All Data Saved Successfully");
                }
                else
                {
                    arrMessage.Add("Upstream updation failed, Check Operator");
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

        #endregion

        #region "Fetch MJCPK"
        //Added by rabbani
        public string FetchMJCPK(string jobCardPK = "0")
        {
            string strSQL = null;
            strSQL = "select job_exp.master_jc_sea_exp_fk" + "from job_card_sea_exp_tbl job_exp" + "where job_exp.job_card_sea_exp_pk=" + jobCardPK;
            try
            {
                return (new WorkFlow()).ExecuteScaler(strSQL);
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

        #endregion

        #region "Export to XML"         ' Manoharan 04June2008 for Qfor-Qfin
        public DataSet Export2XML(string jobCardPK = "0")
        {

            WorkFlow objWF = new WorkFlow();
            DataTable dtSale = null;
            DataTable dtFrt = null;
            DataTable dtCost = null;
            DataSet MainDs = new DataSet();

            try
            {
                dtSale = getSalesHeader(jobCardPK);
                dtFrt = getSalesFreight(jobCardPK);
                dtCost = getSalesCost(jobCardPK);
                MainDs.Tables.Add(dtSale);
                MainDs.Tables.Add(dtFrt);
                MainDs.Tables.Add(dtCost);

                MainDs.Tables[0].TableName = "SALES";
                MainDs.Tables[1].TableName = "CHARGEDETAILS";
                MainDs.Tables[2].TableName = "COSTDETAILS";

                DataRelation relJc_Frt = new DataRelation("relJcFrt", new DataColumn[] { MainDs.Tables["SALES"].Columns["JOB_PK"] }, new DataColumn[] { MainDs.Tables["CHARGEDETAILS"].Columns["JOB_PK"] });
                DataRelation relJc_Cost = new DataRelation("relJcCost", new DataColumn[] { MainDs.Tables["SALES"].Columns["JOB_PK"] }, new DataColumn[] { MainDs.Tables["COSTDETAILS"].Columns["JOB_PK"] });

                relJc_Frt.Nested = true;
                relJc_Cost.Nested = true;

                MainDs.Relations.Add(relJc_Frt);
                MainDs.Relations.Add(relJc_Cost);
                MainDs.DataSetName = "JOBCARDSALES";

                return MainDs;

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

        public DataTable getSalesHeader(string jobCardPK = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT  ");
            strSQL.Append(" ' ' AS \"SALENR\",' ' AS \"SALEDT\",' ' AS \"ACTIVITYDT\",' ' AS \"ROEBASIS\", ");
            strSQL.Append(" Q.jobcard_ref_no as \"JCNUMBER\", Q.jobcard_date as \"JCDATE\", Q.booking_ref_no as \"BKGNUMBER\", ");
            strSQL.Append(" Q.booking_date AS \"BKGDATE\", Q.hbl_ref_no as \"BLNUMBER\", Q.hbl_date as \"BLDATE\", ");
            strSQL.Append(" Q.customer_id  as \"PARTYID\", Q.customer_name  as \"PARTYNAME\", 'CUSTOMER' AS \"PARTYFLAG\", ");
            strSQL.Append(" 'SEA' AS \"BIZTYPE\", Q.place_code  as \"POR\", Q.POL as \"POL\", Q.POD as \"POD\", ");
            strSQL.Append(" Q.PFD as \"PFD\", 'EXPORT' AS \"PROCESSTYPE\", Q.cargo_move_code as \"SHIPMENTTERMS\" , ");
            strSQL.Append(" Q.VSL_VOYAGE AS \"VSLVOYAGE\", 'CONTAINER' AS \"SHIPMENTTYPE\", Q.job_card_sea_exp_pk AS \"JOB_PK\" FROM (SELECT ");
            strSQL.Append(" job_exp.job_card_sea_exp_pk, job_exp.booking_sea_fk, job_exp.jobcard_ref_no , ");
            strSQL.Append(" to_char(job_exp.jobcard_date,'dd-Mon-yyyy') jobcard_date, bst.booking_ref_no, to_char(bst.booking_date,'dd-Mon-yyyy') booking_date, ");
            strSQL.Append(" job_exp.hbl_exp_tbl_fk, nvl(hbl.hbl_ref_no,' ') hbl_ref_no, nvl(to_char(hbl.hbl_date, 'dd-Mon-yyyy'), ' ') hbl_date, job_exp.shipper_cust_mst_fk, shipper.customer_id, ");
            strSQL.Append(" shipper.customer_name,bst.col_place_mst_fk, nvl(col_place.place_code,' ') place_code, bst.port_mst_pol_fk, ");
            strSQL.Append(" pol.port_id as \"POL\",bst.port_mst_pod_fk, pod.port_id as \"POD\",bst.del_place_mst_fk, ");
            strSQL.Append(" nvl(del_place.place_code,' ')  as \"PFD\", job_exp.cargo_move_fk, stm.cargo_move_code, ");
            strSQL.Append(" VVT.VOYAGE_TRN_PK \"VoyagePK\", CASE WHEN V.VESSEL_ID IS NULL THEN ' ' ELSE ");
            strSQL.Append(" V.VESSEL_ID || '/' || VVT.VOYAGE END AS \"VSL_VOYAGE\" ");
            strSQL.Append(" FROM ");
            strSQL.Append(" job_card_sea_exp_tbl job_exp,booking_sea_tbl bst,port_mst_tbl POD,port_mst_tbl POL, ");
            strSQL.Append(" cargo_move_mst_tbl stm,customer_mst_tbl shipper,place_mst_tbl col_place, ");
            strSQL.Append(" place_mst_tbl del_place,VESSEL_VOYAGE_TBL V,  VESSEL_VOYAGE_TRN VVT, hbl_exp_tbl hbl ");
            strSQL.Append(" WHERE ");
            strSQL.Append(" job_exp.job_card_sea_exp_pk = " + jobCardPK);
            strSQL.Append(" AND job_exp.booking_sea_fk   =  bst.booking_sea_pk ");
            strSQL.Append(" AND bst.port_mst_pol_fk  =  pol.port_mst_pk ");
            strSQL.Append(" AND bst.port_mst_pod_fk  =  pod.port_mst_pk ");
            strSQL.Append(" AND bst.col_place_mst_fk =  col_place.place_pk(+) ");
            strSQL.Append(" AND bst.del_place_mst_fk =  del_place.place_pk(+) ");
            strSQL.Append(" AND job_exp.shipper_cust_mst_fk  =  shipper.customer_mst_pk(+) ");
            strSQL.Append(" AND VVT.VESSEL_VOYAGE_TBL_FK =  V.VESSEL_VOYAGE_TBL_PK(+)  ");
            strSQL.Append(" AND JOB_EXP.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+) ");
            strSQL.Append(" and hbl.hbl_exp_tbl_pk(+) = job_exp.hbl_exp_tbl_fk ");
            strSQL.Append(" and job_exp.cargo_move_fk = stm.cargo_move_pk(+)) Q ");

            return objWF.GetDataTable(strSQL.ToString());
        }
        public DataTable getSalesFreight(string jobCardPK = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT ");
            strSQL.Append(" Q.freight_element_id  AS \"CHARGECODE\",' ' AS \"PACKAGETYPE\", ");
            strSQL.Append(" Q.container_type_mst_id AS \"CONTAINERTYPE\",Q.currency_id AS \"CURRENCY\", ");

            //strSQL.Append(" Q.QUANTITY AS ""QUANTITY"",Q.freight_amt/Q.QUANTITY AS ""RATE"", ")
            strSQL.Append(" Q.QUANTITY AS \"QUANTITY\",");
            strSQL.Append(" case when (Q.QUANTITY=0 or Q.freight_amt=0) then 0 ");
            strSQL.Append(" else ");
            strSQL.Append(" Q.freight_amt/Q.QUANTITY ");
            strSQL.Append(" END \"RATE\",");

            strSQL.Append(" Q.freight_type AS \"PCFLAG\",Q.freight_amt AS \"AMOUNT\", ");
            strSQL.Append(" ' ' AS \"VATPERCENTAGE\",' ' AS \"VATAMOUNT\", ");
            strSQL.Append(" Q.location_id AS \"COLLECTLOCATION\",Q.customer_id AS \"COLLECTPARTY\", ");
            strSQL.Append(" ' ' AS \"ROE\",' ' AS \"ROEBASIS\",' ' AS \"AMT_IN_BASE\",' ' as \"STATUS\", Q.job_card_sea_exp_fk AS \"JOB_PK\" ");
            strSQL.Append(" FROM (SELECT ");
            strSQL.Append(" job_trn_sea_exp_fd_pk, job_card_sea_exp_fk,\tcontainer_type_mst_fk, freight.freight_element_id, ");
            strSQL.Append(" freight.freight_element_mst_pk,\tcont.container_type_mst_id, job_trn_fd.currency_mst_fk, ");
            strSQL.Append(" curr.currency_id, (select Count(*) FROM job_trn_sea_exp_cont job_trn_cont ");
            strSQL.Append(" where job_trn_cont.job_card_sea_exp_fk = " + jobCardPK);
            strSQL.Append(" and job_trn_cont.container_type_mst_fk = container_type_mst_fk) as \"QUANTITY\", ");
            strSQL.Append(" DECODE(job_trn_fd.freight_type,1,'Prepaid',2,'Collect') freight_type,  ");
            strSQL.Append(" job_trn_fd.freight_amt, job_trn_fd.location_mst_fk, lmt.location_id , ");
            strSQL.Append(" job_trn_fd.frtpayer_cust_mst_fk, cmt.customer_id ");
            strSQL.Append(" FROM ");
            strSQL.Append(" job_trn_sea_exp_fd job_trn_fd, container_type_mst_tbl cont, currency_type_mst_tbl curr, ");
            strSQL.Append(" freight_element_mst_tbl freight, parameters_tbl prm, job_card_sea_exp_tbl job_exp, ");
            strSQL.Append(" location_mst_tbl lmt, customer_mst_tbl cmt ");
            strSQL.Append(" WHERE ");
            strSQL.Append(" job_trn_fd.job_card_sea_exp_fk = job_exp.job_card_sea_exp_pk ");
            strSQL.Append(" AND job_trn_fd.container_type_mst_fk = cont.container_type_mst_pk(+) ");
            strSQL.Append(" AND job_trn_fd.Currency_Mst_Fk = curr.currency_mst_pk ");
            strSQL.Append(" AND job_trn_fd.freight_element_mst_fk = freight.freight_element_mst_pk ");
            strSQL.Append(" AND job_trn_fd.freight_element_mst_fk = prm.frt_bof_fk ");
            strSQL.Append(" AND job_trn_fd.location_mst_fk = lmt.location_mst_pk (+) ");
            strSQL.Append(" AND job_trn_fd.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+) ");
            strSQL.Append(" AND job_exp.job_card_sea_exp_pk =" + jobCardPK);
            strSQL.Append("  union all  ");
            strSQL.Append("  SELECT ");
            strSQL.Append(" job_trn_sea_exp_fd_pk, job_card_sea_exp_fk, container_type_mst_fk, freight.freight_element_id, ");
            strSQL.Append(" freight.freight_element_mst_pk,\tcont.container_type_mst_id, job_trn_fd.currency_mst_fk, ");
            strSQL.Append(" curr.currency_id, (select Count(*) FROM job_trn_sea_exp_cont job_trn_cont ");
            strSQL.Append(" where job_trn_cont.job_card_sea_exp_fk = " + jobCardPK);
            strSQL.Append(" and job_trn_cont.container_type_mst_fk = container_type_mst_fk) as \"QUANTITY\", ");
            strSQL.Append(" DECODE(job_trn_fd.freight_type,1,'Prepaid',2,'Collect') freight_type, ");
            strSQL.Append(" job_trn_fd.freight_amt, job_trn_fd.location_mst_fk, lmt.location_id , ");
            strSQL.Append(" job_trn_fd.frtpayer_cust_mst_fk, cmt.customer_id ");
            strSQL.Append(" FROM ");
            strSQL.Append(" job_trn_sea_exp_fd job_trn_fd, container_type_mst_tbl cont, currency_type_mst_tbl curr, ");
            strSQL.Append(" freight_element_mst_tbl freight, parameters_tbl prm, job_card_sea_exp_tbl job_exp, ");
            strSQL.Append(" location_mst_tbl lmt, customer_mst_tbl cmt ");
            strSQL.Append(" WHERE ");
            strSQL.Append(" job_trn_fd.job_card_sea_exp_fk = job_exp.job_card_sea_exp_pk ");
            strSQL.Append(" AND job_trn_fd.container_type_mst_fk = cont.container_type_mst_pk(+) ");
            strSQL.Append(" AND job_trn_fd.Currency_Mst_Fk = curr.currency_mst_pk ");
            strSQL.Append(" AND job_trn_fd.freight_element_mst_fk = freight.freight_element_mst_pk ");
            strSQL.Append(" AND job_trn_fd.freight_element_mst_fk not in  prm.frt_bof_fk ");
            strSQL.Append("  AND job_trn_fd.location_mst_fk = lmt.location_mst_pk (+) ");
            strSQL.Append(" AND job_trn_fd.frtpayer_cust_mst_fk = cmt.customer_mst_pk(+) ");
            strSQL.Append(" AND job_exp.job_card_sea_exp_pk = " + jobCardPK);
            strSQL.Append(" )Q");

            return objWF.GetDataTable(strSQL.ToString());
        }
        public DataTable getSalesCost(string jobCardPK = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT ");
            strSQL.Append(" Q.vendor_key AS \"PARTYID\", Q.VENDOR_NAME AS \"PARTYNAME\", 'VENDOR' AS \"PARTYTYPE\", ");
            strSQL.Append(" Q.cost_element_id AS \"COSTCODE\", Q.LOCATION_ID AS \"COSTCENTER\", Q.CONT_TYPE AS \"CONTAINERTYPE\", ");
            //
            strSQL.Append(" ' ' AS \"PACKAGETYPE\", Q.CURRENCY_ID AS \"CURRENCY\" , Q.QUANTITY AS \"QUANTITY\", ");

            //strSQL.Append(" (Q.estimated_amt/QUANTITY) AS ""RATE"", Q.estimated_amt AS ""AMOUNT"", ")
            strSQL.Append(" case when (Q.estimated_amt=0 or QUANTITY=0) then 0 ");
            strSQL.Append(" else ");
            strSQL.Append(" Q.estimated_amt/QUANTITY");
            strSQL.Append(" END \"RATE\",");

            strSQL.Append(" Q.tax_percentage AS \"VATPERCENTAGE\", Q.tax_amt AS \"VATAMOUNT\", Q.invoice_amt AS \"TOTALAMOUNT\", ");
            strSQL.Append(" ' ' AS \"ROE\", ' ' AS \"ROEBASIS\", ' ' AS \"AMT_IN_BASE\", ");
            strSQL.Append(" Q.job_card_sea_exp_fk AS \"JOB_PK\" ");
            strSQL.Append(" FROM (SELECT ");
            strSQL.Append(" job_trn_sea_exp_pia_pk, job_card_sea_exp_fk, vendor_mst_fk, vendor_key, ");
            strSQL.Append(" VENDOR.VENDOR_NAME, cost_element_mst_fk, cost_element_id, LMT.LOCATION_ID,");
            //
            strSQL.Append(" (select DISTINCT CONT.CONTAINER_TYPE_MST_ID FROM job_trn_sea_exp_cont job_trn_cont, ");
            strSQL.Append(" container_type_mst_tbl cont where ");
            strSQL.Append(" CONT.CONTAINER_TYPE_MST_PK = job_trn_cont.container_type_mst_fk ");
            strSQL.Append(" AND job_trn_cont.job_card_sea_exp_fk= " + jobCardPK);
            strSQL.Append(" ) AS \"CONT_TYPE\", ");
            strSQL.Append(" currency_mst_fk, CURR.CURRENCY_ID, (select Count(*) FROM job_trn_sea_exp_cont job_trn_cont, ");
            strSQL.Append(" container_type_mst_tbl cont where ");
            strSQL.Append(" CONT.CONTAINER_TYPE_MST_PK = job_trn_cont.container_type_mst_fk ");
            strSQL.Append(" AND job_trn_cont.job_card_sea_exp_fk = " + jobCardPK);
            strSQL.Append(" ) as \"QUANTITY\", ");
            strSQL.Append(" estimated_amt, tax_percentage, tax_amt,\tinvoice_amt ");
            strSQL.Append(" FROM ");
            strSQL.Append(" job_trn_sea_exp_pia  job_trn_pia, ");
            strSQL.Append(" currency_type_mst_tbl curr, ");
            strSQL.Append(" cost_element_mst_tbl cost_ele, ");
            strSQL.Append(" job_card_sea_exp_tbl job_exp, USER_MST_TBL  UMT, LOCATION_MST_TBL      LMT,");
            //
            strSQL.Append(" VENDOR_MST_TBL VENDOR ");
            strSQL.Append(" WHERE ");
            strSQL.Append(" job_trn_pia.job_card_sea_exp_fk = job_exp.job_card_sea_exp_pk ");
            strSQL.Append(" AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk ");
            strSQL.Append(" AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk ");
            strSQL.Append(" AND VENDOR.VENDOR_MST_PK = job_trn_pia.Vendor_Mst_Fk AND JOB_EXP.CREATED_BY_FK = UMT.USER_MST_PK AND UMT.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK");
            //
            strSQL.Append(" AND job_exp.job_card_sea_exp_pk = " + jobCardPK);
            strSQL.Append(" ) Q");

            return objWF.GetDataTable(strSQL.ToString());
        }
        //adding by thiyagarajan on 10/11/08 to display PDF through JOBCARD Entry Screen :PTS Task
        public string AgentType(string Refno, Int32 process, Int32 biztype)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append(" select decode(inv.CB_DP_LOAD_AGENT,1,'CB',2,'DP',3,'LA') from INV_AGENT_TBL inv where ");
                strSQL.Append(" inv.invoice_ref_no like '" + Refno + "' ");
                //If process = 2 Then
                //    strSQL.Replace("EXP", "IMP")
                //    strSQL.Replace("cb_or_dp_agent", "CB_DP_LOAD_AGENT")
                //End If
                //If biztype = 1 Then
                //    strSQL.Replace("SEA", "AIR")
                //End If
                return objWF.ExecuteScaler(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public ArrayList CreditType(string Refno, Int32 process, Int32 biztype)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            ArrayList result = new ArrayList();
            OracleDataReader cmdreader = null;
            try
            {
                strSQL.Append(" select CR.CR_AGENT_PK PK,decode(CR.CB_DP_LOAD_AGENT,1,'CB',2,'DP') CRType ");
                strSQL.Append(" from CR_AGENT_TBL CR WHERE CR.CREDIT_NOTE_REF_NO LIKE '" + Refno + "' ");
                if (process == 2)
                {
                    strSQL.Replace("EXP", "IMP");
                    strSQL.Replace("CB_DP_LOAD_AGENT", "CB_DP_LOAD_AGENT");
                }
                if (biztype == 1)
                {
                    strSQL.Replace("SEA", "AIR");
                }
                cmdreader = objWF.GetDataReader(strSQL.ToString());
                while (cmdreader.Read())
                {
                    result.Add(cmdreader[0]);
                    result.Add(cmdreader[1]);
                }
                cmdreader.Close();
                objWF.MyConnection.Close();
                return result;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public Int32 CrCustomer(string Refno, Int32 process, Int32 biztype)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            ArrayList result = new ArrayList();
            OracleDataReader cmdreader = null;
            try
            {
                strSQL.Append(" select cust.cr_cust_sea_exp_pk from CR_CUST_SEA_EXP_TBL cust where cust.credit_note_ref_no like '" + Refno + "' ");
                if (biztype == 1)
                {
                    strSQL.Replace("SEA", "AIR");
                }
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public string CRAgentType(string Refno, Int32 process, Int32 biztype)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append(" select decode(inv.CB_DP_LOAD_AGENT,1,'CB',2,'DP') from CR_AGENT_TBL inv where ");
                strSQL.Append(" inv.credit_note_ref_no like '" + Refno + "' ");
                if (process == 2)
                {
                    strSQL.Replace("EXP", "IMP");
                    strSQL.Replace("CB_DP_LOAD_AGENT", "CB_DP_LOAD_AGENT");
                }
                if (biztype == 1)
                {
                    strSQL.Replace("SEA", "AIR");
                }
                return objWF.ExecuteScaler(strSQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public Int32 GetCRPk(string Refno, Int32 process, Int32 biztype)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append(" select inv.cr_agent_pk from CR_AGENT_TBL inv where ");
                strSQL.Append(" inv.credit_note_ref_no like '" + Refno + "' ");
                if (process == 2)
                {
                    strSQL.Replace("EXP", "IMP");
                }
                if (biztype == 1)
                {
                    strSQL.Replace("SEA", "AIR");
                }
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //end by thiyagarajan
        #endregion

        #region "CLP Report MainDS"
        public DataSet FetchRptDS(string JOBPK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL = "select rownum as slnr,";
                strSQL += "job_exp.job_card_sea_exp_pk,";
                strSQL += "job_exp.booking_sea_fk,";
                strSQL += "job_exp.jobcard_ref_no,";
                strSQL += "job_exp.vessel_name,";
                strSQL += "job_exp.voyage,";
                strSQL += "job_exp.survey_ref_nr,";
                strSQL += "job_exp.survey_date,";
                strSQL += "job_exp.stuff_loc,";
                strSQL += "chaagent.agent_id,";
                strSQL += "chaagent.agent_name,";
                strSQL += "job_cont.container_number,";
                strSQL += "cont_type.container_type_mst_id,";
                strSQL += "cont_type.container_type_name,";
                strSQL += "cont_type.container_tareweight_tone,";
                strSQL += "'' agent_seal_no,";
                strSQL += "job_cont.seal_number customs_seal_no,";
                strSQL += "pod.port_name,";
                strSQL += "del_place.place_code,";
                strSQL += "del_place.place_name,";
                strSQL += "job_exp.sb_number,";
                strSQL += "job_exp.sb_date,";
                strSQL += "shipper.customer_name shipper,";
                strSQL += "job_cont.pack_count,";
                strSQL += "job_cont.gross_weight,";
                strSQL += "job_exp.marks_numbers,";
                strSQL += "job_exp.goods_description,";
                strSQL += "consignee.customer_name consignee,";
                strSQL += "job_cont.volume_in_cbm,";
                strSQL += "chaagent.agent_name chaagent,";
                strSQL += "'' csno,'' grno,";
                strSQL += "pod.port_id as sbpod,";
                strSQL += "job_exp.remarks,";
                strSQL += "'' invnr";
                strSQL += "from job_card_sea_exp_tbl   job_exp,";
                strSQL += "agent_mst_tbl          chaagent,";
                strSQL += "job_trn_sea_exp_cont   job_cont,";
                strSQL += "container_type_mst_tbl cont_type,";
                strSQL += "port_mst_tbl           pod,";
                strSQL += "booking_sea_tbl        bkg_sea,";
                strSQL += "place_mst_tbl          del_place,";
                strSQL += "customer_mst_tbl       shipper,";
                strSQL += "customer_mst_tbl       consignee";
                strSQL += "where job_exp.job_card_sea_exp_pk IN( " + JOBPK + " )";
                strSQL += "and job_exp.cha_agent_mst_fk = chaagent.agent_mst_pk(+)";
                strSQL += "and job_exp.job_card_sea_exp_pk(+) = job_cont.job_card_sea_exp_fk";
                strSQL += "and cont_type.container_type_mst_pk(+) = job_cont.container_type_mst_fk";
                strSQL += "and job_exp.booking_sea_fk = bkg_sea.booking_sea_pk";
                strSQL += "and bkg_sea.port_mst_pod_fk = pod.port_mst_pk";
                strSQL += "and bkg_sea.del_place_mst_fk = del_place.place_pk(+)";
                strSQL += "and job_exp.shipper_cust_mst_fk =  shipper.customer_mst_pk(+)";
                strSQL += "and job_exp.consignee_cust_mst_fk =  consignee.customer_mst_pk(+)";
                return (objWF.GetDataSet(strSQL));
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
        #endregion

        #region "Stuffing Report"
        public DataSet FetchBkgPk(string BookingNr)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL = " SELECT BST.BOOKING_SEA_PK ";
                strSQL += " FROM BOOKING_SEA_TBL BST ";
                strSQL += " WHERE BST.BOOKING_REF_NO = '" + BookingNr + "'";
                return (objWF.GetDataSet(strSQL));
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
        public DataSet FetchBookingDetails(int SeaBkgPK)
        {
            string strSql = null;
            WorkFlow Objwk = new WorkFlow();
            strSql = "SELECT JSE.JOB_CARD_SEA_EXP_PK JOBPK, ";
            strSql += "JSE.JOBCARD_REF_NO JOBREFNO, ";
            strSql += "BST.BOOKING_SEA_PK BKGPK, ";
            strSql += "BST.BOOKING_REF_NO BKGREFNO, ";
            strSql += "BST.BOOKING_DATE BKGDATE, ";
            //strSql &= vbCrLf & "(CASE WHEN BST.VOYAGE IS NOT NULL THEN "
            //strSql &= vbCrLf & "BST.VESSEL_NAME ||'-' || BST.VOYAGE "
            //strSql &= vbCrLf & "ELSE"
            //strSql &= vbCrLf & "BST.VESSEL_NAME END ) VESFLIGHT,"
            strSql += "(CASE";
            strSql += "WHEN JSE.VOYAGE_TRN_FK IS NOT NULL THEN";
            strSql += "JSE.VESSEL_NAME || '-' || JSE.VOYAGE";
            strSql += "ELSE";
            strSql += "JSE.VESSEL_NAME ";
            strSql += "END) VESFLIGHT,";
            strSql += "HBL.HBL_REF_NO HBLREFNO,";
            strSql += " MBL.MBL_REF_NO  MBLREFNO,";
            strSql += " JSE.MARKS_NUMBERS MARKS,";

            strSql += " JSE.GOODS_DESCRIPTION GOODS,";
            strSql += "BST.CARGO_TYPE,";
            //strSql &= vbCrLf & "JSE.UCR_NO UCRN0,"
            strSql += "OPTA.OPERATOR_NAME UCRN0,";
            strSql += "'' CLEARANCEPOINT,";
            //strSql &= vbCrLf & " BST.ETD_DATE ETD,"
            strSql += " JSE.ETD_DATE ETD,";
            strSql += "BST.CUST_CUSTOMER_MST_FK,";
            strSql += "CMST.CUSTOMER_NAME SHIPNAME,";
            strSql += "CMST.ACCOUNT_NO SHIPREFNO,";
            strSql += "CDTLS.ADM_ADDRESS_1 SHIPADD1,";
            strSql += "CDTLS.ADM_ADDRESS_2 SHIPADD2,";
            strSql += "CDTLS.ADM_ADDRESS_3 SHIPADD3,";
            strSql += "CDTLS.ADM_CITY SHIPCITY,";
            strSql += "CDTLS.ADM_ZIP_CODE SHIPZIP,";
            strSql += "CDTLS.ADM_EMAIL_ID AS SHIPEMAIL,";
            strSql += "CDTLS.ADM_PHONE_NO_1 SHIPPHONE,";
            strSql += "CDTLS.ADM_FAX_NO SHIPFAX,";
            strSql += " SHIPCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
            strSql += " CONSIGNEE.CUSTOMER_NAME CONSIGNEENAME,";
            strSql += "CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGADD1,";
            strSql += "CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGADD2,";
            strSql += "CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGADD3,";
            strSql += "CONSIGNEEDTLS.ADM_CITY CONSIGCITY,";
            strSql += "CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGZIP,";
            strSql += "CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGEMAIL,";
            strSql += "CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGPHONE,";
            strSql += "CONSIGNEEDTLS.ADM_FAX_NO CONSIGFAX,";
            strSql += " CONSIGCOUNTRY.COUNTRY_NAME CONSICOUNTRY,";
            strSql += " POL.PORT_NAME POLNAME,";
            strSql += " POD.PORT_NAME PODNAME,";
            strSql += " PLD.PLACE_NAME DELNAME,";
            strSql += " COLPLD.PLACE_NAME COLNAME,";

            strSql += " DBAMST.AGENT_MST_PK DBAGENTPK,";
            strSql += " DBAMST.AGENT_NAME  DBAGENTNAME,";
            strSql += " DBADTLS.ADM_ADDRESS_1  DBAGENTADD1,";
            strSql += " DBADTLS.ADM_ADDRESS_2  DBAGENTADD2,";
            strSql += " DBADTLS.ADM_ADDRESS_3  DBAGENTADD3,";
            strSql += " DBADTLS.ADM_CITY  DBAGENTCITY,";
            strSql += " DBADTLS.ADM_ZIP_CODE DBAGENTZIP,";
            strSql += " DBADTLS.ADM_EMAIL_ID DBAGENTEMAIL,";
            strSql += " DBADTLS.ADM_PHONE_NO_1 DBAGENTPHONE,";
            strSql += " DBADTLS.ADM_FAX_NO DBAGENTFAX,";
            strSql += " DBCOUNTRY.COUNTRY_NAME DBCOUNTRY,";
            strSql += "STMST.INCO_CODE TERMS,";
            strSql += " NVL(JSE.INSURANCE_AMT,0) INSURANCE,";
            strSql += " BST.PYMT_TYPE ,";
            strSql += " CGMST.commodity_group_desc COMMCODE,";
            //strSql &= vbCrLf & " BST.ETA_DATE ETA,"
            strSql += " JSE.ETA_DATE ETA,";
            strSql += " BST.GROSS_WEIGHT GROSS,";
            strSql += " BST.CHARGEABLE_WEIGHT CHARWT,";
            strSql += " BST.NET_WEIGHT NETWT,";
            strSql += " BST.VOLUME_IN_CBM VOLUME";

            strSql += "FROM   JOB_CARD_SEA_EXP_TBL JSE,";
            strSql += " BOOKING_SEA_TBL BST,";
            strSql += " CUSTOMER_MST_TBL CMST,";
            strSql += " OPERATOR_MST_TBL        OPTA,";
            strSql += " VESSEL_VOYAGE_TBL       VVT,";
            strSql += " VESSEL_VOYAGE_TRN       VVTT,";
            strSql += " CUSTOMER_MST_TBL CONSIGNEE,";
            strSql += " CUSTOMER_CONTACT_DTLS CDTLS,";
            strSql += " CUSTOMER_CONTACT_DTLS CONSIGNEEDTLS,";
            strSql += " COUNTRY_MST_TBL SHIPCOUNTRY,";
            strSql += " COUNTRY_MST_TBL CONSIGCOUNTRY,";
            strSql += " PORT_MST_TBL POL,";
            strSql += " PORT_MST_TBL POD,";
            strSql += " PLACE_MST_TBL PLD,";
            strSql += " PLACE_MST_TBL COLPLD,";
            strSql += "AGENT_MST_TBL DBAMST,";
            strSql += "AGENT_CONTACT_DTLS DBADTLS,";
            strSql += "COUNTRY_MST_TBL DBCOUNTRY,";
            strSql += "SHIPPING_TERMS_MST_TBL STMST,";
            strSql += " COMMODITY_GROUP_MST_TBL CGMST,";
            strSql += "HBL_EXP_TBL HBL,";
            strSql += "MBL_EXP_TBL MBL";

            strSql += "WHERE BST.BOOKING_SEA_PK IN ('" + SeaBkgPK + "')";
            strSql += "AND JSE.HBL_EXP_TBL_FK=HBL.HBL_EXP_TBL_PK(+)";
            strSql += "AND JSE.MBL_EXP_TBL_FK=MBL.MBL_EXP_TBL_PK(+)";
            strSql += "AND   CMST.CUSTOMER_MST_PK(+)=BST.CUST_CUSTOMER_MST_FK";
            strSql += "AND   CONSIGNEE.CUSTOMER_MST_PK(+)=BST.CONS_CUSTOMER_MST_FK";
            strSql += "AND   CDTLS.CUSTOMER_MST_FK(+)=CMST.CUSTOMER_MST_PK";
            strSql += "AND CONSIGNEE.CUSTOMER_MST_PK=CONSIGNEEDTLS.CUSTOMER_MST_FK(+)";
            strSql += " AND SHIPCOUNTRY.COUNTRY_MST_PK(+)=CDTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND CONSIGCOUNTRY.COUNTRY_MST_PK(+)=CONSIGNEEDTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND   JSE.BOOKING_SEA_FK(+)=BST.BOOKING_SEA_PK";
            strSql += " AND JSE.VOYAGE_TRN_FK = VVTT.VOYAGE_TRN_PK(+)";
            strSql += " AND VVTT.VESSEL_VOYAGE_TBL_FK= VVT.VESSEL_VOYAGE_TBL_PK(+)";
            strSql += " AND VVT.OPERATOR_MST_FK = OPTA.OPERATOR_MST_PK(+)";
            strSql += " AND   BST.PORT_MST_POL_FK=POL.PORT_MST_PK(+)";
            strSql += " AND   BST.PORT_MST_POD_FK=POD.PORT_MST_PK(+)";
            strSql += " AND   BST.DEL_PLACE_MST_FK=PLD.PLACE_PK(+)";
            strSql += " AND   BST.COL_PLACE_MST_FK=COLPLD.PLACE_PK(+)";
            strSql += " AND   BST.DP_AGENT_MST_FK=DBAMST.AGENT_MST_PK(+)";
            strSql += " AND   DBAMST.AGENT_MST_PK=DBADTLS.AGENT_MST_FK(+)";
            strSql += "AND DBCOUNTRY.COUNTRY_MST_PK(+)=DBADTLS.ADM_COUNTRY_MST_FK";
            strSql += " AND  STMST.SHIPPING_TERMS_MST_PK(+)=BST.CUST_CUSTOMER_MST_FK";
            strSql += " AND  BST.COMMODITY_GROUP_FK=CGMST.COMMODITY_GROUP_PK(+)";
            // strSql &= vbCrLf & "AND  BST.BOOKING_SEA_PK IN ('" & SeaBkgPK & "')"
            try
            {
                return Objwk.GetDataSet(strSql);
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
        public DataSet FetchSeaContainers(string BkgPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT BST.BOOKING_SEA_PK BKGPK, JSE.CONTAINER_NUMBER CONTAINER";
            Strsql += "FROM JOB_TRN_SEA_EXP_CONT JSE,BOOKING_SEA_TBL BST,JOB_CARD_SEA_EXP_TBL JS";
            Strsql += "WHERE BST.BOOKING_SEA_PK = JS.BOOKING_SEA_FK";
            Strsql += "AND JSE.JOB_CARD_SEA_EXP_FK=JS.JOB_CARD_SEA_EXP_PK";
            Strsql += " AND BST.BOOKING_SEA_PK IN (" + BkgPK + ")";
            try
            {
                return Objwk.GetDataSet(Strsql);
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
        public DataSet Get_ConDet(string JBPk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            try
            {
                Strsql = "SELECT J.JOB_CARD_SEA_EXP_FK,";
                Strsql += " J.CONTAINER_NUMBER,";
                Strsql += " CTYPE.CONTAINER_TYPE_MST_ID,";
                Strsql += " CMT.COMMODITY_NAME,";
                Strsql += " J.SEAL_NUMBER , ";
                Strsql += " PTMT.PACK_TYPE_ID,";
                Strsql += " J.PACK_COUNT, ";
                Strsql += " J.GROSS_WEIGHT, ";
                Strsql += " J.VOLUME_IN_CBM,";
                Strsql += " J.NET_WEIGHT ";
                Strsql += "FROM JOB_TRN_SEA_EXP_CONT   J,JOB_CARD_SEA_EXP_TBL   JS,";
                Strsql += " COMMODITY_MST_TBL      CMT,       PACK_TYPE_MST_TBL      PTMT,";
                Strsql += "CONTAINER_TYPE_MST_TBL CTYPE";
                Strsql += "WHERE JS.JOB_CARD_SEA_EXP_PK = J.JOB_CARD_SEA_EXP_FK";
                Strsql += "AND CTYPE.CONTAINER_TYPE_MST_PK(+) = J.CONTAINER_TYPE_MST_FK";
                Strsql += "AND J.COMMODITY_MST_FK= CMT.COMMODITY_MST_PK(+)";
                Strsql += "AND J.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK(+)";
                Strsql += "AND J.JOB_CARD_SEA_EXP_FK in(" + JBPk + ")";
                return Objwk.GetDataSet(Strsql);
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
        public DataSet FetchTransportNote(string JBPk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            try
            {
                Strsql = "SELECT JCSE.JOB_CARD_SEA_EXP_PK,";
                Strsql += "TIST.TRANSPORT_INST_SEA_PK,";
                Strsql += "TIST.TRANS_INST_REF_NO,";
                Strsql += "TIST.MT_CTR_PICKUP_REF,";
                Strsql += "TIST.MT_CTR_PICKUP_BY,";
                Strsql += "TIST.CARGO_PICKUP_REF_NO,";
                Strsql += "TIST.CARGO_PICKUP_BY,";
                Strsql += "TIST.DELIVERY_REF_NO,";
                Strsql += "TIST.DELIVERY_BY";
                Strsql += "FROM TRANSPORT_INST_SEA_TBL TIST, JOB_CARD_SEA_EXP_TBL JCSE";
                Strsql += "    WHERE TIST.JOB_CARD_FK = JCSE.JOB_CARD_SEA_EXP_PK ";
                Strsql += "AND   JCSE.JOB_CARD_SEA_EXP_PK in(" + JBPk + ")";

                return Objwk.GetDataSet(Strsql);
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
        #endregion

        #region "Fetch Location of Port "
        public DataSet fetch_Port_Location_Fk(string strPODfk, string JCDate = "")
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                if (string.IsNullOrEmpty(JCDate))
                {
                    strQuery.Append(" SELECT DISTINCT A.LOCATION_MST_PK, A.LOCATION_ID ");
                    strQuery.Append(" FROM LOCATION_MST_TBL A, PORT_MST_TBL B, LOCATION_WORKING_PORTS_TRN C ");
                    strQuery.Append(" WHERE A.LOCATION_MST_PK = B.LOCATION_MST_FK");
                    strQuery.Append(" AND B.LOCATION_MST_FK = C.LOCATION_MST_FK");
                    strQuery.Append(" AND B.PORT_MST_PK=" + strPODfk);
                    strQuery.Append("");
                }
                else
                {
                    strQuery.Append("SELECT DISTINCT LOC.LOCATION_MST_PK,");
                    strQuery.Append("                LOC.LOCATION_ID,");
                    strQuery.Append("                CNT.CURRENCY_MST_FK,");
                    strQuery.Append("                GET_EX_RATE(CNT.CURRENCY_MST_FK,");
                    strQuery.Append("                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                    strQuery.Append("                            TO_DATE('" + JCDate + "', 'DD/MM/YYYY')) ROE,");
                    strQuery.Append("                GET_EX_RATE_BUY(CNT.CURRENCY_MST_FK,");
                    strQuery.Append("                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                    strQuery.Append("                            TO_DATE('" + JCDate + "', 'DD/MM/YYYY')) ROE_BUY,");
                    strQuery.Append("                CUR.CURRENCY_ID ");
                    strQuery.Append("  FROM LOCATION_MST_TBL           LOC,");
                    strQuery.Append("       PORT_MST_TBL               PORT,");
                    strQuery.Append("       LOCATION_WORKING_PORTS_TRN LOCP,");
                    strQuery.Append("       COUNTRY_MST_TBL            CNT,CURRENCY_TYPE_MST_TBL CUR");
                    strQuery.Append(" WHERE LOC.LOCATION_MST_PK = PORT.LOCATION_MST_FK");
                    strQuery.Append("   AND PORT.LOCATION_MST_FK = LOCP.LOCATION_MST_FK");
                    strQuery.Append("   AND CNT.COUNTRY_MST_PK = LOC.COUNTRY_MST_FK");
                    strQuery.Append("   AND CUR.CURRENCY_MST_PK = CNT.CURRENCY_MST_FK");
                    strQuery.Append("   AND PORT.PORT_MST_PK =" + strPODfk);
                }
                return ObjWk.GetDataSet(strQuery.ToString());
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
        #endregion

        #region "FETCH FREIGHT"
        public DataSet FetchFreightElemet(string JOBPK, string BKGPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT DISTINCT JFD.FREIGHT_ELEMENT_MST_FK, QFT.CHECK_ADVATOS");
                sb.Append("  FROM JOB_TRN_SEA_EXP_FD         JFD,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL    FMT,");
                sb.Append("       QUOTATION_TRN_SEA_FRT_DTLS QFT,");
                sb.Append("       QUOTATION_SEA_TBL          QST,");
                sb.Append("       QUOTATION_TRN_SEA_FCL_LCL  QTS,");
                sb.Append("       JOB_CARD_SEA_EXP_TBL JOB,");
                sb.Append("       BOOKING_SEA_TBL            BKG,");
                sb.Append("       BOOKING_TRN_SEA_FCL_LCL    BTS");
                sb.Append(" WHERE JFD.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND QFT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND JOB.JOB_CARD_SEA_EXP_PK=JFD.JOB_CARD_SEA_EXP_FK");
                sb.Append("   AND JOB.BOOKING_SEA_FK=BKG.BOOKING_SEA_PK");
                sb.Append("   and BTS.TRANS_REF_NO = QST.QUOTATION_REF_NO");
                sb.Append("   AND QST.QUOTATION_SEA_PK = QTS.QUOTATION_SEA_FK");
                sb.Append("   AND QFT.QUOTE_TRN_SEA_FK = QTS.QUOTE_TRN_SEA_PK");
                sb.Append("   AND BTS.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                sb.Append("   AND JFD.JOB_CARD_SEA_EXP_FK = " + JOBPK);
                sb.Append("   AND QFT.CHECK_ADVATOS = 1");
                sb.Append("   AND BKG.BOOKING_SEA_PK=" + BKGPK);
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
        #endregion

        #region "FetchPickUpDropDetails"
        public DataSet FetchPickUpDropDetails(string PickUpDropPK = "0", string JobPK = "0", string BizType = "0", string ProcessType = "0")
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with54 = objWK.MyCommand;
                _with54.CommandType = CommandType.StoredProcedure;
                _with54.CommandText = objWK.MyUserName + ".JOBSPENDIN_DOCREC_PKG.JOB_PICKUP_DROP_DETAILS";

                objWK.MyCommand.Parameters.Clear();
                var _with55 = objWK.MyCommand.Parameters;
                _with55.Add("MAIN_PK_IN", PickUpDropPK).Direction = ParameterDirection.Input;
                _with55.Add("JOB_PK_IN", JobPK).Direction = ParameterDirection.Input;
                _with55.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with55.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with55.Add("MAIN_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);
                return dsData;
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
        #endregion
    }
}