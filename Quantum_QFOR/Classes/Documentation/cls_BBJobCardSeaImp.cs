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
    public class cls_BBJobCardSeaImp : CommonFeatures
    {
        cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();

        cls_SeaBookingEntry objVesselVoyage = new cls_SeaBookingEntry();
        #region "GetMainBookingData"

        #region "Fetch Main Jobcard for export"
        public DataSet FetchMainJobCardData(string jobCardPK = "0")
        {

            StringBuilder strSQL = new StringBuilder();

            strSQL.Append( " SELECT");
            strSQL.Append( "    job_imp.job_card_sea_imp_pk,");
            strSQL.Append( "    job_imp.jobcard_ref_no,");
            strSQL.Append( "    job_imp.jobcard_date,");
            strSQL.Append( "    job_imp.job_card_status,");
            strSQL.Append( "    job_imp.job_card_closed_on,");
            strSQL.Append( "    job_imp.WIN_XML_STATUS,");
            strSQL.Append( "    job_imp.WIN_TOTAL_QTY,");
            strSQL.Append( "    job_imp.WIN_REC_QTY,");
            strSQL.Append( "    job_imp.WIN_BALANCE_QTY,");
            strSQL.Append( "    job_imp.WIN_TOTAL_WT,");
            strSQL.Append( "    job_imp.WIN_REC_WT,");
            strSQL.Append( "    job_imp.WIN_BALANCE_WT,");
            strSQL.Append( "    job_imp.remarks,");
            strSQL.Append( "    job_imp.cargo_type,");
            strSQL.Append( "    job_imp.cust_customer_mst_fk,");
            //strSQL.Append(vbCrLf & "    cust.customer_id ""customer_id"",")
            //strSQL.Append(vbCrLf & "    cust.customer_name ""customer_name"",")
            strSQL.Append( "     NVL((SELECT CMT.CUSTOMER_ID  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append( "     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.CUST_CUSTOMER_MST_FK),");
            strSQL.Append( "      (SELECT TMT.CUSTOMER_ID FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append( "      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.CUST_CUSTOMER_MST_FK)) \"customer_id\",");
            strSQL.Append( "     NVL((SELECT CMT.CUSTOMER_NAME  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append( "     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.CUST_CUSTOMER_MST_FK),");
            strSQL.Append( "      (SELECT TMT.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append( "      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.CUST_CUSTOMER_MST_FK)) \"customer_name\",");
            strSQL.Append( "    job_imp.del_place_mst_fk,");
            strSQL.Append( "    upper(del_place.place_name) \"DeliveryPlace\",");
            strSQL.Append( "    job_imp.port_mst_pol_fk,");
            strSQL.Append( "    pol.port_id \"POL\",");
            strSQL.Append( "    job_imp.port_mst_pod_fk,");
            strSQL.Append( "    pod.port_id \"POD\",");
            strSQL.Append( "    job_imp.operator_mst_fk,");
            strSQL.Append( "    oprator.operator_id \"operator_id\",");
            strSQL.Append( "    oprator.operator_name \"operator_name\",");
            strSQL.Append( "    VVT.VOYAGE_TRN_PK \"VoyagePK\",");
            strSQL.Append( "    V.VESSEL_ID \"vessel_id\",   ");
            strSQL.Append( "    V.VESSEL_NAME \"VESSEL_NAME\",");
            strSQL.Append( "    VVT.VOYAGE \"VOYAGE\",");
            //strSQL.Append(vbCrLf & "    job_imp.eta_date,")
            //strSQL.Append(vbCrLf & "    job_imp.etd_date,")
            //strSQL.Append(vbCrLf & "    job_imp.arrival_date,")
            //strSQL.Append(vbCrLf & "    job_imp.departure_date,")
            strSQL.Append( "    TO_CHAR(job_imp.eta_date,DATETIMEFORMAT24)eta_date , ");
            strSQL.Append( "    TO_CHAR(job_imp.etd_date,DATETIMEFORMAT24) etd_date, ");
            strSQL.Append( "    TO_CHAR(job_imp.arrival_date,DATETIMEFORMAT24) arrival_date, ");
            strSQL.Append( "    TO_CHAR(job_imp.departure_date,DATETIMEFORMAT24) departure_date, ");

            strSQL.Append( "    job_imp.shipper_cust_mst_fk,");
            //strSQL.Append(vbCrLf & "    shipper.customer_id ""Shipper"",")
            // strSQL.Append(vbCrLf & "    shipper.customer_name ""ShipperName"",")
            strSQL.Append( "     NVL((SELECT CMT.CUSTOMER_ID  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append( "     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.SHIPPER_CUST_MST_FK),");
            strSQL.Append( "      (SELECT TMT.CUSTOMER_ID FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append( "      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.SHIPPER_CUST_MST_FK)) \"Shipper\",");
            strSQL.Append( "     NVL((SELECT CMT.CUSTOMER_NAME  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append( "     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.SHIPPER_CUST_MST_FK),");
            strSQL.Append( "      (SELECT TMT.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append( "      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.SHIPPER_CUST_MST_FK)) \"ShipperName\",");
            strSQL.Append( "    job_imp.consignee_cust_mst_fk,");
            //strSQL.Append(vbCrLf & "    consignee.customer_id ""Consignee"",")
            //strSQL.Append(vbCrLf & "    consignee.customer_name ""ConsigneeName"",")
            strSQL.Append( "     NVL((SELECT CMT.CUSTOMER_ID  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append( "     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.CONSIGNEE_CUST_MST_FK),");
            strSQL.Append( "      (SELECT TMT.CUSTOMER_ID FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append( "      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.CONSIGNEE_CUST_MST_FK)) \"Consignee\",");
            strSQL.Append( "     NVL((SELECT CMT.CUSTOMER_NAME  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append( "     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.CONSIGNEE_CUST_MST_FK),");
            strSQL.Append( "      (SELECT TMT.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append( "      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.CONSIGNEE_CUST_MST_FK)) \"ConsigneeName\",");
            strSQL.Append( "    notify1_cust_mst_fk,");
            // strSQL.Append(vbCrLf & "    notify1.customer_id ""Notify1"",")
            //strSQL.Append(vbCrLf & "    notify1.customer_name ""Notify1Name"",")
            strSQL.Append( "     NVL((SELECT CMT.CUSTOMER_ID  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append( "     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.NOTIFY1_CUST_MST_FK),");
            strSQL.Append( "      (SELECT TMT.CUSTOMER_ID FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append( "      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.NOTIFY1_CUST_MST_FK)) \"Notify1\",");
            strSQL.Append( "     NVL((SELECT CMT.CUSTOMER_NAME  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append( "     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.NOTIFY1_CUST_MST_FK),");
            strSQL.Append( "      (SELECT TMT.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append( "      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.NOTIFY1_CUST_MST_FK)) \"Notify1Name\",");

            strSQL.Append( "    notify2_cust_mst_fk,");
            strSQL.Append( "    notify2.customer_id \"Notify2\",");
            strSQL.Append( "    notify2.customer_name \"Notify2Name\",");
            strSQL.Append( "    job_imp.cb_agent_mst_fk,");
            strSQL.Append( "    cbagnt.agent_id \"cbAgent\",");
            strSQL.Append( "    cbagnt.agent_name \"cbAgentName\",");
            strSQL.Append( "    job_imp.cl_agent_mst_fk,");
            strSQL.Append( "    clagnt.agent_id \"clAgent\",");
            strSQL.Append( "    clagnt.agent_name \"clAgentName\",");
            strSQL.Append( "    job_imp.version_no,");
            strSQL.Append( "    job_imp.ucr_no,");
            strSQL.Append( "    job_imp.goods_description,");
            strSQL.Append( "    job_imp.del_address,");
            strSQL.Append( "    job_imp.hbl_ref_no,");
            strSQL.Append( "    job_imp.hbl_date,");
            strSQL.Append( "    job_imp.mbl_ref_no,");
            strSQL.Append( "    job_imp.mbl_date,");
            strSQL.Append( "    job_imp.marks_numbers,");
            strSQL.Append( "    job_imp.weight_mass,");
            strSQL.Append( "    job_imp.cargo_move_fk,");
            strSQL.Append( "    job_imp.pymt_type,");
            strSQL.Append( "    job_imp.shipping_terms_mst_fk,");
            strSQL.Append( "    job_imp.insurance_amt,");
            strSQL.Append( "    job_imp.insurance_currency,");
            strSQL.Append( "    job_imp.pol_agent_mst_fk,");
            strSQL.Append( "    polagnt.agent_id \"polAgent\",");
            strSQL.Append( "    polagnt.agent_name \"polAgentName\", ");
            strSQL.Append( "    job_imp.commodity_group_fk,");
            strSQL.Append( "    comm.commodity_group_desc,");
            //fields are added after the UAT.
            strSQL.Append( "    depot.vendor_id \"depot_id\",");
            strSQL.Append( "    depot.vendor_name \"depot_name\",");
            strSQL.Append( "    depot.vendor_mst_pk \"depot_pk\",");
            strSQL.Append( "    carrier.vendor_id \"carrier_id\",");
            strSQL.Append( "    carrier.vendor_name \"carrier_name\",");
            strSQL.Append( "    carrier.vendor_mst_pk \"carrier_pk\",");
            strSQL.Append( "    country.country_id \"country_id\",");
            strSQL.Append( "    country.country_name \"country_name\",");
            strSQL.Append( "    country.country_mst_pk \"country_mst_pk\",");
            strSQL.Append( "    job_imp.da_number \"da_number\",");
            // strSQL.Append(vbCrLf & "    job_imp.hbl_exp_tbl_fk, ")
            //strSQL.Append(vbCrLf & "    job_imp.mbl_exp_tbl_fk, ")
            strSQL.Append( "    job_imp.clearance_address, ");
            //' strSQL.Append(vbCrLf & "    job_imp.JC_AUTO_MANUAL ")
            //adding by thiyagarajan on 26/3/09
            strSQL.Append( "    job_imp.JC_AUTO_MANUAL,job_imp.HBL_SURRENDERED HBLSURR,job_imp.MBL_SURRENDERED MBLSURR,");
            strSQL.Append( "    job_imp.MBLSURRDT,job_imp.HBLSURRDT, ");
            //adding by thiyagarajan on 16/4/09
            //end

            //Code Added By Anil on 17 Aug 09
            strSQL.Append( "    job_imp.sb_number,job_imp.sb_date, ");
            //End By Anil

            //added by surya prasad for introducing base currency.
            strSQL.Append( "   curr.currency_id,");
            strSQL.Append( "   curr.currency_mst_pk base_currency_fk,");
            //end
            //'
            strSQL.Append( "  job_imp.LC_SHIPMENT,");
            strSQL.Append( "  job_imp.Lc_Number,");
            strSQL.Append( "  job_imp.Lc_Date,");
            strSQL.Append( "  job_imp.Lc_Expires_On,");
            strSQL.Append( "  job_imp.Lc_Cons_Bank,");
            strSQL.Append( "  job_imp.Lc_Remarks,");
            strSQL.Append( "  job_imp.Bro_Received,");
            strSQL.Append( "  job_imp.Bro_Number,");
            strSQL.Append( "  job_imp.Bro_Date,");
            strSQL.Append( "  job_imp.Bro_Issuedby,");
            strSQL.Append( "  job_imp.Bro_Remarks, ");
            strSQL.Append( "  job_imp.CRQ_DATE,");
            strSQL.Append( "  job_imp.CRQ,");
            strSQL.Append( "  job_imp.PO_NUMBER,");
            strSQL.Append( "  job_imp.PO_DATE,");
            strSQL.Append( "  job_imp.ROUTING_INST,");
            strSQL.Append( "  job_imp.PO_SEND_ON_DATE,");
            strSQL.Append( "  job_imp.DO_STATUS, ");
            strSQL.Append( "    NVL(job_imp.CHK_CSR,1) CHK_CSR,");
            //strSQL.Append(vbCrLf & "    NVL(EMP.EMPLOYEE_MST_PK,NVL(CONS_SE.EMPLOYEE_MST_PK,0)) SALES_EXEC_FK,")
            //strSQL.Append(vbCrLf & "    NVL(EMP.EMPLOYEE_ID,CONS_SE.EMPLOYEE_ID) SALES_EXEC_ID,")
            //strSQL.Append(vbCrLf & "    NVL(EMP.EMPLOYEE_NAME,CONS_SE.EMPLOYEE_NAME) SALES_EXEC_NAME, ")
            strSQL.Append( "    NVL(EMP.EMPLOYEE_MST_PK,(SELECT NVL(CONS_SE.EMPLOYEE_MST_PK,0) FROM EMPLOYEE_MST_TBL CONS_SE,CUSTOMER_MST_TBL CONSIG ");
            strSQL.Append( "    WHERE CONS_SE.EMPLOYEE_MST_PK(+)= CONSIG.REP_EMP_MST_FK ");
            strSQL.Append( "  AND JOB_IMP.CUST_CUSTOMER_MST_FK = CONSIG.CUSTOMER_MST_PK)) SALES_EXEC_FK,");
            strSQL.Append( "    NVL(EMP.EMPLOYEE_ID,(SELECT NVL(CONS_SE.EMPLOYEE_ID,0) FROM EMPLOYEE_MST_TBL CONS_SE,CUSTOMER_MST_TBL CONSIG ");
            strSQL.Append( "    WHERE CONS_SE.EMPLOYEE_MST_PK(+)= CONSIG.REP_EMP_MST_FK ");
            strSQL.Append( "  AND JOB_IMP.CUST_CUSTOMER_MST_FK = CONSIG.CUSTOMER_MST_PK)) SALES_EXEC_ID,");
            strSQL.Append( "    NVL(EMP.EMPLOYEE_NAME,(SELECT NVL(CONS_SE.EMPLOYEE_NAME,0) FROM EMPLOYEE_MST_TBL CONS_SE,CUSTOMER_MST_TBL CONSIG ");
            strSQL.Append( "    WHERE CONS_SE.EMPLOYEE_MST_PK(+)= CONSIG.REP_EMP_MST_FK ");
            strSQL.Append( "  AND JOB_IMP.CUST_CUSTOMER_MST_FK = CONSIG.CUSTOMER_MST_PK)) SALES_EXEC_NAME,");

            strSQL.Append( "    NVL(job_imp.chk_can,0) chk_can, NVL(job_imp.chk_do,0) chk_do,NVL(job_imp.chk_rec,0) chk_rec, NVL(job_imp.chk_pay,0) chk_pay,");
            strSQL.Append( "    NVL(job_imp.cc_req,0) cc_req,NVL(job_imp.cc_ie,0) cc_ie,job_imp.LINE_BKG_NR,job_imp.LINE_BKG_DT,job_imp.LINER_TERMS_FK,job_imp.ONC_FK,job_imp.ONC_MOVE_FK,");
            strSQL.Append( "    job_imp.cha_agent_mst_fk, ");
            strSQL.Append( "    chaagnt.VENDOR_ID \"CHAAgentID\",");
            strSQL.Append( "    chaagnt.VENDOR_NAME \"CHAAgentName\",");
            strSQL.Append( "    job_imp.WIN_UNIQ_REF_ID, ");
            strSQL.Append( "    job_imp.WIN_SEND_USER_NAME,");
            strSQL.Append( "    job_imp.WIN_SEND_SECRET_KEY, ");
            strSQL.Append( "    job_imp.WIN_MEM_JOBREF_NR, ");
            //strSQL.Append(vbCrLf & "    job_imp.LINER_TERMS_FK,")
            strSQL.Append( "    job_imp.RFS_DATE,");
            strSQL.Append( "    job_imp.RFS,");
            strSQL.Append( "    job_imp.WIN_QUOT_REF_NR,");
            strSQL.Append( "    job_imp.WIN_INCO_PLACE,");
            strSQL.Append( "    job_imp.POO_FK, ");
            strSQL.Append( "    job_imp.WIN_CONSOL_REF_NR,");
            strSQL.Append( "    job_imp.WIN_PICK_ONC_MOVE_FK,");
            strSQL.Append( "    job_imp.WIN_XML_STATUS,");
            strSQL.Append( "    job_imp.WIN_CUTTOFF_DT,");
            strSQL.Append( "    job_imp.WIN_CUTTOFF_TIME");
            strSQL.Append( "    FROM ");
            strSQL.Append( "    job_card_sea_imp_tbl job_imp,");
            strSQL.Append( "    port_mst_tbl POD,");
            strSQL.Append( "    port_mst_tbl POL,");
            //strSQL.Append(vbCrLf & "    customer_mst_tbl cust,")
            // strSQL.Append(vbCrLf & "    customer_mst_tbl consignee,")
            //strSQL.Append(vbCrLf & "    customer_mst_tbl shipper,")
            //strSQL.Append(vbCrLf & "    customer_mst_tbl notify1,")
            strSQL.Append( "    customer_mst_tbl notify2,");
            strSQL.Append( "    place_mst_tbl del_place,");
            strSQL.Append( "    operator_mst_tbl oprator,");
            strSQL.Append( "    agent_mst_tbl clagnt, ");
            strSQL.Append( "    agent_mst_tbl cbagnt,");
            strSQL.Append( "    agent_mst_tbl polagnt,");
            strSQL.Append( "    commodity_group_mst_tbl comm, ");
            strSQL.Append( "    VESSEL_VOYAGE_TBL V, ");
            strSQL.Append( "    VESSEL_VOYAGE_TRN VVT, ");
            strSQL.Append( "    vendor_mst_tbl  depot,");
            strSQL.Append( "    vendor_mst_tbl  carrier,");
            strSQL.Append( "    country_mst_tbl country,");
            strSQL.Append( "    currency_type_mst_tbl curr,");
            strSQL.Append( "    EMPLOYEE_MST_TBL        EMP, ");
            //strSQL.Append(vbCrLf & "    EMPLOYEE_MST_TBL        CONS_SE ,") 'CONSIGNEE SALES PERSON
            strSQL.Append( "    VENDOR_MST_TBL chaagnt ");
            strSQL.Append( "    WHERE");
            strSQL.Append( "    job_imp.job_card_sea_imp_pk          = " + jobCardPK);
            strSQL.Append( "    AND job_imp.port_mst_pol_fk          =  pol.port_mst_pk");
            strSQL.Append( "    AND job_imp.port_mst_pod_fk          =  pod.port_mst_pk");
            strSQL.Append( "    AND job_imp.del_place_mst_fk         =  del_place.place_pk(+)");
            //strSQL.Append(vbCrLf & "    AND job_imp.cust_customer_mst_fk     =  cust.customer_mst_pk(+) ")
            strSQL.Append( "    AND job_imp.operator_mst_fk          =  oprator.operator_mst_pk");
            //strSQL.Append(vbCrLf & "    AND job_imp.shipper_cust_mst_fk      =  shipper.customer_mst_pk(+)")
            // strSQL.Append(vbCrLf & "    AND job_imp.consignee_cust_mst_fk    =  consignee.customer_mst_pk(+)")
            //strSQL.Append(vbCrLf & "    AND job_imp.notify1_cust_mst_fk      =  notify1.customer_mst_pk(+)")
            strSQL.Append( "    AND job_imp.Notify2_Cust_Mst_Fk      =  notify2.customer_mst_pk(+)");
            strSQL.Append( "    AND job_imp.cl_agent_mst_fk          =  clagnt.agent_mst_pk(+)");
            strSQL.Append( "    AND job_imp.Cb_Agent_Mst_Fk          =  cbagnt.agent_mst_pk(+)");
            strSQL.Append( "    AND job_imp.pol_agent_mst_fk         =  polagnt.agent_mst_pk(+)");
            strSQL.Append( "    AND job_imp.commodity_group_fk       =  comm.commodity_group_pk(+)");
            strSQL.Append( "    AND job_imp.cha_agent_mst_fk         =  chaagnt.VENDOR_MST_PK(+)");

            //conditions are added after the UAT.
            strSQL.Append( "    AND job_imp.transporter_depot_fk     =  depot.vendor_mst_pk(+)");
            strSQL.Append( "    AND job_imp.transporter_carrier_fk   =  carrier.vendor_mst_pk(+)");
            strSQL.Append( "    AND job_imp.country_origin_fk        =  country.country_mst_pk(+)");
            strSQL.Append( "    AND VVT.VESSEL_VOYAGE_TBL_FK         = V.VESSEL_VOYAGE_TBL_PK(+)");
            strSQL.Append( "    AND JOB_IMP.VOYAGE_TRN_FK            = VVT.VOYAGE_TRN_PK(+)");

            //strSQL.Append(vbCrLf & "    AND consignee.REP_EMP_MST_FK=CONS_SE.EMPLOYEE_MST_PK(+) ")
            strSQL.Append( "    AND JOB_IMP.EXECUTIVE_MST_FK=EMP.EMPLOYEE_MST_PK(+) ");
            //added by surya prasad for introducing base currency.
            strSQL.Append( "     and curr.currency_mst_pk(+) = job_imp.BASE_CURRENCY_MST_FK");
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
        //add by latha for fetching the login location id
        public string fetchlocationid()
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();
            SQL.Append( "SELECT ");
            SQL.Append( "      l.location_id ");
            SQL.Append( "      from location_mst_tbl l ");
            SQL.Append( "      where l.location_mst_pk = " + LoggedIn_Loc_FK);
            try
            {
                return objWF.ExecuteScaler(SQL.ToString());
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

        #region "Get container data"
        public DataSet GetContainerData(string jobCardPK = "0")
        {

            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append( "SELECT ");
            SQL.Append( "      CONT_TRN.JOB_TRN_CONT_PK,");
            SQL.Append( "    CG.COMMODITY_GROUP_PK container_number,");
            SQL.Append( "    CG.COMMODITY_GROUP_CODE  container_type_mst_fk,");
            SQL.Append( "  commodity_name,NVL(PACK_TYPE_MST_FK,0) PACK_TYPE_MST_FK,DM.DIMENTION_ID gross_weight, ");
            SQL.Append( " pack_count,NVL(CHARGEABLE_WEIGHT,0) CHARGEABLE_WEIGHT,volume_in_cbm, ");
            SQL.Append( "  seal_number, NVL(NET_WEIGHT,0) NET_WEIGHT,' ' fetch_comm,");
            SQL.Append( "      TO_CHAR(cont_trn.load_date,DATETIMEFORMAT24) gen_land_date,");
            SQL.Append( "      container_type_mst_id,commodity_mst_fk,NVL(DM.DIMENTION_UNIT_MST_PK,COMMODITY_MST_FK) COMMODITY_MST_FKS ");

            SQL.Append( "     ");
            //added By prakash chandra on 6/1/2009 for implementing multiple commodities
            SQL.Append( "FROM");
            SQL.Append( "      job_trn_cont cont_trn,");
            SQL.Append( "      JOB_CARD_TRN job_card,");
            SQL.Append( "      container_type_mst_tbl cont,");
            SQL.Append( "      pack_type_mst_tbl pack,DIMENTION_UNIT_MST_TBL DM,");
            SQL.Append( "      commodity_mst_tbl comm,COMMODITY_GROUP_MST_TBL CG");
            SQL.Append( "WHERE");
            SQL.Append( "      cont_trn.job_card_trn_fk =" + jobCardPK);
            SQL.Append( "      AND cont_trn.job_card_trn_fk = job_card.JOB_CARD_TRN_PK");
            SQL.Append( "      AND DM.DIMENTION_UNIT_MST_PK(+)=CONT_TRN.BASIS_FK");
            SQL.Append( "      AND cont_trn.container_type_mst_fk = cont.container_type_mst_pk(+)");
            SQL.Append( "      AND cont_trn.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
            SQL.Append( "      AND cont_trn.commodity_mst_fk = comm.commodity_mst_pk(+)");
            SQL.Append( "      AND CG.COMMODITY_GROUP_PK=COMM.COMMODITY_GROUP_FK");

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
        #endregion

        #region "Get Freight data"
        public DataSet GetFreightData(string jobCardPK = "0", string BaseCurrFk = "0")
        {

            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append("SELECT JOB_TRN_FD_PK,");
            sb.Append("       CMT.COMMODITY_NAME CONTAINER_TYPE_MST_FK,");
            sb.Append("       FRT.FREIGHT_ELEMENT_ID,");
            sb.Append("       FRT.FREIGHT_ELEMENT_NAME,");
            sb.Append("       FRT.FREIGHT_ELEMENT_MST_PK,");
            sb.Append("       FD_TRN.BASIS,");
            sb.Append("       JCT.PACK_COUNT QUANTITY,");
            sb.Append("       DECODE(FD_TRN.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') FREIGHT_TYPE,");
            sb.Append("       FD_TRN.LOCATION_MST_FK \"location_fk\",");
            sb.Append("       LOC.LOCATION_ID \"location_id\",");
            sb.Append("       FD_TRN.FRTPAYER_CUST_MST_FK \"frtpayer_mst_fk\",");
            sb.Append("       CUS.CUSTOMER_ID \"frtpayer\",");
            sb.Append("       CURR.CURRENCY_ID CURRENCY_MST_FK,");
            sb.Append("       ABS(FD_TRN.RATEPERBASIS)RATEPERBASIS,");
            sb.Append("       FD_TRN.FREIGHT_AMT,");
            if (Convert.ToInt32(BaseCurrFk) != 0)
            {
                sb.Append("       ROUND(GET_EX_RATE(FD_TRN.CURRENCY_MST_FK, " + BaseCurrFk + ", round(TO_DATE(JOB_CARD.JOBCARD_DATE,DATEFORMAT) - .5)), 4) AS ROE,");
                sb.Append("       (FD_TRN.FREIGHT_AMT * ROUND(GET_EX_RATE(FD_TRN.CURRENCY_MST_FK, " + BaseCurrFk + ", round(TO_DATE(JOB_CARD.JOBCARD_DATE,DATEFORMAT) - .5)), 4)) TOTAL,");
            }
            else
            {
                sb.Append("       FD_TRN.EXCHANGE_RATE ROE,");
                sb.Append("       (FD_TRN.FREIGHT_AMT * FD_TRN.EXCHANGE_RATE) TOTAL,");
            }

            sb.Append("       '0' \"Delete\",");
            sb.Append("       CMT.COMMODITY_MST_PK,");
            sb.Append("       JCT.JOB_TRN_CONT_PK,");
            sb.Append("       CURR.CURRENCY_MST_PK CURR_FK,");
            sb.Append("       JCT.CHARGEABLE_WEIGHT,");
            sb.Append("       JCT.VOLUME_IN_CBM,");
            sb.Append("       DU.DIMENTION_ID,FRT.CREDIT");
            sb.Append("  FROM JOB_TRN_FD      FD_TRN,");
            sb.Append("       JOB_TRN_CONT    JCT,");
            sb.Append("       COMMODITY_MST_TBL       CMT,");
            sb.Append("       JOB_CARD_TRN    JOB_CARD,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CURR,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FRT,");
            sb.Append("       PARAMETERS_TBL          PRM,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CONT,");
            sb.Append("       LOCATION_MST_TBL        LOC,");
            sb.Append("       CUSTOMER_MST_TBL        CUS,");
            sb.Append("       DIMENTION_UNIT_MST_TBL DU");
            sb.Append("   WHERE FD_TRN.JOB_CARD_TRN_FK = " + jobCardPK);
            sb.Append("   AND FD_TRN.JOB_CARD_TRN_FK = JOB_CARD.JOB_CARD_TRN_PK");
            sb.Append("   AND DU.DIMENTION_UNIT_MST_PK(+)=JCT.BASIS_FK");
            sb.Append("   AND JCT.JOB_TRN_CONT_PK= FD_TRN.JOB_TRN_CONT_FK");
            sb.Append("   AND CMT.COMMODITY_MST_PK = JCT.COMMODITY_MST_FK");
            sb.Append("   AND FD_TRN.CONTAINER_TYPE_MST_FK = CONT.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND FD_TRN.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)");
            sb.Append("   AND FD_TRN.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("   AND FD_TRN.FRTPAYER_CUST_MST_FK = CUS.CUSTOMER_MST_PK(+)");
            sb.Append("   AND FD_TRN.LOCATION_MST_FK = LOC.LOCATION_MST_PK(+)");
            sb.Append("   AND NVL(FD_TRN.SERVICE_TYPE_FLAG,0)<>1");
            sb.Append(" ORDER BY FRT.PREFERENCE");
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
        public DataSet GetFrtData(string jobCardPK = "0")
        {

            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();
            SQL.Append( "SELECT");
            SQL.Append( "   fd_trn.inv_cust_trn_sea_imp_fk,");
            SQL.Append( "   fd_trn.inv_agent_trn_sea_imp_fk,");
            SQL.Append( "   fd_trn.consol_invoice_trn_fk");
            SQL.Append( "FROM");
            SQL.Append( "   job_trn_sea_imp_fd fd_trn,");
            SQL.Append( "   job_card_sea_imp_tbl job_card,");
            SQL.Append( "   currency_type_mst_tbl curr,");
            SQL.Append( "   freight_element_mst_tbl frt,");
            SQL.Append( "   container_type_mst_tbl cont,");
            SQL.Append( "   location_mst_tbl loc,");
            SQL.Append( "   customer_mst_tbl cus");
            SQL.Append( "WHERE");
            SQL.Append( "   fd_trn.job_card_sea_imp_fk = " + jobCardPK);
            SQL.Append( "   AND fd_trn.job_card_sea_imp_fk = job_card.job_card_sea_imp_pk");
            SQL.Append( "   AND fd_trn.container_type_mst_fk = cont.container_type_mst_pk(+)");
            SQL.Append( "   AND fd_trn.currency_mst_fk = curr.currency_mst_pk(+)");
            SQL.Append( "   AND fd_trn.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            SQL.Append( "   AND fd_trn.frtpayer_cust_mst_fk = cus.customer_mst_pk(+)");
            SQL.Append( "   AND fd_trn.location_mst_fk = loc.location_mst_pk(+)");
            SQL.Append("   AND NVL(FD_TRN.SERVICE_TYPE_FLAG,0)<>1");
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
        #endregion

        #region "Get transshipment details"
        public DataSet GetTransshipmentData(string jobCardPK = "0")
        {

            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append( "SELECT");
            SQL.Append( "   tp_trn.job_trn_sea_imp_tp_pk,");
            SQL.Append( "   tp_trn.transhipment_no,");
            SQL.Append( "   tp_trn.port_mst_fk,");
            SQL.Append( "   port.port_id,");
            SQL.Append( "   port.port_name,");
            SQL.Append( "   DECODE(tp_trn.WIN_TYPE,1,'Main Carriage',2,'On Carriage') WIN_TYPE,");
            SQL.Append( "   (SELECT QDT.DD_ID FROM QFOR_DROP_DOWN_TBL QDT ");
            SQL.Append( "   WHERE QDT.DD_FLAG = 'Carriage_Mode_IMP_POD' ");
            SQL.Append( "   AND QDT.DROPDOWN_PK = tp_trn.WIN_MODE) WIN_MODE,");
            SQL.Append( "   tp_trn.vessel_name,");
            SQL.Append( "   tp_trn.voyage,");
            SQL.Append( "   TO_CHAR(tp_trn.eta_date,'" + dateFormat + "') eta_date,");
            SQL.Append( "   TO_CHAR(tp_trn.etd_date,'" + dateFormat + "') etd_date,");
            SQL.Append( "   '0' \"Delete\",");
            SQL.Append( "   tp_trn.voyage_trn_fk \"GridVoyagePK\" ");
            SQL.Append( "FROM");
            SQL.Append( "   job_trn_sea_imp_tp tp_trn,");
            SQL.Append( "   job_card_sea_imp_tbl job_card,");
            SQL.Append( "   vessel_voyage_trn vvt,");
            SQL.Append( "   port_mst_tbl port");
            SQL.Append( "WHERE");
            SQL.Append( "   tp_trn.job_card_sea_imp_fk =" + jobCardPK);
            SQL.Append( "   AND tp_trn.job_card_sea_imp_fk = job_card.job_card_sea_imp_pk");
            SQL.Append( "   AND tp_trn.port_mst_fk = port.port_mst_pk(+)");
            SQL.Append( "   and tp_trn.voyage_trn_fk = vvt.voyage_trn_pk(+)");

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
        #endregion

        #region "Fetch PIA details"
        public DataSet GetPIAData(string jobCardPK = "0")
        {

            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append( "SELECT");
            SQL.Append( "   pia_trn.job_trn_sea_imp_pia_pk,");
            SQL.Append( "   pia_trn.vendor_key,");
            SQL.Append( "   cost_element_id,");
            SQL.Append( "   pia_trn.invoice_number,");
            SQL.Append( "   to_char(pia_trn.invoice_date, dateformat) invoice_date,");
            SQL.Append( "   pia_trn.currency_mst_fk,");
            SQL.Append( "   pia_trn.invoice_amt,");
            SQL.Append( "   pia_trn.tax_percentage,");
            SQL.Append( "   pia_trn.tax_amt,");
            SQL.Append( "   pia_trn.estimated_amt,");
            SQL.Append( "   invoice_amt - estimated_amt diff_amount,");
            SQL.Append( "   pia_trn.vendor_mst_fk, ");
            SQL.Append( "   pia_trn.cost_element_mst_fk,");
            SQL.Append( "   pia_trn.attached_file_name,'false' as \"Delete\", MJC_TRN_SEA_IMP_PIA_FK");
            SQL.Append( "FROM");
            SQL.Append( "   job_trn_sea_imp_pia pia_trn,");
            SQL.Append( "   cost_element_mst_tbl cost_elmnt,");
            SQL.Append( "   currency_type_mst_tbl curr,");
            SQL.Append( "   job_card_sea_imp_tbl job_card");
            SQL.Append( "WHERE");
            SQL.Append( "   pia_trn.job_card_sea_imp_fk =" + jobCardPK);
            SQL.Append( "   AND pia_trn.job_card_sea_imp_fk = job_card.job_card_sea_imp_pk");
            SQL.Append( "   AND pia_trn.cost_element_mst_fk = cost_elmnt.cost_element_mst_pk(+)");
            SQL.Append( "   AND pia_trn.currency_mst_fk     = curr.currency_mst_pk(+)");

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
        public DataSet GetPIA(string jobCardPK = "0")
        {

            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append( "SELECT");
            SQL.Append( "   pia_trn.invoice_sea_tbl_fk,");
            SQL.Append( "   pia_trn.inv_agent_trn_sea_imp_fk,");
            SQL.Append( "   pia_trn.inv_supplier_fk");
            SQL.Append( "FROM");
            SQL.Append( "   job_trn_sea_imp_pia pia_trn,");
            SQL.Append( "   cost_element_mst_tbl cost_elmnt,");
            SQL.Append( "   currency_type_mst_tbl curr,");
            SQL.Append( "   job_card_sea_imp_tbl job_card");
            SQL.Append( "WHERE");
            SQL.Append( "   pia_trn.job_card_sea_imp_fk =" + jobCardPK);
            SQL.Append( "   AND pia_trn.job_card_sea_imp_fk = job_card.job_card_sea_imp_pk");
            SQL.Append( "   AND pia_trn.cost_element_mst_fk = cost_elmnt.cost_element_mst_pk(+)");
            SQL.Append( "   AND pia_trn.currency_mst_fk     = curr.currency_mst_pk(+)");

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
        #endregion

        #region " Fetch Cost details data export"
        public DataSet FetchCostDetail(string jobCardPK = "0", int basecurrency = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append("SELECT JEC.JOB_TRN_SEA_IMP_COST_PK,");
                strSQL.Append("       JEC.JOB_CARD_SEA_IMP_FK,");
                strSQL.Append("       VMT.VENDOR_MST_PK,");
                strSQL.Append("       CMT.COST_ELEMENT_MST_PK,");
                strSQL.Append("       JEC.VENDOR_KEY,");
                strSQL.Append("       CMT.COST_ELEMENT_ID,");
                strSQL.Append("       CMT.COST_ELEMENT_NAME,");
                strSQL.Append("       DECODE(JEC.PTMT_TYPE,1,'Prepaid',2,'Collect') PTMT_TYPE,");
                strSQL.Append("       LMT.LOCATION_ID,");
                strSQL.Append("       CURR.CURRENCY_ID,");
                strSQL.Append("       JEC.ESTIMATED_COST,");
                strSQL.Append("       ROUND(GET_EX_RATE_BUY(JEC.CURRENCY_MST_FK, " + basecurrency + ", round(TO_DATE(JOB.JOBCARD_DATE,DATEFORMAT) - .5)), 4) AS ROE,");
                strSQL.Append("       JEC.TOTAL_COST,");
                strSQL.Append("       ''DEL_FLAG,");
                strSQL.Append("       ''SEL_FLAG,");
                strSQL.Append("       JEC.LOCATION_MST_FK,");
                strSQL.Append("       JEC.CURRENCY_MST_FK");
                strSQL.Append("  FROM JOB_TRN_SEA_IMP_COST  JEC,");
                strSQL.Append("       JOB_CARD_SEA_IMP_TBL  JOB,");
                strSQL.Append("       VENDOR_MST_TBL        VMT,");
                strSQL.Append("       COST_ELEMENT_MST_TBL  CMT,");
                strSQL.Append("       CURRENCY_TYPE_MST_TBL CURR,");
                strSQL.Append("       LOCATION_MST_TBL      LMT");
                strSQL.Append(" WHERE JEC.JOB_CARD_SEA_IMP_FK = JOB.JOB_CARD_SEA_IMP_PK");
                strSQL.Append("   AND JEC.COST_ELEMENT_MST_FK = CMT.COST_ELEMENT_MST_PK");
                strSQL.Append("   AND JEC.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
                strSQL.Append("   AND JEC.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                strSQL.Append("   AND JEC.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                strSQL.Append("   AND JOB.JOB_CARD_SEA_IMP_PK = " + jobCardPK);
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
        public DataSet FetchCostDet(int jobcardpk)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT JOBCOST.JOB_TRN_SEA_IMP_COST_PK");
                sb.Append("  FROM JOB_CARD_SEA_IMP_TBL JOB,");
                sb.Append("       JOB_TRN_SEA_IMP_COST JOBCOST,");
                sb.Append("       INV_SUPPLIER_TBL     INV,");
                sb.Append("       INV_SUPPLIER_TRN_TBL INVTRN");
                sb.Append(" WHERE JOB.JOB_CARD_SEA_IMP_PK = JOBCOST.JOB_CARD_SEA_IMP_FK");
                sb.Append("   AND JOB.JOB_CARD_SEA_IMP_PK = " + jobcardpk);
                sb.Append("   AND INVTRN.JOB_TRN_EST_FK = JOBCOST.JOB_TRN_SEA_IMP_COST_PK");
                sb.Append("   AND INV.INV_SUPPLIER_PK = INVTRN.INV_SUPPLIER_TBL_FK");
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
        #endregion

        #endregion

        // By Amit to Ftech the Vessel/Voyage Detail
        // On 29-March-07 for EFS must have
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
        // End

        // By Amit to Fetch Agent Detail & JCType
        // On 04-April-07 for 
        #region "To Fetch Agent Detail"
        public DataSet Get_Agent_Detail(int JobCardPK)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append(" SELECT AMST.AGENT_MST_PK," );
                strQuery.Append(" AMST.AGENT_ID," );
                strQuery.Append(" AMST.AGENT_NAME" );
                strQuery.Append(" FROM AGENT_MST_TBL AMST," );
                strQuery.Append(" JOB_CARD_SEA_EXP_TBL EJOB," );
                strQuery.Append(" JOB_CARD_SEA_IMP_TBL IJOB," );
                strQuery.Append(" USER_MST_TBL UMST" );
                strQuery.Append(" WHERE EJOB.JOBCARD_REF_NO = IJOB.JOBCARD_REF_NO" );
                strQuery.Append(" AND AMST.LOCATION_MST_FK=UMST.DEFAULT_LOCATION_FK" );
                strQuery.Append("  AND EJOB.CREATED_BY_FK = UMST.USER_MST_PK" );
                //strQuery.Append(" AND AMST.LOCATION_AGENT = 1" & vbCrLf)
                strQuery.Append(" AND IJOB.JOB_CARD_SEA_IMP_PK = '" + JobCardPK + "'");
                return objWF.GetDataSet(strQuery.ToString());
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

        public DataSet Get_JC_Type(int JobCardPK)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append(" SELECT JC.JC_AUTO_MANUAL " );
                strQuery.Append(" FROM JOB_CARD_SEA_IMP_TBL JC" );
                strQuery.Append(" WHERE JC.JOB_CARD_SEA_IMP_PK = '" + JobCardPK + "'");
                return objWF.GetDataSet(strQuery.ToString());
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
        // End

        #region "Save Function"

        public ArrayList Save(ref DataSet M_DataSet, ref DataSet dsContainerData, ref DataSet dsTPDetails, ref DataSet dsFreightDetails, ref DataSet dsPurchaseInventory, ref DataSet dsCostDetails, ref DataSet dsPickUpDetails, ref DataSet dsDropDetails, ref bool Update, ref bool isEdting,
        ref object ucrNo, ref string jobCardRefNumber, ref string userLocation, ref string employeeID, ref long JobCardPK, ref DataSet dsOtherCharges, string PODetails = "", DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null, DataSet dsDoc = null,
        int IsWINSave = 0, DataSet DSShipper = null, DataSet DSCons = null, DataSet DSNotify = null)
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
            int ShipperPK = 0;
            int ConsigneePK = 0;
            int NotifyPK = 0;
            // To save in Vessel/Voyage
            //
            if (Update == true)
            {
                strVoyagepk = 0;
            }
            if (strVoyagepk == 0 & !string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["VESSEL_ID"].ToString()))
            {
                //strVoyagepk = 0
                TRAN1 = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = TRAN1;
                objWK.MyCommand.Connection = objWK.MyConnection;

                arrMessage = objVesselVoyage.SaveVesselMaster(strVoyagepk,
                    Convert.ToString(getDefault(M_DataSet.Tables[0].Rows[0]["VESSEL_NAME"], "")),
                    Convert.ToInt32(getDefault(M_DataSet.Tables[0].Rows[0]["OPERATOR_MST_FK"], 0)),
                    Convert.ToString(getDefault(M_DataSet.Tables[0].Rows[0]["VESSEL_ID"], "")),
                    Convert.ToString(getDefault(M_DataSet.Tables[0].Rows[0]["VOYAGE"], "")), 
                    objWK.MyCommand,
                    Convert.ToInt32(getDefault(M_DataSet.Tables[0].Rows[0]["PORT_MST_POL_FK"], 0)), 
                    Convert.ToString(M_DataSet.Tables[0].Rows[0]["PORT_MST_POD_FK"]), 
                    DateTime.MinValue,
                    Convert.ToDateTime(getDefault(Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["ETD_DATE"], DateTime.MinValue)), null)),
                    DateTime.MinValue,
                    Convert.ToDateTime(getDefault(Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["ETA_DATE"], DateTime.MinValue)), null)),
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
                    TRAN1.Commit();
                    arrMessage.Clear();
                }
            }
            ///

            TRAN = objWK.MyConnection.BeginTransaction();
            
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            OracleCommand insContainerDetails = new OracleCommand();
            OracleCommand updContainerDetails = new OracleCommand();

            OracleCommand insPickUpDetails = new OracleCommand();
            OracleCommand updPickUpDetails = new OracleCommand();

            OracleCommand insDropDetails = new OracleCommand();
            OracleCommand updDropDetails = new OracleCommand();

            OracleCommand insTPDetails = new OracleCommand();
            OracleCommand updTPDetails = new OracleCommand();
            OracleCommand delTPDetails = new OracleCommand();

            OracleCommand insFreightDetails = new OracleCommand();
            OracleCommand updFreightDetails = new OracleCommand();
            OracleCommand delFreightDetails = new OracleCommand();

            OracleCommand insPurchaseInvDetails = new OracleCommand();
            OracleCommand updPurchaseInvDetails = new OracleCommand();
            OracleCommand delPurchaseInvDetails = new OracleCommand();

            OracleCommand insCostDetails = new OracleCommand();
            //'Added By Koteshwari 
            OracleCommand updCostDetails = new OracleCommand();
            OracleCommand delCostDetails = new OracleCommand();

            //Dim insIncomeChargeDetails As New OracleCommand
            //Dim updIncomeChargeDetails As New OracleCommand
            //Dim delIncomeChargeDetails As New OracleCommand

            //Dim insExpenseChargeDetails As New OracleCommand
            //Dim updExpenseChargeDetails As New OracleCommand
            //Dim delExpenseChargeDetails As New OracleCommand

            OracleCommand insOtherChargesDetails = new OracleCommand();
            OracleCommand updOtherChargesDetails = new OracleCommand();
            OracleCommand delOtherChargesDetails = new OracleCommand();
            DataSet dsTrackNtrace = new DataSet();

            if (isEdting == false)
            {
                jobCardRefNumber = GenerateProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(userLocation), Convert.ToInt32(employeeID), System.DateTime.Now,"" ,"" ,"" , M_LAST_MODIFIED_BY_FK);
            }

            // ucrNo = ucrNo & jobCardRefNumber

            try
            {
                //'For Customer Save
                if (IsWINSave == 1)
                {
                    if ((DSShipper != null))
                    {
                        if (!(SaveWINCustomerTemp(ref objWK, ref TRAN, ref DSShipper, ref ShipperPK, 1)))
                        {
                            arrMessage.Add("Error while saving Customer");
                            return arrMessage;
                        }
                    }
                    if ((DSCons != null))
                    {
                        if (!(SaveWINCustomerTemp(ref objWK, ref TRAN, ref DSCons, ref ConsigneePK, 2)))
                        {
                            arrMessage.Add("Error while saving Customer");
                            return arrMessage;
                        }
                    }
                    if ((DSNotify != null))
                    {
                        if (!(SaveWINCustomerTemp(ref objWK, ref TRAN, ref DSNotify, ref NotifyPK, 3)))
                        {
                            arrMessage.Add("Error while saving Customer");
                            return arrMessage;
                        }
                    }
                }
                //'End

                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;

                dsTrackNtrace = dsContainerData.Copy();
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_CARD_SEA_IMP_TBL_INS";
                var _with2 = _with1.Parameters;

                insCommand.Parameters.Add("JOBCARD_REF_NO_IN", jobCardRefNumber).Direction = ParameterDirection.Input;

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

                insCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "cargo_type").Direction = ParameterDirection.Input;
                insCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                if (ConsigneePK > 0 & IsWINSave == 1)
                {
                    insCommand.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", ConsigneePK).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "cust_customer_mst_fk").Direction = ParameterDirection.Input;
                    insCommand.Parameters["CUST_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                }

                insCommand.Parameters.Add("DEL_PLACE_MST_FK_IN", OracleDbType.Int32, 10, "del_place_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200, "del_address").Direction = ParameterDirection.Input;
                insCommand.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PORT_MST_POL_FK_IN", OracleDbType.Int32, 10, "port_mst_pol_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PORT_MST_POD_FK_IN", OracleDbType.Int32, 10, "port_mst_pod_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("OPERATOR_MST_FK_IN", OracleDbType.Int32, 10, "operator_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "vessel_name").Direction = ParameterDirection.Input;
                insCommand.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VOYAGE_IN", OracleDbType.Varchar2, 10, "voyage").Direction = ParameterDirection.Input;
                insCommand.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters.Add("VOYAGE_FK_IN", OracleDbType.Varchar2, 10, "VoyagePK").Direction = ParameterDirection.Input
                insCommand.Parameters.Add("VOYAGE_FK_IN", getDefault(strVoyagepk, DBNull.Value)).Direction = ParameterDirection.Input;
                insCommand.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input
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

                //insCommand.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input
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

                //insCommand.Parameters.Add("ARRIVAL_DATE_IN", OracleDbType.Date, 20, "arrival_date").Direction = ParameterDirection.Input
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

                //insCommand.Parameters.Add("DEPARTURE_DATE_IN", OracleDbType.Date, 20, "departure_date").Direction = ParameterDirection.Input
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

                if (ShipperPK > 0 & IsWINSave == 1)
                {
                    insCommand.Parameters.Add("SHIPPER_CUST_MST_FK_IN", ShipperPK).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("SHIPPER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "shipper_cust_mst_fk").Direction = ParameterDirection.Input;
                    insCommand.Parameters["SHIPPER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                }

                if (ConsigneePK > 0 & IsWINSave == 1)
                {
                    insCommand.Parameters.Add("CONSIGNEE_CUST_MST_FK_IN", ConsigneePK).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("CONSIGNEE_CUST_MST_FK_IN", OracleDbType.Int32, 10, "consignee_cust_mst_fk").Direction = ParameterDirection.Input;
                    insCommand.Parameters["CONSIGNEE_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                }

                if (NotifyPK > 0 & IsWINSave == 1)
                {
                    insCommand.Parameters.Add("NOTIFY1_CUST_MST_FK_IN", NotifyPK).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("NOTIFY1_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify1_cust_mst_fk").Direction = ParameterDirection.Input;
                    insCommand.Parameters["NOTIFY1_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                }

                insCommand.Parameters.Add("NOTIFY2_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify2_cust_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["NOTIFY2_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CB_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cb_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CB_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CL_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cl_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MARKS_NUMBERS_IN", OracleDbType.Varchar2, 2000, "marks_numbers").Direction = ParameterDirection.Input;
                insCommand.Parameters["MARKS_NUMBERS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("GOODS_DESCRIPTION_IN", OracleDbType.Varchar2, 4000, "goods_description").Direction = ParameterDirection.Input;
                insCommand.Parameters["GOODS_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WEIGHT_MASS_IN", OracleDbType.Varchar2, 2000, "weight_mass").Direction = ParameterDirection.Input;
                insCommand.Parameters["WEIGHT_MASS_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters.Add("UCR_NO_IN", ucrNo).Direction = ParameterDirection.Input

                insCommand.Parameters.Add("UCR_NO_IN", OracleDbType.Varchar2, 40, "UCR_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["UCR_NO_IN"].SourceVersion = DataRowVersion.Current;


                insCommand.Parameters.Add("HBL_REF_NO_IN", OracleDbType.Varchar2, 20, "hbl_ref_no").Direction = ParameterDirection.Input;
                insCommand.Parameters["HBL_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("HBL_DATE_IN", OracleDbType.Date, 20, "hbl_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["HBL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MBL_REF_NO_IN", OracleDbType.Varchar2, 20, "mbl_ref_no").Direction = ParameterDirection.Input;
                insCommand.Parameters["MBL_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MBL_DATE_IN", OracleDbType.Date, 20, "mbl_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["MBL_DATE_IN"].SourceVersion = DataRowVersion.Current;

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

                insCommand.Parameters.Add("POL_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "pol_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["POL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TRANSPORTER_DEPOT_FK_IN", OracleDbType.Int32, 10, "depot_pk").Direction = ParameterDirection.Input;
                insCommand.Parameters["TRANSPORTER_DEPOT_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("TRANSPORTER_CARRIER_FK_IN", OracleDbType.Int32, 10, "carrier_pk").Direction = ParameterDirection.Input;
                insCommand.Parameters["TRANSPORTER_CARRIER_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("COUNTRY_ORIGIN_FK_IN", OracleDbType.Int32, 10, "country_mst_pk").Direction = ParameterDirection.Input;
                insCommand.Parameters["COUNTRY_ORIGIN_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("DA_NUMBER_IN", OracleDbType.Varchar2, 20, "da_number").Direction = ParameterDirection.Input;
                insCommand.Parameters["DA_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "commodity_group_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CLEARANCE_ADDRESS_IN", OracleDbType.Varchar2, 200, "clearance_address").Direction = ParameterDirection.Input;
                insCommand.Parameters["CLEARANCE_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_PK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                //adding by thiyagarajan on 26/3/09
                insCommand.Parameters.Add("HBLSURR_IN", OracleDbType.Int32, 1, "HBLSURR").Direction = ParameterDirection.Input;
                insCommand.Parameters["HBLSURR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MBLSURR_IN", OracleDbType.Int32, 1, "MBLSURR").Direction = ParameterDirection.Input;
                insCommand.Parameters["MBLSURR_IN"].SourceVersion = DataRowVersion.Current;
                //end

                //adding by thiyagarajan on 16/4/09
                insCommand.Parameters.Add("HBLSURRDT_IN", OracleDbType.Date, 20, "HBLSURRDT").Direction = ParameterDirection.Input;
                insCommand.Parameters["HBLSURRDT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MBLSURRDT_IN", OracleDbType.Date, 20, "MBLSURRDT").Direction = ParameterDirection.Input;
                insCommand.Parameters["MBLSURRDT_IN"].SourceVersion = DataRowVersion.Current;
                //end
                //ADDED BY SURYA PRASAD FOR INTRODUCING CURRENCY
                insCommand.Parameters.Add("BASE_CURRENCY_FK_IN", OracleDbType.Int32, 10, "base_currency_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["BASE_CURRENCY_FK_IN"].SourceVersion = DataRowVersion.Current;
                //END

                //Code Added By Anil on 17 Aug 09
                insCommand.Parameters.Add("SB_DATE_IN", OracleDbType.Date, 20, "sb_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["SB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("SB_NO_IN", OracleDbType.Varchar2, 20, "sb_number").Direction = ParameterDirection.Input;
                insCommand.Parameters["SB_NO_IN"].SourceVersion = DataRowVersion.Current;
                //End By Anil
                //'
                insCommand.Parameters.Add("LC_SHIPMENT_IN", OracleDbType.Int32, 1, "LC_SHIPMENT").Direction = ParameterDirection.Input;
                insCommand.Parameters["LC_SHIPMENT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("LC_NUMBER_IN", OracleDbType.Varchar2, 20, "Lc_Number").Direction = ParameterDirection.Input;
                insCommand.Parameters["LC_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("LC_DATE_IN", OracleDbType.Date, 20, "Lc_Date").Direction = ParameterDirection.Input;
                insCommand.Parameters["LC_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("LC_EXPIRES_ON_IN", OracleDbType.Date, 20, "Lc_Expires_On").Direction = ParameterDirection.Input;
                insCommand.Parameters["LC_EXPIRES_ON_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("LC_CONS_BANK_IN", OracleDbType.Varchar2, 20, "Lc_Cons_Bank").Direction = ParameterDirection.Input;
                insCommand.Parameters["LC_CONS_BANK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("LC_REMARKS_IN", OracleDbType.Varchar2, 20, "Lc_Remarks").Direction = ParameterDirection.Input;
                insCommand.Parameters["LC_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("BRO_RECEIVED_IN", OracleDbType.Int32, 1, "Bro_Received").Direction = ParameterDirection.Input;
                insCommand.Parameters["BRO_RECEIVED_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("BRO_NUMBER_IN", OracleDbType.Varchar2, 20, "Bro_Number").Direction = ParameterDirection.Input;
                insCommand.Parameters["BRO_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("BRO_DATE_IN", OracleDbType.Date, 20, "Bro_Date").Direction = ParameterDirection.Input;
                insCommand.Parameters["BRO_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("BRO_ISSUEDBY_IN", OracleDbType.Varchar2, 20, "Bro_Issuedby").Direction = ParameterDirection.Input;
                insCommand.Parameters["BRO_ISSUEDBY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("BRO_REMARKS_IN", OracleDbType.Varchar2, 20, "Bro_Remarks").Direction = ParameterDirection.Input;
                insCommand.Parameters["BRO_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CRQ_Date"].ToString()))
                {
                    insCommand.Parameters.Add("CRQ_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("CRQ_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["CRQ_Date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["CRQ_DATE_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CRQ_IN", OracleDbType.Int32, 1, "CRQ").Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CHK_CAN_IN", OracleDbType.Int32, 1, "chk_can").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_CAN_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CHK_DO_IN", OracleDbType.Int32, 1, "chk_do").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_DO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CHK_PAY_IN", OracleDbType.Int32, 1, "chk_pay").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_PAY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CHK_REC_IN", OracleDbType.Int32, 1, "chk_rec").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_REC_IN"].SourceVersion = DataRowVersion.Current;
                //'
                if (PODetails.Length > 0)
                {
                    string[] PO_Details = null;
                    PO_Details = PODetails.Split('~');

                    insCommand.Parameters.Add("PO_NUMBER_IN", (string.IsNullOrEmpty(PO_Details[0]) ? "" : PO_Details[0])).Direction = ParameterDirection.Input;
                    insCommand.Parameters["PO_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("PO_DATE_IN", (string.IsNullOrEmpty(PO_Details[1]) ? "" : PO_Details[1])).Direction = ParameterDirection.Input;
                    insCommand.Parameters["PO_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("ROUTING_INST_IN", (string.IsNullOrEmpty(PO_Details[2]) ? "" : PO_Details[2])).Direction = ParameterDirection.Input;
                    insCommand.Parameters["ROUTING_INST_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("SENT_ON_IN", (string.IsNullOrEmpty(PO_Details[3]) ? "" : PO_Details[3])).Direction = ParameterDirection.Input;
                    insCommand.Parameters["SENT_ON_IN"].SourceVersion = DataRowVersion.Current;

                }
                else
                {
                    insCommand.Parameters.Add("PO_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    insCommand.Parameters["PO_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("PO_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    insCommand.Parameters["PO_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("ROUTING_INST_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    insCommand.Parameters["ROUTING_INST_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("SENT_ON_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    insCommand.Parameters["SENT_ON_IN"].SourceVersion = DataRowVersion.Current;
                }

                //nomination parameters
                insCommand.Parameters.Add("CHK_CSR_IN", OracleDbType.Int32, 1, "CHK_CSR").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_CSR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("EXECUTIVE_MST_FK_IN", OracleDbType.Int32, 10, "SALES_EXEC_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["EXECUTIVE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //--------------------------------------------------------------------------------
                insCommand.Parameters.Add("CC_REQ_IN", OracleDbType.Int32, 1, "cc_req").Direction = ParameterDirection.Input;
                insCommand.Parameters["CC_REQ_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CC_IE_IN", OracleDbType.Int32, 1, "cc_ie").Direction = ParameterDirection.Input;
                insCommand.Parameters["CC_IE_IN"].SourceVersion = DataRowVersion.Current;


                //Added by Raghavendra


                insCommand.Parameters.Add("LINE_BKG_NR_IN", OracleDbType.Varchar2, 50, "LINE_BKG_NR").Direction = ParameterDirection.Input;
                insCommand.Parameters["LINE_BKG_NR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("LINE_BKG_DT_IN", OracleDbType.Date, 20, "LINE_BKG_DT").Direction = ParameterDirection.Input;
                insCommand.Parameters["LINE_BKG_DT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("LINER_TERMS_FK_IN", OracleDbType.Int32, 10, "LINER_TERMS_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["LINER_TERMS_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ONC_FK_IN", OracleDbType.Int32, 10, "ONC_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["ONC_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ONC_MOVE_FK_IN", OracleDbType.Int32, 10, "ONC_MOVE_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["ONC_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;
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
                //'WIN Intigration Fields
                insCommand.Parameters.Add("WIN_UNIQ_REF_ID_IN", OracleDbType.Varchar2, 20, "WIN_UNIQ_REF_ID").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_UNIQ_REF_ID_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_SEND_USER_NAME_IN", OracleDbType.Varchar2, 50, "WIN_SEND_USER_NAME").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_SEND_USER_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_SEND_SECRET_KEY_IN", OracleDbType.Varchar2, 12, "WIN_SEND_SECRET_KEY").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_SEND_SECRET_KEY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_MEM_JOBREF_NR_IN", OracleDbType.Varchar2, 50, "WIN_MEM_JOBREF_NR").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_MEM_JOBREF_NR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RFS_DATE_IN", OracleDbType.Date, 20, "RFS_DATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["RFS_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RFS_IN", OracleDbType.Int32, 1, "RFS").Direction = ParameterDirection.Input;
                insCommand.Parameters["RFS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_QUOT_REF_NR_IN", OracleDbType.Varchar2, 20, "WIN_QUOT_REF_NR").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_QUOT_REF_NR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_INCO_PLACE_IN", OracleDbType.Varchar2, 50, "WIN_INCO_PLACE").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_INCO_PLACE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("POO_FK_IN", OracleDbType.Int32, 10, "POO_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["POO_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_PICK_ONC_MOVE_FK_IN", OracleDbType.Int32, 10, "WIN_PICK_ONC_MOVE_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_PICK_ONC_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_CONSOL_REF_NR_IN", OracleDbType.Varchar2, 20, "WIN_CONSOL_REF_NR").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_CONSOL_REF_NR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_CUTTOFF_DT_IN", OracleDbType.Date, 20, "WIN_CUTTOFF_DT").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_CUTTOFF_DT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_CUTTOFF_TIME_IN", OracleDbType.Date, 20, "WIN_CUTTOFF_TIME").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_CUTTOFF_TIME_IN"].SourceVersion = DataRowVersion.Current;

                //'End
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_CARD_SEA_IMP_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;



                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_CARD_SEA_IMP_TBL_UPD";
                var _with4 = _with3.Parameters;

                updCommand.Parameters.Add("JOB_CARD_SEA_IMP_PK_IN", OracleDbType.Int32, 10, "job_card_sea_imp_pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_SEA_IMP_PK_IN"].SourceVersion = DataRowVersion.Current;

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

                updCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "cargo_type").Direction = ParameterDirection.Input;
                updCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                if (ConsigneePK > 0 & IsWINSave == 1)
                {
                    updCommand.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", ConsigneePK).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "cust_customer_mst_fk").Direction = ParameterDirection.Input;
                    updCommand.Parameters["CUST_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                }

                updCommand.Parameters.Add("DEL_PLACE_MST_FK_IN", OracleDbType.Int32, 10, "del_place_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200, "del_address").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PORT_MST_POL_FK_IN", OracleDbType.Int32, 10, "port_mst_pol_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PORT_MST_POD_FK_IN", OracleDbType.Int32, 10, "port_mst_pod_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("OPERATOR_MST_FK_IN", OracleDbType.Int32, 10, "operator_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

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

                if (ShipperPK > 0 & IsWINSave == 1)
                {
                    updCommand.Parameters.Add("SHIPPER_CUST_MST_FK_IN", ShipperPK).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("SHIPPER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "shipper_cust_mst_fk").Direction = ParameterDirection.Input;
                    updCommand.Parameters["SHIPPER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                }

                if (ConsigneePK > 0 & IsWINSave == 1)
                {
                    updCommand.Parameters.Add("CONSIGNEE_CUST_MST_FK_IN", ConsigneePK).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("CONSIGNEE_CUST_MST_FK_IN", OracleDbType.Int32, 10, "consignee_cust_mst_fk").Direction = ParameterDirection.Input;
                    updCommand.Parameters["CONSIGNEE_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                }

                if (NotifyPK > 0 & IsWINSave == 1)
                {
                    updCommand.Parameters.Add("NOTIFY1_CUST_MST_FK_IN", NotifyPK).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("NOTIFY1_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify1_cust_mst_fk").Direction = ParameterDirection.Input;
                    updCommand.Parameters["NOTIFY1_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                }

                updCommand.Parameters.Add("NOTIFY2_CUST_MST_FK_IN", OracleDbType.Int32, 10, "notify2_cust_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["NOTIFY2_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CB_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cb_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CB_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CL_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cl_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MARKS_NUMBERS_IN", OracleDbType.Varchar2, 2000, "marks_numbers").Direction = ParameterDirection.Input;
                updCommand.Parameters["MARKS_NUMBERS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("GOODS_DESCRIPTION_IN", OracleDbType.Varchar2, 4000, "goods_description").Direction = ParameterDirection.Input;
                updCommand.Parameters["GOODS_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WEIGHT_MASS_IN", OracleDbType.Varchar2, 2000, "weight_mass").Direction = ParameterDirection.Input;
                updCommand.Parameters["WEIGHT_MASS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("UCR_NO_IN", OracleDbType.Varchar2, 40, "ucr_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["UCR_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("HBL_REF_NO_IN", OracleDbType.Varchar2, 20, "hbl_ref_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["HBL_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("HBL_DATE_IN", OracleDbType.Date, 20, "hbl_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["HBL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MBL_REF_NO_IN", OracleDbType.Varchar2, 20, "mbl_ref_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["MBL_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MBL_DATE_IN", OracleDbType.Date, 20, "mbl_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["MBL_DATE_IN"].SourceVersion = DataRowVersion.Current;

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

                updCommand.Parameters.Add("POL_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "pol_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["POL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TRANSPORTER_DEPOT_FK_IN", OracleDbType.Int32, 10, "depot_pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["TRANSPORTER_DEPOT_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("TRANSPORTER_CARRIER_FK_IN", OracleDbType.Int32, 10, "carrier_pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["TRANSPORTER_CARRIER_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COUNTRY_ORIGIN_FK_IN", OracleDbType.Int32, 10, "country_mst_pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["COUNTRY_ORIGIN_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DA_NUMBER_IN", OracleDbType.Varchar2, 20, "da_number").Direction = ParameterDirection.Input;
                updCommand.Parameters["DA_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "commodity_group_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CLEARANCE_ADDRESS_IN", OracleDbType.Varchar2, 200, "clearance_address").Direction = ParameterDirection.Input;
                updCommand.Parameters["CLEARANCE_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                //adding by thiyagarajan on 26/3/09
                updCommand.Parameters.Add("HBLSURR_IN", OracleDbType.Int32, 1, "HBLSURR").Direction = ParameterDirection.Input;
                updCommand.Parameters["HBLSURR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MBLSURR_IN", OracleDbType.Int32, 1, "MBLSURR").Direction = ParameterDirection.Input;
                updCommand.Parameters["MBLSURR_IN"].SourceVersion = DataRowVersion.Current;
                //end

                //adding by thiyagarajan on 16/4/09
                updCommand.Parameters.Add("HBLSURRDT_IN", OracleDbType.Date, 20, "HBLSURRDT").Direction = ParameterDirection.Input;
                updCommand.Parameters["HBLSURRDT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MBLSURRDT_IN", OracleDbType.Date, 20, "MBLSURRDT").Direction = ParameterDirection.Input;
                updCommand.Parameters["MBLSURRDT_IN"].SourceVersion = DataRowVersion.Current;
                //end

                //Code Added By Anil on 17 Aug 09
                updCommand.Parameters.Add("SB_DATE_IN", OracleDbType.Date, 20, "sb_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["SB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SB_NO_IN", OracleDbType.Varchar2, 20, "sb_number").Direction = ParameterDirection.Input;
                updCommand.Parameters["SB_NO_IN"].SourceVersion = DataRowVersion.Current;
                //End By Anil
                //'
                updCommand.Parameters.Add("LC_SHIPMENT_IN", OracleDbType.Int32, 1, "LC_SHIPMENT").Direction = ParameterDirection.Input;
                updCommand.Parameters["LC_SHIPMENT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LC_NUMBER_IN", OracleDbType.Varchar2, 20, "Lc_Number").Direction = ParameterDirection.Input;
                updCommand.Parameters["LC_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LC_DATE_IN", OracleDbType.Date, 20, "Lc_Date").Direction = ParameterDirection.Input;
                updCommand.Parameters["LC_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LC_EXPIRES_ON_IN", OracleDbType.Date, 20, "Lc_Expires_On").Direction = ParameterDirection.Input;
                updCommand.Parameters["LC_EXPIRES_ON_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LC_CONS_BANK_IN", OracleDbType.Varchar2, 20, "Lc_Cons_Bank").Direction = ParameterDirection.Input;
                updCommand.Parameters["LC_CONS_BANK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LC_REMARKS_IN", OracleDbType.Varchar2, 20, "Lc_Remarks").Direction = ParameterDirection.Input;
                updCommand.Parameters["LC_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BRO_RECEIVED_IN", OracleDbType.Int32, 1, "Bro_Received").Direction = ParameterDirection.Input;
                updCommand.Parameters["BRO_RECEIVED_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BRO_NUMBER_IN", OracleDbType.Varchar2, 20, "Bro_Number").Direction = ParameterDirection.Input;
                updCommand.Parameters["BRO_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BRO_DATE_IN", OracleDbType.Date, 20, "Bro_Date").Direction = ParameterDirection.Input;
                updCommand.Parameters["BRO_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BRO_ISSUEDBY_IN", OracleDbType.Varchar2, 20, "Bro_Issuedby").Direction = ParameterDirection.Input;
                updCommand.Parameters["BRO_ISSUEDBY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BRO_REMARKS_IN", OracleDbType.Varchar2, 20, "Bro_Remarks").Direction = ParameterDirection.Input;
                updCommand.Parameters["BRO_REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CRQ_Date"].ToString()))
                {
                    updCommand.Parameters.Add("CRQ_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("CRQ_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["CRQ_Date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["CRQ_DATE_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CRQ_IN", OracleDbType.Int32, 1, "CRQ").Direction = ParameterDirection.Input;
                //'
                updCommand.Parameters.Add("CHK_CAN_IN", OracleDbType.Int32, 1, "chk_can").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHK_CAN_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CHK_DO_IN", OracleDbType.Int32, 1, "chk_do").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHK_DO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CHK_PAY_IN", OracleDbType.Int32, 1, "chk_pay").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHK_PAY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CHK_REC_IN", OracleDbType.Int32, 1, "chk_rec").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHK_REC_IN"].SourceVersion = DataRowVersion.Current;
                //'
                if (PODetails.Length > 0)
                {
                    string[] PO_Details = null;
                    PO_Details = PODetails.Split('~');
                    updCommand.Parameters.Add("PO_NUMBER_IN", (string.IsNullOrEmpty(PO_Details[0]) ? "" : PO_Details[0])).Direction = ParameterDirection.Input;
                    updCommand.Parameters["PO_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    updCommand.Parameters.Add("PO_DATE_IN", (string.IsNullOrEmpty(PO_Details[1]) ? "" : PO_Details[1])).Direction = ParameterDirection.Input;
                    updCommand.Parameters["PO_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updCommand.Parameters.Add("ROUTING_INST_IN", (string.IsNullOrEmpty(PO_Details[2]) ? "" : PO_Details[2])).Direction = ParameterDirection.Input;
                    updCommand.Parameters["ROUTING_INST_IN"].SourceVersion = DataRowVersion.Current;

                    updCommand.Parameters.Add("SENT_ON_IN", (string.IsNullOrEmpty(PO_Details[3]) ? "" : PO_Details[3])).Direction = ParameterDirection.Input;
                    updCommand.Parameters["SENT_ON_IN"].SourceVersion = DataRowVersion.Current;
                }
                else
                {
                    updCommand.Parameters.Add("PO_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    updCommand.Parameters["PO_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    updCommand.Parameters.Add("PO_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    updCommand.Parameters["PO_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updCommand.Parameters.Add("ROUTING_INST_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    updCommand.Parameters["ROUTING_INST_IN"].SourceVersion = DataRowVersion.Current;

                    updCommand.Parameters.Add("SENT_ON_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    updCommand.Parameters["SENT_ON_IN"].SourceVersion = DataRowVersion.Current;
                }

                //nomination parameters
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

                updCommand.Parameters.Add("LINE_BKG_NR_IN", OracleDbType.Varchar2, 50, "LINE_BKG_NR").Direction = ParameterDirection.Input;
                updCommand.Parameters["LINE_BKG_NR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LINE_BKG_DT_IN", OracleDbType.Date, 20, "LINE_BKG_DT").Direction = ParameterDirection.Input;
                updCommand.Parameters["LINE_BKG_DT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LINER_TERMS_FK_IN", OracleDbType.Int32, 10, "LINER_TERMS_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["LINER_TERMS_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ONC_FK_IN", OracleDbType.Int32, 10, "ONC_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["ONC_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ONC_MOVE_FK_IN", OracleDbType.Int32, 10, "ONC_MOVE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["ONC_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;
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
                //'WIN Intigration Fields
                updCommand.Parameters.Add("WIN_UNIQ_REF_ID_IN", OracleDbType.Varchar2, 20, "WIN_UNIQ_REF_ID").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_UNIQ_REF_ID_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_SEND_USER_NAME_IN", OracleDbType.Varchar2, 50, "WIN_SEND_USER_NAME").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_SEND_USER_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_SEND_SECRET_KEY_IN", OracleDbType.Varchar2, 12, "WIN_SEND_SECRET_KEY").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_SEND_SECRET_KEY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_MEM_JOBREF_NR_IN", OracleDbType.Varchar2, 50, "WIN_MEM_JOBREF_NR").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_MEM_JOBREF_NR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RFS_DATE_IN", OracleDbType.Date, 20, "RFS_DATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["RFS_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RFS_IN", OracleDbType.Int32, 1, "RFS").Direction = ParameterDirection.Input;
                updCommand.Parameters["RFS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_QUOT_REF_NR_IN", OracleDbType.Varchar2, 20, "WIN_QUOT_REF_NR").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_QUOT_REF_NR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_INCO_PLACE_IN", OracleDbType.Varchar2, 50, "WIN_INCO_PLACE").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_INCO_PLACE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("POO_FK_IN", OracleDbType.Int32, 10, "POO_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["POO_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_PICK_ONC_MOVE_FK_IN", OracleDbType.Int32, 10, "WIN_PICK_ONC_MOVE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_PICK_ONC_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_CONSOL_REF_NR_IN", OracleDbType.Varchar2, 20, "WIN_CONSOL_REF_NR").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_CONSOL_REF_NR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_CUTTOFF_DT_IN", OracleDbType.Date, 20, "WIN_CUTTOFF_DT").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_CUTTOFF_DT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_CUTTOFF_TIME_IN", OracleDbType.Date, 20, "WIN_CUTTOFF_TIME").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_CUTTOFF_TIME_IN"].SourceVersion = DataRowVersion.Current;

                //'End
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                

                var _with5 = objWK.MyDataAdapter;

                _with5.InsertCommand = insCommand;
                _with5.InsertCommand.Transaction = TRAN;

                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;

                //RecAfct = .Update(M_DataSet)
                RecAfct = _with5.Update(M_DataSet.Tables[0]);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    if (isEdting == false)
                    {
                        RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]),Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                        //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    }
                    return arrMessage;
                }
                else
                {
                    if (isEdting == false)
                    {
                        JobCardPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                        HttpContext.Current.Session.Add("JobCardPK", JobCardPK);
                    }
                }


                if ((dsContainerData != null))
                {
                    var _with6 = insContainerDetails;
                    _with6.Connection = objWK.MyConnection;
                    _with6.CommandType = CommandType.StoredProcedure;
                    _with6.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_CONT_INS";
                    var _with7 = _with6.Parameters;

                    insContainerDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    //insContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 16, "container_number").Direction = ParameterDirection.Input
                    //insContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current
                    insContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    //insContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input
                    //insContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                    insContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    //insContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "seal_number").Direction = ParameterDirection.Input
                    //insContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current
                    insContainerDetails.Parameters.Add("SEAL_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    insContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "volume_in_cbm").Direction = ParameterDirection.Input;
                    insContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                    //insContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input
                    //insContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current
                    insContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    //insContainerDetails.Parameters.Add("NET_WEIGHT_IN", OracleDbType.Int32, 10, "net_weight").Direction = ParameterDirection.Input
                    //insContainerDetails.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current
                    insContainerDetails.Parameters.Add("NET_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    insContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "chargeable_weight").Direction = ParameterDirection.Input;
                    insContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    insContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "pack_type_mst_fk").Direction = ParameterDirection.Input;
                    insContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "pack_count").Direction = ParameterDirection.Input;
                    insContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                    insContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "commodity_mst_fk").Direction = ParameterDirection.Input;
                    insContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    //Added By Prakash chandra on 6/1/2009 for pts: multiple commodity selection 

                    insContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Int32, 10, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                    insContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;
                    //insContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", DBNull.Value).Direction = ParameterDirection.Input

                    insContainerDetails.Parameters.Add("GEN_LAND_DATE_IN", OracleDbType.Date, 20, "gen_land_date").Direction = ParameterDirection.Input;
                    insContainerDetails.Parameters["GEN_LAND_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    insContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_CARD_SEA_IMP_CONT_PK").Direction = ParameterDirection.Output;
                    insContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;




                    var _with8 = updContainerDetails;
                    _with8.Connection = objWK.MyConnection;
                    _with8.CommandType = CommandType.StoredProcedure;
                    _with8.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_CONT_UPD";
                    var _with9 = _with8.Parameters;

                    updContainerDetails.Parameters.Add("JOB_TRN_SEA_IMP_CONT_PK_IN", OracleDbType.Int32, 10, "job_card_sea_imp_cont_pk").Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["JOB_TRN_SEA_IMP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    //updContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 16, "container_number").Direction = ParameterDirection.Input
                    //updContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current
                    updContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    //updContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input
                    //updContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                    updContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    //updContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "seal_number").Direction = ParameterDirection.Input
                    //updContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current
                    updContainerDetails.Parameters.Add("SEAL_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    updContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "volume_in_cbm").Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                    //updContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input
                    //updContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current
                    updContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    //updContainerDetails.Parameters.Add("NET_WEIGHT_IN", OracleDbType.Int32, 10, "net_weight").Direction = ParameterDirection.Input
                    //updContainerDetails.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current
                    updContainerDetails.Parameters.Add("NET_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    updContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "chargeable_weight").Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "pack_type_mst_fk").Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "pack_count").Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "commodity_mst_fk").Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    //Added By Prakash chandra on 6/1/2009 for pts: multiple commodity selection 

                    updContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Int32, 10, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;
                    //updContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", DBNull.Value).Direction = ParameterDirection.Input

                    updContainerDetails.Parameters.Add("GEN_LAND_DATE_IN", OracleDbType.Date, 20, "gen_land_date").Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["GEN_LAND_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                    var _with10 = objWK.MyDataAdapter;

                    _with10.InsertCommand = insContainerDetails;
                    _with10.InsertCommand.Transaction = TRAN;

                    _with10.UpdateCommand = updContainerDetails;
                    _with10.UpdateCommand.Transaction = TRAN;

                    RecAfct = _with10.Update(dsContainerData.Tables[0]);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        if (isEdting == false)
                        {
                            RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        return arrMessage;
                    }
                }

                //Manjunath for Cargo Pick up & Drop Address
                if ((dsPickUpDetails != null))
                {
                    var _with11 = insPickUpDetails;
                    _with11.Connection = objWK.MyConnection;
                    _with11.CommandType = CommandType.StoredProcedure;
                    _with11.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_INS";

                    _with11.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with11.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with11.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with11.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with11.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with11.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with11.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with11.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with11.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with11.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with11.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with11.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with11.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with11.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with11.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with11.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with11.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with11.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with11.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with11.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Output;
                    _with11.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                    var _with12 = updPickUpDetails;
                    _with12.Connection = objWK.MyConnection;
                    _with12.CommandType = CommandType.StoredProcedure;
                    _with12.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_UPD";

                    _with12.Parameters.Add("PICK_DROP_MST_PK_IN", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Input;
                    _with12.Parameters["PICK_DROP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with12.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with12.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with12.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with12.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with12.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with12.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with12.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with12.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with12.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with12.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with12.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with12.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with12.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with12.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with12.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with12.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with12.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with12.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with12.Parameters.Add("MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with12.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with12.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with13 = objWK.MyDataAdapter;

                    _with13.InsertCommand = insPickUpDetails;
                    _with13.InsertCommand.Transaction = TRAN;

                    _with13.UpdateCommand = updPickUpDetails;
                    _with13.UpdateCommand.Transaction = TRAN;

                    RecAfct = _with13.Update(dsPickUpDetails);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }

                if ((dsDropDetails != null))
                {
                    var _with14 = insDropDetails;
                    _with14.Connection = objWK.MyConnection;
                    _with14.CommandType = CommandType.StoredProcedure;
                    _with14.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_INS";

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

                    _with14.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with14.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Output;
                    _with14.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                    var _with15 = updDropDetails;
                    _with15.Connection = objWK.MyConnection;
                    _with15.CommandType = CommandType.StoredProcedure;
                    _with15.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_UPD";

                    _with15.Parameters.Add("PICK_DROP_MST_PK_IN", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Input;
                    _with15.Parameters["PICK_DROP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with15.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with15.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with15.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with15.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with15.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with15.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with15.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with15.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with15.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with15.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with15.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with15.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with15.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with15.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with15.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with15.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with15.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with15.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with15.Parameters.Add("MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with15.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with15.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with16 = objWK.MyDataAdapter;

                    _with16.InsertCommand = insDropDetails;
                    _with16.InsertCommand.Transaction = TRAN;

                    _with16.UpdateCommand = updDropDetails;
                    _with16.UpdateCommand.Transaction = TRAN;

                    RecAfct = _with16.Update(dsDropDetails);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                //End Manjunath
                if ((dsTPDetails != null))
                {
                    var _with17 = insTPDetails;
                    _with17.Connection = objWK.MyConnection;
                    _with17.CommandType = CommandType.StoredProcedure;
                    _with17.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_TP_INS";
                    var _with18 = _with17.Parameters;

                    insTPDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                    insTPDetails.Parameters.Add("WIN_TYPE_IN", OracleDbType.Int32, 3, "WIN_TYPE").Direction = ParameterDirection.Input;
                    insTPDetails.Parameters["WIN_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    insTPDetails.Parameters.Add("WIN_MODE_IN", OracleDbType.Int32, 3, "WIN_MODE").Direction = ParameterDirection.Input;
                    insTPDetails.Parameters["WIN_MODE_IN"].SourceVersion = DataRowVersion.Current;

                    insTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_IMP_TP_PK").Direction = ParameterDirection.Output;
                    insTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                    var _with19 = updTPDetails;
                    _with19.Connection = objWK.MyConnection;
                    _with19.CommandType = CommandType.StoredProcedure;
                    _with19.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_TP_UPD";
                    var _with20 = _with19.Parameters;

                    updTPDetails.Parameters.Add("JOB_TRN_SEA_IMP_TP_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_tp_pk").Direction = ParameterDirection.Input;
                    updTPDetails.Parameters["JOB_TRN_SEA_IMP_TP_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updTPDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                    updTPDetails.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input;
                    updTPDetails.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updTPDetails.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input;
                    updTPDetails.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updTPDetails.Parameters.Add("WIN_TYPE_IN", OracleDbType.Int32, 3, "WIN_TYPE").Direction = ParameterDirection.Input;
                    updTPDetails.Parameters["WIN_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    updTPDetails.Parameters.Add("WIN_MODE_IN", OracleDbType.Int32, 3, "WIN_MODE").Direction = ParameterDirection.Input;
                    updTPDetails.Parameters["WIN_MODE_IN"].SourceVersion = DataRowVersion.Current;

                    updTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                    var _with21 = delTPDetails;
                    _with21.Connection = objWK.MyConnection;
                    _with21.CommandType = CommandType.StoredProcedure;
                    _with21.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_TP_DEL";

                    delTPDetails.Parameters.Add("JOB_TRN_SEA_IMP_TP_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_tp_pk").Direction = ParameterDirection.Input;
                    delTPDetails.Parameters["JOB_TRN_SEA_IMP_TP_PK_IN"].SourceVersion = DataRowVersion.Current;

                    delTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    delTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                    var _with22 = objWK.MyDataAdapter;

                    _with22.InsertCommand = insTPDetails;
                    _with22.InsertCommand.Transaction = TRAN;

                    _with22.UpdateCommand = updTPDetails;
                    _with22.UpdateCommand.Transaction = TRAN;

                    _with22.DeleteCommand = delTPDetails;
                    _with22.DeleteCommand.Transaction = TRAN;
                    RecAfct = _with22.Update(dsTPDetails.Tables[0]);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        if (isEdting == false)
                        {
                            RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        return arrMessage;
                    }

                }

                if ((dsFreightDetails != null))
                {
                    var _with23 = insFreightDetails;
                    _with23.Connection = objWK.MyConnection;
                    _with23.CommandType = CommandType.StoredProcedure;
                    _with23.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_INS";
                    var _with24 = _with23.Parameters;

                    insFreightDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    //insFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input
                    //insFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                    insFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    insFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                    //latha
                    insFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "Frtpayer_mst_fk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;
                    //Added by Faheem
                    insFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "RATEPERBASIS").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;
                    //End
                    //'SURCHARGE
                    insFreightDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 50, "SURCHARGE").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;
                    //'SURCHARGE

                    insFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURR_FK").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    //'changed

                    insFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("JOB_CARD_SEA_IMP_CONT_PK_IN", OracleDbType.Int32, 10, "JOB_CARD_SEA_IMP_CONT_PK").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["JOB_CARD_SEA_IMP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_IMP_FD_PK").Direction = ParameterDirection.Output;
                    insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;




                    var _with25 = updFreightDetails;
                    _with25.Connection = objWK.MyConnection;
                    _with25.CommandType = CommandType.StoredProcedure;
                    _with25.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_UPD";
                    var _with26 = _with25.Parameters;

                    updFreightDetails.Parameters.Add("JOB_TRN_SEA_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_fd_pk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["JOB_TRN_SEA_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    //updFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input
                    //updFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                    updFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    updFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                    //latha
                    updFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "Frtpayer_mst_fk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;
                    //Added by Faheem
                    updFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "RATEPERBASIS").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;
                    //End
                    updFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURR_FK").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    //'curency

                    updFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    //'SURCHARGE
                    updFreightDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 50, "SURCHARGE").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;
                    //'SURCHARGE

                    updFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with27 = delFreightDetails;
                    _with27.Connection = objWK.MyConnection;
                    _with27.CommandType = CommandType.StoredProcedure;
                    _with27.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_DEL";

                    delFreightDetails.Parameters.Add("JOB_TRN_SEA_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_fd_pk").Direction = ParameterDirection.Input;
                    delFreightDetails.Parameters["JOB_TRN_SEA_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                    delFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    delFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with28 = objWK.MyDataAdapter;

                    _with28.InsertCommand = insFreightDetails;
                    _with28.InsertCommand.Transaction = TRAN;

                    _with28.UpdateCommand = updFreightDetails;
                    _with28.UpdateCommand.Transaction = TRAN;

                    _with28.DeleteCommand = delFreightDetails;
                    _with28.DeleteCommand.Transaction = TRAN;

                    //RecAfct = .Update(dsFreightDetails)
                    RecAfct = _with28.Update(dsFreightDetails.Tables[0]);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        if (isEdting == false)
                        {
                            RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        return arrMessage;
                    }

                }

                if ((dsPurchaseInventory != null))
                {
                    var _with29 = insPurchaseInvDetails;
                    _with29.Connection = objWK.MyConnection;
                    _with29.CommandType = CommandType.StoredProcedure;
                    _with29.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_PIA_INS";
                    var _with30 = _with29.Parameters;

                    insPurchaseInvDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    insPurchaseInvDetails.Parameters.Add("COST_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "cost_element_mst_fk").Direction = ParameterDirection.Input;
                    insPurchaseInvDetails.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insPurchaseInvDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "vendor_key").Direction = ParameterDirection.Input;
                    insPurchaseInvDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                    insPurchaseInvDetails.Parameters.Add("INVOICE_NUMBER_IN", OracleDbType.Varchar2, 20, "invoice_number").Direction = ParameterDirection.Input;
                    insPurchaseInvDetails.Parameters["INVOICE_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    insPurchaseInvDetails.Parameters.Add("INVOICE_DATE_IN", OracleDbType.Date, 20, "invoice_date").Direction = ParameterDirection.Input;
                    insPurchaseInvDetails.Parameters["INVOICE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    insPurchaseInvDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                    insPurchaseInvDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insPurchaseInvDetails.Parameters.Add("INVOICE_AMT_IN", OracleDbType.Int32, 10, "invoice_amt").Direction = ParameterDirection.Input;
                    insPurchaseInvDetails.Parameters["INVOICE_AMT_IN"].SourceVersion = DataRowVersion.Current;

                    insPurchaseInvDetails.Parameters.Add("TAX_PERCENTAGE_IN", OracleDbType.Int32, 5, "tax_percentage").Direction = ParameterDirection.Input;
                    insPurchaseInvDetails.Parameters["TAX_PERCENTAGE_IN"].SourceVersion = DataRowVersion.Current;

                    insPurchaseInvDetails.Parameters.Add("TAX_AMT_IN", OracleDbType.Int32, 10, "tax_amt").Direction = ParameterDirection.Input;
                    insPurchaseInvDetails.Parameters["TAX_AMT_IN"].SourceVersion = DataRowVersion.Current;

                    insPurchaseInvDetails.Parameters.Add("ESTIMATED_AMT_IN", OracleDbType.Int32, 10, "estimated_amt").Direction = ParameterDirection.Input;
                    insPurchaseInvDetails.Parameters["ESTIMATED_AMT_IN"].SourceVersion = DataRowVersion.Current;

                    insPurchaseInvDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "vendor_mst_fk").Direction = ParameterDirection.Input;
                    insPurchaseInvDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insPurchaseInvDetails.Parameters.Add("ATTACHED_FILE_NAME_IN", OracleDbType.Varchar2, 50, "attached_file_name").Direction = ParameterDirection.Input;
                    insPurchaseInvDetails.Parameters["ATTACHED_FILE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                    insPurchaseInvDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_IMP_PIA_PK").Direction = ParameterDirection.Output;
                    insPurchaseInvDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                    var _with31 = updPurchaseInvDetails;
                    _with31.Connection = objWK.MyConnection;
                    _with31.CommandType = CommandType.StoredProcedure;
                    _with31.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_PIA_UPD";
                    var _with32 = _with31.Parameters;

                    updPurchaseInvDetails.Parameters.Add("JOB_TRN_SEA_IMP_PIA_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_pia_pk").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["JOB_TRN_SEA_IMP_PIA_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    updPurchaseInvDetails.Parameters.Add("COST_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "cost_element_mst_fk").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "vendor_key").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("INVOICE_NUMBER_IN", OracleDbType.Varchar2, 20, "invoice_number").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["INVOICE_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("INVOICE_DATE_IN", OracleDbType.Date, 20, "invoice_date").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["INVOICE_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("INVOICE_AMT_IN", OracleDbType.Int32, 10, "invoice_amt").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["INVOICE_AMT_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("TAX_PERCENTAGE_IN", OracleDbType.Int32, 5, "tax_percentage").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["TAX_PERCENTAGE_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("TAX_AMT_IN", OracleDbType.Int32, 10, "tax_amt").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["TAX_AMT_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("ESTIMATED_AMT_IN", OracleDbType.Int32, 10, "estimated_amt").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["ESTIMATED_AMT_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "vendor_mst_fk").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("ATTACHED_FILE_NAME_IN", OracleDbType.Varchar2, 50, "attached_file_name").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["ATTACHED_FILE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updPurchaseInvDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with33 = delPurchaseInvDetails;
                    _with33.Connection = objWK.MyConnection;
                    _with33.CommandType = CommandType.StoredProcedure;
                    _with33.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_PIA_DEL";

                    delPurchaseInvDetails.Parameters.Add("JOB_TRN_SEA_IMP_PIA_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_pia_pk").Direction = ParameterDirection.Input;
                    delPurchaseInvDetails.Parameters["JOB_TRN_SEA_IMP_PIA_PK_IN"].SourceVersion = DataRowVersion.Current;

                    delPurchaseInvDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    delPurchaseInvDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                    var _with34 = objWK.MyDataAdapter;

                    _with34.InsertCommand = insPurchaseInvDetails;
                    _with34.InsertCommand.Transaction = TRAN;

                    _with34.UpdateCommand = updPurchaseInvDetails;
                    _with34.UpdateCommand.Transaction = TRAN;

                    _with34.DeleteCommand = delPurchaseInvDetails;
                    _with34.DeleteCommand.Transaction = TRAN;

                    RecAfct = _with34.Update(dsPurchaseInventory.Tables[0]);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        if (isEdting == false)
                        {
                            RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                        }
                        return arrMessage;
                    }
                }
                //'Added By Koteshwari on 25/4/2011
                if ((dsCostDetails != null))
                {
                    var _with35 = insCostDetails;
                    _with35.Connection = objWK.MyConnection;
                    _with35.CommandType = CommandType.StoredProcedure;
                    _with35.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_INS";
                    var _with36 = _with35.Parameters;
                    insCostDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                    //'surcharge
                    insCostDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                    insCostDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                    insCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_IMP_COST_PK").Direction = ParameterDirection.Output;
                    insCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with37 = updCostDetails;
                    _with37.Connection = objWK.MyConnection;
                    _with37.CommandType = CommandType.StoredProcedure;
                    _with37.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_UPD";
                    var _with38 = _with37.Parameters;

                    updCostDetails.Parameters.Add("JOB_TRN_IMP_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_SEA_IMP_COST_PK").Direction = ParameterDirection.Input;
                    updCostDetails.Parameters["JOB_TRN_IMP_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updCostDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                    //'surcharge
                    updCostDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                    updCostDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                    updCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "CLng(RETURN_VALUE)").Direction = ParameterDirection.Output;
                    updCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with39 = delCostDetails;
                    _with39.Connection = objWK.MyConnection;
                    _with39.CommandType = CommandType.StoredProcedure;
                    _with39.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_DEL";

                    delCostDetails.Parameters.Add("JOB_TRN_IMP_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_SEA_IMP_COST_PK").Direction = ParameterDirection.Input;
                    delCostDetails.Parameters["JOB_TRN_IMP_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    delCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    delCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

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
                }
                //'End Koteshwari
                if ((dsIncomeChargeDetails != null) & (dsExpenseChargeDetails == null))
                {
                    if (!SaveSecondaryServices(ref objWK, ref TRAN, Convert.ToInt32(JobCardPK), ref dsIncomeChargeDetails, ref dsExpenseChargeDetails))
                    {
                        arrMessage.Add("Error while saving secondary service details");
                        return arrMessage;
                    }
                }


                if ((dsOtherCharges != null))
                {
                    var _with41 = insOtherChargesDetails;
                    _with41.Connection = objWK.MyConnection;
                    _with41.CommandType = CommandType.StoredProcedure;
                    _with41.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_OTH_CHRG_INS";

                    _with41.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with41.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    _with41.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                    _with41.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    //latha

                    _with41.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "PaymentType").Direction = ParameterDirection.Input;
                    _with41.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;


                    _with41.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                    _with41.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "Frtpayer_mst_fk").Direction = ParameterDirection.Input;
                    _with41.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    //end

                    _with41.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                    _with41.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                    _with41.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with41.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "job_trn_sea_imp_oth_pk").Direction = ParameterDirection.Output;
                    _with41.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;



                    var _with42 = updOtherChargesDetails;
                    _with42.Connection = objWK.MyConnection;
                    _with42.CommandType = CommandType.StoredProcedure;
                    _with42.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_OTH_CHRG_UPD";

                    _with42.Parameters.Add("JOB_TRN_SEA_IMP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_oth_pk").Direction = ParameterDirection.Input;
                    _with42.Parameters["JOB_TRN_SEA_IMP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with42.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    _with42.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                    _with42.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "PaymentType").Direction = ParameterDirection.Input;
                    _with42.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                    _with42.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_mst_fk").Direction = ParameterDirection.Input;
                    _with42.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                    _with42.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                    _with42.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with42.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with43 = delOtherChargesDetails;
                    _with43.Connection = objWK.MyConnection;
                    _with43.CommandType = CommandType.StoredProcedure;
                    _with43.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_OTH_CHRG_DEL";

                    _with43.Parameters.Add("JOB_TRN_SEA_IMP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_oth_pk").Direction = ParameterDirection.Input;
                    _with43.Parameters["JOB_TRN_SEA_IMP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

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

                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    if (isEdting == false)
                    {
                        RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                        //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    }
                    return arrMessage;
                }
                else
                {
                    cls_JobCardView objJCFclLcl = new cls_JobCardView();
                    objJCFclLcl.CREATED_BY = Convert.ToInt64(M_CREATED_BY_FK);
                    objJCFclLcl.LAST_MODIFIED_BY = Convert.ToInt64(M_CREATED_BY_FK);
                    objJCFclLcl.ConfigurationPK = Convert.ToInt64(M_Configuration_PK);
                    arrMessage = (ArrayList)objJCFclLcl.SaveJobCardDoc(Convert.ToString(JobCardPK), TRAN, dsDoc, 2, 2);
                    //Biztype -2(Sea),Process Type -2(Import)
                    if (arrMessage.Count > 0)
                    {
                        if (!(string.Compare(arrMessage[0].ToString(), "Saved") > 0))
                        {
                            TRAN.Rollback();
                            if (isEdting == false)
                            {
                                RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                                //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                            }
                            return arrMessage;
                        }
                    }
                    arrMessage = (ArrayList)SaveTrackAndTrace(Convert.ToInt32(JobCardPK), TRAN, ref dsTrackNtrace, Convert.ToInt32(userLocation), isEdting, ref M_DataSet);
                    //'If arrMessage.Count > 0 Then
                    if (string.Compare(arrMessage[0].ToString(), "Saved") > 0)
                    {
                        TRAN.Commit();
                        //Push to financial system if realtime is selected
                        if (JobCardPK > 0)
                        {
                            cls_Scheduler objSch = new cls_Scheduler();
                            ArrayList schDtls = null;
                            bool errGen = false;
                            if (objSch.GetSchedulerPushType() == true)
                            {
                                //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                                //try
                                //{
                                //    schDtls = objSch.FetchSchDtls();
                                //    //'Used to Fetch the Sch Dtls
                                //    objPush.UpdateCostCentre(schDtls[10], schDtls[2], schDtls[6], schDtls[4], ref errGen, schDtls[5].ToString().ToUpper(), schDtls[0].ToString().ToUpper(), , , JobCardPK);
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 1, Session["USER_PK"]);
                                //    }
                                //}
                                //catch (Exception ex)
                                //{
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 2, Session["USER_PK"]);
                                //    }
                                //}
                            }
                        }
                        //*****************************************************************
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        if (isEdting == false)
                        {
                            RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                    }
                    return arrMessage;
                }

            }
            catch (OracleException oraexp)
            {
                if (isEdting == false)
                {
                    RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                throw oraexp;
            }
            catch (Exception ex)
            {
                if (isEdting == false)
                {
                    RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }
        public bool SaveSecondaryServices(ref WorkFlow objWK, ref OracleTransaction TRAN, int JobCardPK, ref DataSet dsIncomeChargeDetails, ref DataSet dsExpenseChargeDetails)
        {
            if ((dsIncomeChargeDetails != null))
            {
                //----------------------------------Income Charge Details----------------------------------
                foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                {
                    int Frt_Pk = 0;
                    try
                    {
                        Frt_Pk = Convert.ToInt32(ri["JOB_TRN_SEA_IMP_FD_PK"]);
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
                        _with45.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_UPD";
                        _with45.Parameters.Add("JOB_TRN_SEA_IMP_FD_PK_IN", ri["JOB_TRN_SEA_IMP_FD_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with45.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_INS";
                        _with45.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }
                    _with45.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("BASIS_IN", getDefault(ri["BASIS"], DBNull.Value)).Direction = ParameterDirection.Input;
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
                            _with45.Parameters.Add("PROCESS_IN", 2).Direction = ParameterDirection.Input;
                            _with45.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with45.ExecuteNonQuery();
                            Frt_Pk = Convert.ToInt32(_with45.Parameters["RETURN_VALUE"].Value);
                            ri["JOB_TRN_SEA_IMP_FD_PK"] = Frt_Pk;
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
                        Cost_Pk = Convert.ToInt32(re["JOB_TRN_SEA_IMP_COST_PK"]);
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
                        _with46.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_UPD";
                        _with46.Parameters.Add("JOB_TRN_IMP_EST_PK_IN", re["JOB_TRN_SEA_IMP_COST_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with46.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_INS";
                        _with46.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }

                    _with46.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
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
                            _with46.Parameters.Add("PROCESS_IN", 2).Direction = ParameterDirection.Input;
                            _with46.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with46.ExecuteNonQuery();
                            Cost_Pk = Convert.ToInt32(_with46.Parameters["RETURN_VALUE"].Value);
                            re["JOB_TRN_SEA_IMP_COST_PK"] = Cost_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            ClearRemovedServices(ref objWK, ref TRAN, JobCardPK, ref dsIncomeChargeDetails, ref dsExpenseChargeDetails);
            return true;
        }
        public bool ClearRemovedServices(ref WorkFlow objWK, ref OracleTransaction TRAN, int JobCardPK, ref DataSet dsIncomeChargeDetails, ref DataSet dsExpenseChargeDetails)
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
                            SelectedFrtPks = ri["JOB_TRN_SEA_IMP_FD_PK"].ToString();
                        }
                        else
                        {
                            SelectedFrtPks += "," + ri["JOB_TRN_SEA_IMP_FD_PK"];
                        }
                    }
                    foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedCostPks))
                        {
                            SelectedCostPks = re["JOB_TRN_SEA_IMP_COST_PK"].ToString();
                        }
                        else
                        {
                            SelectedCostPks += "," + re["JOB_TRN_SEA_IMP_COST_PK"];
                        }
                    }

                    var _with47 = objWK.MyCommand;
                    _with47.Transaction = TRAN;
                    _with47.CommandType = CommandType.StoredProcedure;
                    _with47.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.DELETE_SEA_IMP_SEC_CHG_EXCEPT";
                    _with47.Parameters.Clear();
                    _with47.Parameters.Add("JOB_CARD_SEA_IMP_PK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with47.Parameters.Add("JOB_TRN_SEA_IMP_FD_PKS", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
                    _with47.Parameters.Add("JOB_TRN_SEA_IMP_COST_PKS", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
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

        #region "Track N Trace Save Functionality"
        public object SaveTrackAndTrace(int jobPk, OracleTransaction TRAN, ref DataSet dsContainerData, int nlocationfk, bool IsEditing, ref DataSet dsMain)
        {
            int Cnt = 0;
            int POD = 0;
            var UpdCntrOnQuay = false;
            string strContData = null;
            System.DateTime ArrDate = default(System.DateTime);
            string Status = null;
            string PortDis = null;

            try
            {
                //The Below Lines are modified By Anand for Capturing Arrived At POD :12/11/08

                if (string.IsNullOrEmpty((dsMain.Tables[0].Rows[0]["ARRIVAL_DATE"].ToString())))
                {
                    ArrDate = System.DateTime.Now;
                }
                else
                {
                    ArrDate = Convert.ToDateTime(dsMain.Tables[0].Rows[0]["ARRIVAL_DATE"]);
                }

                if (string.IsNullOrEmpty((dsMain.Tables[0].Rows[0]["PORT_MST_POD_FK"].ToString())))
                {
                    POD = 0;
                }
                else
                {
                    POD = Convert.ToInt32(dsMain.Tables[0].Rows[0]["PORT_MST_POD_FK"]);
                }

                PortDis = CheckPOD(POD);
                Status = "Vessel Arrived At '" + PortDis + "' at '" + ArrDate + "' ";

                Status = "Vessel Arrived At '" + PortDis + "' at '" + ArrDate + "' ";

                objTrackNTrace.DeleteOnSaveTraceExportOnATDLDUpd(jobPk, 2, 2, "Vessel Voyage", "CNT-DT-DATA-DEL-SEA-IMP", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                "Null");
                for (Cnt = 0; Cnt <= dsContainerData.Tables[0].Rows.Count - 1; Cnt++)
                {
                    if (!string.IsNullOrEmpty((dsContainerData.Tables[0].Rows[Cnt]["gen_land_date"].ToString())))
                    {
                        UpdCntrOnQuay = true;
                        var _with48 = dsContainerData.Tables[0].Rows[Cnt];
                        // Updated by Amit on 05-Jan-07 to solve Issue of Track & Trace
                        // The Status "Discharge Container XXXXXXX at XXXX(POD ID)" should be displayed as "Container XXXXXXXX discharged at XXXXX (POD Name)"
                        if (string.IsNullOrEmpty((dsMain.Tables[0].Rows[0]["POD"].ToString())))
                        {
                            //strContData = "Discharge Container " & .Item("container_number") & "~" & .Item("gen_land_date")
                            strContData = "Container " + _with48["container_number"] + " discharged" + "~" + _with48["gen_land_date"];
                        }
                        else
                        {
                            //strContData = "Discharge Container " & .Item("container_number") & " at  " & dsMain.Tables(0).Rows(0).Item("POD") & "~" & .Item("gen_land_date")
                            strContData = "Container " + _with48["container_number"] + " discharged at  " + dsMain.Tables[0].Rows[0]["POD"] + "~" + _with48["gen_land_date"];
                        }
                        // End

                        // strContData = "Discharge Container " & .Item("container_number") & " at  " & dsMain.Tables(0).Rows(0).Item("del_address") & "~" & .Item("gen_land_date")
                        arrMessage = objTrackNTrace.SaveBBTrackAndTraceImportJob(jobPk, 2, 2, strContData, "CNT-ON-QUAY-DT-UPD-SEA-IMP", nlocationfk, TRAN, "INS", CREATED_BY, "O");
                    }
                }
                //modified by thiyagarajan on 18/12/08 for import track n trace
                if (IsEditing == false)
                {
                    arrMessage = objTrackNTrace.SaveBBTrackAndTraceImportJob(jobPk, 2, 2, "Job Card", "JOB-INS-SEA-IMP", nlocationfk, TRAN, "INS", CREATED_BY, "O");
                    arrMessage = objTrackNTrace.SaveTrackAndTraceImportJobATA(jobPk, 2, 2, Status, "JOB-INS-SEA-IMP-ATA", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                    ArrDate);
                }
                else if (Convert.ToInt32(CheckATA(jobPk)) == 0)
                {
                    arrMessage = objTrackNTrace.SaveTrackAndTraceImportJobATA(jobPk, 2, 2, Status, "JOB-INS-SEA-IMP-ATA", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                    ArrDate);
                }
                arrMessage.Clear();
                arrMessage.Add("All Data Saved Successfully");
                return arrMessage;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 22/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                arrMessage.Add("Error");
                return arrMessage;
            }
        }
        //The Below Lines are modified By Anand for Capturing Arrived At POD :12/11/08
        public string CheckPOD(Int32 POD)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                string sql = null;
                string PortDis = "";

                sql = "select pmt.port_name from port_mst_tbl pmt where pmt.port_mst_pk='" + POD + "'";
                PortDis = objWF.ExecuteScaler(sql);

                return PortDis;
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
        //created by thiyagarajan on 18/12/08 for import track n trace
        public string CheckATA(Int32 jobpk)
        {
            WorkFlow objWF = new WorkFlow();
            string sql = null;
            try
            {
                sql = "select count(*) from TRACK_N_TRACE_TBL TR WHERE TR.STATUS LIKE '%Vessel Arrived At%' and TR.BIZ_TYPE=2 AND ";
                sql += " TR.PROCESS = 2 And TR.JOB_CARD_FK = " + jobpk;
                return objWF.ExecuteScaler(sql);
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
                strSQL.Append( "SELECT (SELECT TO_CHAR(SYSDATE,'yy')  FROM dual)||country.country_id||cmt.vat_no");
                strSQL.Append( "    FROM");
                strSQL.Append( "    customer_contact_dtls cust_det,");
                strSQL.Append( "    customer_mst_tbl cmt,");
                strSQL.Append( "    country_mst_tbl country,");
                strSQL.Append( "    location_mst_tbl lmt");
                strSQL.Append( "    WHERE");
                strSQL.Append( "    cmt.customer_mst_pk =" + customerID);
                strSQL.Append( "    AND cust_det.customer_mst_fk = cmt.customer_mst_pk(+)");
                strSQL.Append( "    AND cust_det.adm_location_mst_fk = lmt.location_mst_pk(+)");
                strSQL.Append( "    AND lmt.country_mst_fk = country.country_mst_pk(+)");

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
        #endregion

        public DataSet FillContainerTypeDataSet(string pk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();


            strSQL.Append( " SELECT distinct");
            strSQL.Append( " cont.container_type_mst_pk,");
            strSQL.Append( " cont.container_type_mst_id");
            strSQL.Append( " FROM");
            strSQL.Append( "  job_trn_sea_imp_cont job_trn,");
            strSQL.Append( " container_type_mst_tbl cont");
            strSQL.Append( " WHERE");
            strSQL.Append( " job_trn.container_type_mst_fk = cont.container_type_mst_pk");

            strSQL.Append( " and job_trn.job_card_sea_imp_fk =" + pk);

            strSQL.Append( " ORDER BY cont.container_type_mst_id");


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

        #region "Fill Combo"
        public DataSet FillCargoMoveCode()
        {

            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT 0 cargo_move_pk, 'Select' cargo_move_code ");
            strSQL.Append(" FROM DUAL ");
            strSQL.Append(" UNION ");

            strSQL.Append(" SELECT");
            strSQL.Append("      c.cargo_move_pk,");
            strSQL.Append("      c.cargo_move_code");
            strSQL.Append(" FROM");
            strSQL.Append("      cargo_move_mst_tbl c");
            strSQL.Append(" WHERE");
            strSQL.Append("      c.active_flag = 1");
            strSQL.Append("  group by cargo_move_pk,cargo_move_code ");

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

            strSQL.Append(" SELECT 0 shipping_terms_mst_pk, 'Select' inco_code ");
            strSQL.Append(" FROM DUAL ");
            strSQL.Append(" UNION ");

            strSQL.Append("SELECT");
            strSQL.Append("      s.shipping_terms_mst_pk,");
            strSQL.Append("      s.inco_code ");
            strSQL.Append(" FROM");
            strSQL.Append("      shipping_terms_mst_tbl s ");
            strSQL.Append(" WHERE");
            strSQL.Append("      s.active_flag = 1 ");
            strSQL.Append("  group by shipping_terms_mst_pk,inco_code ");

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

        #region " Fetch Revenue data export"
        public DataSet FetchRevenueData(string jobCardPK = "0")
        {
            //Dim strSQL As StringBuilder = New StringBuilder
            //Dim objWF As New WorkFlow
            //Try
            //    strSQL.Append(vbCrLf & "select")
            //    strSQL.Append(vbCrLf & "    inv.invoice_ref_no,")
            //    strSQL.Append(vbCrLf & "    inv.invoice_date,")
            //    strSQL.Append(vbCrLf & "    nvl(inv.invoice_amt,0) + nvl(inv.vat_amt,0) - nvl(inv.discount_amt,0),")
            //    strSQL.Append(vbCrLf & "    curr.currency_id")
            //    strSQL.Append(vbCrLf & "from")
            //    strSQL.Append(vbCrLf & "    inv_cust_sea_imp_tbl inv,")
            //    strSQL.Append(vbCrLf & "    currency_type_mst_tbl curr,")
            //    strSQL.Append(vbCrLf & "    job_card_sea_imp_tbl job_imp")
            //    strSQL.Append(vbCrLf & "where")
            //    strSQL.Append(vbCrLf & "    inv.job_card_sea_imp_fk = job_imp.job_card_sea_imp_pk")
            //    strSQL.Append(vbCrLf & "    AND job_imp.job_card_sea_imp_pk = " + jobCardPK)
            //    strSQL.Append(vbCrLf & "    AND inv.currency_mst_fk = curr.currency_mst_pk")

            //    Return objWF.GetDataSet(strSQL.ToString)
            //Catch sqlExp As OracleException
            //    ErrorMessage = sqlExp.Message
            //    Throw sqlExp
            //Catch exp As Exception
            //    ErrorMessage = exp.Message
            //    Throw exp
            //End Try

            //Dim strSQL As StringBuilder = New StringBuilder

            WorkFlow objWF = new WorkFlow();

            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with49 = objWF.MyCommand.Parameters;
                _with49.Add("JOBCARD_PK", jobCardPK).Direction = ParameterDirection.Input;
                _with49.Add("JOB_SEA_IMP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_SEA_IMP");
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

        #region "Calculate_TAX" ''Added by subhransu for tax calculation
        public object Calculate_TAX(string jobCardID = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append(" SELECT  NVL(SUM(JP.TAX_AMT), 0) AS COST_TAX");
                sb.Append("   FROM JOB_CARD_SEA_IMP_TBL   JC,");
                sb.Append("        JOB_TRN_SEA_IMP_PIA    JP");
                sb.Append("  WHERE ");
                sb.Append("     JC.JOB_CARD_SEA_IMP_PK = " + jobCardID + "");
                sb.Append("    AND JC.JOB_CARD_SEA_IMP_PK = JP.JOB_CARD_SEA_IMP_FK(+)");

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
        public object Calculate_TAX_Cost(string jobCardID = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            try
            {

                sb.Append("  SELECT NVL(SUM(CI.TAX_AMT), 0) AS REVENUE_TAX");
                sb.Append("  FROM JOB_CARD_SEA_IMP_TBL   JC,");
                sb.Append("   CONSOL_INVOICE_TRN_TBL CI");
                sb.Append("  WHERE JC.JOB_CARD_SEA_IMP_PK = CI.JOB_CARD_FK(+)");
                sb.Append("  AND JC.JOB_CARD_SEA_IMP_PK = " + jobCardID + "");
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

        #region "GetRevenueDetails"
        public DataSet GetRevenueDetails(int LocationPk, ref decimal actualCost, ref decimal actualRevenue, ref decimal estimatedCost, ref decimal estimatedRevenue, string jobCardPK)
        {

            //Dim SQL As New System.Text.StringBuilder
            WorkFlow objWF = new WorkFlow();
            //Snigdharani - 10/11/2008 - making the values same as consolidation screen.
            try
            {
                DataSet DS = new DataSet();
                //adding "CurrPk" by thiyagarajan on 24/11/08 for location based currency task
                var _with50 = objWF.MyCommand.Parameters;
                _with50.Add("JCPK", jobCardPK).Direction = ParameterDirection.Input;
                _with50.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with50.Add("JOB_IMP_SEA", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_COST_REVENUE_PROFIT", "FETCH_JOBCARD_IMP_SEA");
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

        public DataSet FillJobCardOtherChargesDataSet(string pk = "0")
        {

            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT OTH_CHRG.JOB_TRN_SEA_IMP_OTH_PK,");
            sb.Append("       FRT.FREIGHT_ELEMENT_MST_PK,");
            sb.Append("       FRT.FREIGHT_ELEMENT_ID,");
            sb.Append("       FRT.FREIGHT_ELEMENT_NAME,");
            sb.Append("       DECODE(OTH_CHRG.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') PAYMENTTYPE,");
            sb.Append("       OTH_CHRG.LOCATION_MST_FK \"location_fk\",");
            sb.Append("       LOC.LOCATION_ID \"location_id\",");
            sb.Append("       OTH_CHRG.FRTPAYER_CUST_MST_FK \"frtpayer_mst_fk\",");
            sb.Append("       CUS.CUSTOMER_ID \"frtpayer\",");
            sb.Append("       CURR.CURRENCY_ID CURRENCY_MST_PK,");
            sb.Append("       OTH_CHRG.EXCHANGE_RATE \"ROE\",");
            sb.Append("       OTH_CHRG.AMOUNT AMOUNT,");
            sb.Append("       ROUND((OTH_CHRG.AMOUNT * OTH_CHRG.EXCHANGE_RATE), 2) TOTAL,");
            sb.Append("       'false' \"Delete\",");
            sb.Append("       CURR.CURRENCY_MST_PK CURRFK");
            sb.Append("  FROM JOB_TRN_SEA_IMP_OTH_CHRG OTH_CHRG,");
            sb.Append("       JOB_CARD_SEA_IMP_TBL     JOBCARD_MST,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL  FRT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL    CURR,");
            sb.Append("       LOCATION_MST_TBL         LOC,");
            sb.Append("       CUSTOMER_MST_TBL         CUS");
            sb.Append(" WHERE OTH_CHRG.JOB_CARD_SEA_IMP_FK = JOBCARD_MST.JOB_CARD_SEA_IMP_PK");
            sb.Append("   AND OTH_CHRG.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("   AND OTH_CHRG.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK(+)");
            sb.Append("   AND OTH_CHRG.JOB_CARD_SEA_IMP_FK = " + pk);
            sb.Append("   AND OTH_CHRG.FRTPAYER_CUST_MST_FK = CUS.CUSTOMER_MST_PK(+)");
            sb.Append("   AND OTH_CHRG.LOCATION_MST_FK = LOC.LOCATION_MST_PK(+)");
            sb.Append(" ORDER BY FREIGHT_ELEMENT_ID");

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

        #region "Frieght Element"
        public DataSet FetchFret(int jobcardpk)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                ///strsql = "select * from  CONSOL_INVOICE_TRN_TBl where JOB_CARD_FK = " & jobcardpk & " AND FRT_OTH_ELEMENT = 1"
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT CON.*, COMM.COMMODITY_MST_PK,COMM.COMMODITY_NAME");
                sb.Append("  FROM CONSOL_INVOICE_TRN_TBL CON,");
                sb.Append("       JOB_TRN_SEA_IMP_FD     FD_TRN,");
                sb.Append("       JOB_TRN_SEA_IMP_CONT   JOB_TRN,");
                sb.Append("       COMMODITY_MST_TBL      COMM");
                sb.Append(" WHERE CON.FRT_OTH_ELEMENT = 1");
                sb.Append("   AND JOB_TRN.JOB_CARD_SEA_IMP_FK = FD_TRN.JOB_CARD_SEA_IMP_FK");
                sb.Append("   AND JOB_TRN.COMMODITY_MST_FK = COMM.COMMODITY_MST_PK");
                sb.Append("   AND CON.FRT_OTH_ELEMENT_FK = FD_TRN.FREIGHT_ELEMENT_MST_FK");
                sb.Append("   AND CON.CONSOL_INVOICE_TRN_PK = FD_TRN.CONSOL_INVOICE_TRN_FK");
                sb.Append("   AND CON.JOB_CARD_FK = " + jobcardpk);
                return objWF.GetDataSet(sb.ToString());
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
        public DataSet FetchAgentFret(int jobcardpk)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("SELECT INVTRN.*, COMM.COMMODITY_MST_PK, COMM.COMMODITY_NAME");
                sb.Append("  FROM INV_AGENT_TBL     INV,");
                sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
                sb.Append("       JOB_TRN_SEA_IMP_FD        JOB_TRN_FD,");
                sb.Append("       JOB_TRN_SEA_IMP_CONT      JOB_TRN,");
                sb.Append("       COMMODITY_MST_TBL         COMM");
                sb.Append(" WHERE INVTRN.COST_FRT_ELEMENT = 2");
                sb.Append("   AND COMM.COMMODITY_MST_PK = JOB_TRN.COMMODITY_MST_FK(+)");
                sb.Append("   AND INV.JOB_CARD_SEA_IMP_FK = JOB_TRN_FD.JOB_CARD_SEA_IMP_FK");
                sb.Append("   AND JOB_TRN.JOB_CARD_SEA_IMP_FK = INV.JOB_CARD_SEA_IMP_FK");
                sb.Append("   AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
                sb.Append("   AND INVTRN.COST_FRT_ELEMENT_FK = JOB_TRN_FD.FREIGHT_ELEMENT_MST_FK");
                sb.Append("   AND INV.JOB_CARD_SEA_IMP_FK = " + jobcardpk);

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

        public DataSet getOthChr(string pk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append( "         SELECT");
            strSQL.Append( "         oth_chrg.inv_cust_trn_sea_imp_fk,");
            strSQL.Append( "         oth_chrg.inv_agent_trn_sea_imp_fk,");
            strSQL.Append( "         oth_chrg.consol_invoice_trn_fk");
            strSQL.Append( "FROM");
            strSQL.Append( "         job_trn_sea_imp_oth_chrg oth_chrg,");
            strSQL.Append( "         job_card_sea_imp_tbl jobcard_mst,");
            strSQL.Append( "         freight_element_mst_tbl frt,");
            strSQL.Append( "         currency_type_mst_tbl curr,");
            //latha
            strSQL.Append( "   location_mst_tbl loc,");
            strSQL.Append( "   customer_mst_tbl cus");
            strSQL.Append( "WHERE");
            strSQL.Append( "         oth_chrg.job_card_sea_imp_fk = jobcard_mst.job_card_sea_imp_pk");
            strSQL.Append( "         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            strSQL.Append( "         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
            strSQL.Append( "         AND oth_chrg.job_card_sea_imp_fk    = " + pk);
            //latha
            strSQL.Append( "   AND oth_chrg.frtpayer_cust_mst_fk = cus.customer_mst_pk(+) ");
            strSQL.Append( "   AND oth_chrg.location_mst_fk = loc.location_mst_pk(+) ");

            strSQL.Append( "ORDER BY freight_element_id ");

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

        #region " Fetch base currency Exchange rate on vessel voyage"
        public DataSet FetchVesselVoyageROE(Int64 voyage)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            try
            {
                strSQL = " SELECT C.CURRENCY_MST_PK," + " 1  ROE" + " FROM CURRENCY_TYPE_MST_TBL C" + " WHERE C.CURRENCY_MST_PK =" + "(SELECT CMT.CURRENCY_MST_FK FROM CORPORATE_MST_TBL CMT)" + " AND C.ACTIVE_FLAG = 1" + " UNION" + " SELECT CURR.CURRENCY_MST_PK," + " EXCHANGE_RATE ROE" + " FROM CURRENCY_TYPE_MST_TBL CURR, Exchange_Rate_Trn EXC" + " WHERE EXC.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK" + " AND CURR.ACTIVE_FLAG = 1" + " AND EXC.Voyage_Trn_Fk is not null" + " AND exc.voyage_trn_fk = " + voyage + " AND EXC.CURRENCY_MST_BASE_FK =" + " (SELECT CMT.CURRENCY_MST_FK FROM CORPORATE_MST_TBL CMT)" + " AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK" + " UNION" + " SELECT T.CURRENCY_MST_PK," + " TO_NUMBER(NULL) ROE" + " FROM CURRENCY_TYPE_MST_TBL T,CORPORATE_MST_TBL CC" + " WHERE T.CURRENCY_MST_PK NOT IN" + " ( SELECT CURRENCY_MST_PK" + " FROM CURRENCY_TYPE_MST_TBL , Exchange_Rate_Trn " + " WHERE CURRENCY_MST_FK = CURRENCY_MST_PK" + " AND ACTIVE_FLAG = 1" + " AND voyage_trn_fk = " + voyage + " AND CURRENCY_MST_BASE_FK =" + " (SELECT CURRENCY_MST_FK FROM CORPORATE_MST_TBL CMT)" + " AND CURRENCY_MST_BASE_FK <> CURRENCY_MST_FK )" + " AND T.CURRENCY_MST_PK<>CC.CURRENCY_MST_FK";


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

        #region " Fetch base currency Exchange rate export"
        public DataSet FetchROE(Int64 baseCurrency, string todate)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strSQL.Append( "SELECT");
                strSQL.Append( "    CURR.CURRENCY_MST_PK,");
                strSQL.Append( "    CURR.CURRENCY_ID,");
                //strSQL.Append(vbCrLf & "    ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK,(select c.currency_mst_fk from corporate_mst_tbl c),round(sysdate - .5)),6) AS ROE")
                //removing "Corporate mst tbl" by thiyagarajan on 21/11/08 for location based currency task
                strSQL.Append( "    ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",to_date('" + todate + "',dateformat)),6) as ROE ");
                //)),6) AS ROE")
                strSQL.Append( "FROM");
                strSQL.Append( "    CURRENCY_TYPE_MST_TBL CURR");
                strSQL.Append( "WHERE");
                strSQL.Append( "    CURR.ACTIVE_FLAG = 1");

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
                //SQL.Append(vbCrLf & "select i.inv_cust_sea_imp_pk from inv_cust_sea_imp_tbl i where i.job_card_sea_imp_fk = " & jobCardPK)
                SQL.Append("SELECT CON.CONSOL_INVOICE_PK");
                SQL.Append("  FROM CONSOL_INVOICE_TBL CON, CONSOL_INVOICE_TRN_TBL CONTRN");
                SQL.Append(" WHERE CON.CONSOL_INVOICE_PK = CONTRN.CONSOL_INVOICE_FK");
                SQL.Append("  AND CON.BUSINESS_TYPE = 2");
                SQL.Append("  AND CON.PROCESS_TYPE = 2");
                SQL.Append("   AND CONTRN.JOB_CARD_FK = " + jobCardPK);
            }
            else if (invoiceType == 2)
            {
                SQL.Append( "select i.inv_agent_pk from INV_AGENT_TBL i where i.CB_DP_LOAD_AGENT=1 AND  i.job_card_fk = " + jobCardPK);
            }
            else if (invoiceType == 3)
            {
                SQL.Append( "select i.inv_agent_pk from INV_AGENT_TBL i where  i.CB_DP_LOAD_AGENT=2 AND i.job_card_fk = " + jobCardPK);
            }

            oraReader = objWF.GetDataReader(SQL.ToString());

            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], DBNull.Value)))
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
        #endregion

        #region " Fetch Weight and Volume"
        public string FetchWeightAndVolume(string jobcardNumber)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            OracleDataReader oraReader = null;
            string strReturnValue = "";
            string strWeight = "0";
            string strVolume = "0";

            try
            {
                strSQL.Append( "select sum(nvl(cont.gross_weight,0)),");
                strSQL.Append( "           sum(nvl(cont.volume_in_cbm,0))");
                strSQL.Append( "      from job_card_sea_imp_tbl job, job_trn_sea_imp_cont cont");
                strSQL.Append( "     where job.job_card_sea_imp_pk = " + jobcardNumber);
                strSQL.Append( "   and job.job_card_sea_imp_pk = cont.job_card_sea_imp_fk");

                oraReader = objWF.GetDataReader(strSQL.ToString());

                while (oraReader.Read())
                {
                    if ((!object.ReferenceEquals(oraReader[0], DBNull.Value)))
                    {
                        strWeight = Convert.ToString(oraReader[0]);
                    }

                    if ((!object.ReferenceEquals(oraReader[1], DBNull.Value)))
                    {
                        strVolume = Convert.ToString(oraReader[1]);
                    }
                }

                strReturnValue = strWeight + "|" + strVolume;

                return strReturnValue;

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

        #region " Update Job Card data to Export Side"
        public object SaveDataToExport(string JobCardRefNo, long CargoMovePk, long Shipment_Terms, long PymtType, string ConsigneePK, string ETADate, string ArrivalDate)
        {
            string strSQL = null;

            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL = " update job_card_sea_exp_tbl job_exp ";
                strSQL = strSQL +  " set job_exp.pymt_type = " + PymtType;
                if (CargoMovePk > 0)
                {
                    strSQL = strSQL +  " , job_exp.cargo_move_fk = " + CargoMovePk;
                }
                if (Shipment_Terms > 0)
                {
                    strSQL = strSQL +  " , job_exp.shipping_terms_mst_fk =   " + Shipment_Terms;
                }
                strSQL = strSQL +  " , job_exp.CONSIGNEE_CUST_MST_FK = " + ConsigneePK;
                strSQL = strSQL +  " , job_exp.ETA_DATE =  to_date('" + ETADate + "', datetimeformat24)";
                //" & ETADate & "'"
                strSQL = strSQL +  " , job_exp.ARRIVAL_DATE = to_date('" + ArrivalDate + "', datetimeformat24)";
                //" & ArrivalDate & "'"
                strSQL = strSQL +  " where job_exp.jobcard_ref_no in ( ";
                strSQL = strSQL +  "  select j.jobcard_ref_no from job_card_sea_imp_tbl jj,job_card_sea_exp_tbl j ";
                strSQL = strSQL +  " where jj.jobcard_ref_no = j.jobcard_ref_no  ";
                strSQL = strSQL +  " and jj.jobcard_ref_no = '" + JobCardRefNo + "' )";
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
        #endregion


        #region "Get Invoice PK"
        public long GetPKValue(string strID, string FieldType)
        {

            System.Text.StringBuilder SQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            long lngPK = 0;

            if (FieldType == "POL" | FieldType == "POD" | FieldType == "PFD")
            {
                SQL.Append( "select p.port_mst_pk from port_mst_tbl p where p.port_id = '" + strID + "' ");
            }
            else if (FieldType == "Customer" | FieldType == "Shipper" | FieldType == "Consignee" | FieldType == "Notify1" | FieldType == "Freight Payer")
            {
                SQL.Append( "select c.customer_mst_pk from customer_mst_tbl c where c.customer_id = '" + strID + "' ");
            }
            else if (FieldType == "Line Operator")
            {
                SQL.Append( "select op.operator_mst_pk from operator_mst_tbl op where op.operator_id = '" + strID + "' ");
            }
            else if (FieldType == "POL Agent" | FieldType == "X-Trade")
            {
                SQL.Append( "select agnt.agent_mst_pk  from agent_mst_tbl agnt where agnt.agent_id = '" + strID + "' ");
            }
            else if (FieldType == "Shippment Term")
            {
                SQL.Append( "select SHPTERM.SHIPPING_TERMS_MST_PK FROM SHIPPING_TERMS_MST_TBL SHPTERM WHERE SHPTERM.INCO_CODE  = '" + strID + "' ");
            }
            else if (FieldType == "Cargo MoveCode")
            {
                SQL.Append( " select  c.cargo_move_pk FROM cargo_move_mst_tbl c where c.cargo_move_code= '" + strID + "' ");
            }
            else if (FieldType == "First Vsl")
            {
                SQL.Append( " select VOYTRN.VOYAGE_TRN_PK  from VESSEL_VOYAGE_TRN VOYTRN, VESSEL_VOYAGE_TBL VSLVOY ");
                SQL.Append("  WHERE(VSLVOY.VESSEL_VOYAGE_TBL_PK = VOYTRN.VESSEL_VOYAGE_TBL_FK) ");
                SQL.Append("  AND VSLVOY.VESSEL_NAME = '" + strID + "' ");
            }
            else if (FieldType == "Commodity Grp")
            {
                SQL.Append( " select commgrp.commodity_group_pk from commodity_group_mst_tbl commgrp where commgrp.commodity_group_code  = '" + strID + "' ");
            }
            else if (FieldType == "Container Type")
            {
                SQL.Append( "select cnt.container_type_mst_pk  from container_type_mst_tbl cnt where cnt.container_type_mst_id  = '" + strID + "' ");
            }
            else if (FieldType == "Pack Type")
            {
                SQL.Append( "select pck.pack_type_mst_pk  from pack_type_mst_tbl pck where pck.pack_type_id  = '" + strID + "' ");
            }
            else if (FieldType == "Commodity")
            {
                SQL.Append( "SELECT comm.commodity_mst_pk FROM commodity_mst_tbl comm where comm.commodity_id ='" + strID + "' ");
            }
            else if (FieldType == "Freight Element")
            {
                SQL.Append( "select frt.freight_element_mst_pk from  freight_element_mst_tbl frt where frt.freight_element_id ='" + strID + "' ");
            }
            else if (FieldType == "Freight Curency")
            {
                SQL.Append( "select curr.currency_mst_pk from currency_type_mst_tbl curr where curr.currency_id ='" + strID + "' ");
            }


            try
            {
                lngPK = Convert.ToInt64(objWF.GetDataTable(SQL.ToString()).Rows[0][0]);
                return lngPK;

            }
            catch (OracleException sqlExp)
            {
                return 0;
            }
            catch (Exception exp)
            {
                return 0;
            }

        }
        #endregion

        #region "Fetch Commodity Group"
        public DataSet FetchCommGrp()
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL.Append( "SELECT CG.COMMODITY_GROUP_PK,CG.COMMODITY_GROUP_CODE,CG.COMMODITY_GROUP_DESC,CG.VERSION_NO");
                strSQL.Append( " FROM COMMODITY_GROUP_MST_TBL CG");
                strSQL.Append( "WHERE CG.ACTIVE_FLAG=1");
                strSQL.Append( "    ORDER BY COMMODITY_GROUP_CODE");
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

        //'For WIN Customer Save 
        #region "Temp Customer Save"
        public bool SaveWINCustomerTemp(ref WorkFlow objWK, ref OracleTransaction TRAN, ref DataSet DSCust, ref int Custpk, int CustCat)
        {

            int Biztype = 0;
            int Transaction_type = 0;
            string Category = null;
            Biztype = 3;
            Transaction_type = 0;
            if (CustCat == 1)
            {
                Category = "Shipper";
            }
            else if (CustCat == 2)
            {
                Category = "Consignee";
            }
            else
            {
                Category = "NotifyParty";
            }

            var _with51 = objWK.MyCommand;
            _with51.Parameters.Clear();
            _with51.Transaction = TRAN;
            _with51.CommandType = CommandType.StoredProcedure;
            _with51.CommandText = objWK.MyUserName + ".TEMP_CUSTOMER_TBL_PKG.TEMP_CUSTOMER_TBL_INS";

            //.Parameters.Add("CUSTOMER_ID_IN", DSCust.Tables(0).Rows(0).Item("" & Category & "_ReferenceNumber")).Direction = ParameterDirection.Input
            _with51.Parameters.Add("CUSTOMER_ID_IN", DBNull.Value).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("CUSTOMER_NAME_IN", DSCust.Tables[0].Rows[0]["" + Category + "_Name1"]).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("ACTIVE_FLAG_IN", 1).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("CREDIT_LIMIT_IN", DBNull.Value).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("CREDIT_DAYS_IN", DBNull.Value).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("SECURITY_CHK_REQD_IN", DBNull.Value).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("ACCOUNT_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("BUSINESS_TYPE_IN", Biztype).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("CUSTOMER_TYPE_FK_IN", CustCat).Direction = ParameterDirection.Input;
            //'Newd to save Shipper,Customer
            _with51.Parameters.Add("VAT_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("TRANSACTION_TYPE_IN", Transaction_type).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("RECONCILE_STATUS_IN", DBNull.Value).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("RECONCILED_BY_IN", DBNull.Value).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("PERMANENT_CUST_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
            //'Contact Details
            _with51.Parameters.Add("ADM_ADDRESS_1_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine1"]).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("ADM_ADDRESS_2_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine2"]).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("ADM_ADDRESS_3_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine3"]).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("ADM_ZIP_CODE_IN", DSCust.Tables[0].Rows[0]["" + Category + "_PostalCode"]).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("ADM_CITY_IN", DSCust.Tables[0].Rows[0]["" + Category + "_City"]).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("ADM_CONTACT_PERSON_IN", DSCust.Tables[0].Rows[0]["" + Category + "_ContactPerson"]).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("ADM_PHONE_NO_1_IN", DSCust.Tables[0].Rows[0]["" + Category + "_PhoneNumber"]).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("ADM_FAX_NO_IN", DSCust.Tables[0].Rows[0]["" + Category + "_FaxNumber"]).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("ADM_PHONE_NO_2_IN", DSCust.Tables[0].Rows[0]["" + Category + "_CellNumber"]).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("ADM_EMAIL_ID_IN", DSCust.Tables[0].Rows[0]["" + Category + "_Email"]).Direction = ParameterDirection.Input;
            //'End
            _with51.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
            _with51.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
            try
            {
                _with51.ExecuteNonQuery();
                Custpk = Convert.ToInt32(_with51.Parameters["RETURN_VALUE"].Value);
            }
            catch (Exception ex)
            {
                return false;
            }
            return true;
        }
        public bool SaveWINCustomer(ref WorkFlow objWK, ref OracleTransaction TRAN, ref DataSet DSCust, ref int Custpk, int CustCat)
        {

            int Biztype = 0;
            int Transaction_type = 0;
            int Temp_patry = 0;
            int Adm_loc = 0;
            string Category = null;
            Biztype = 3;
            Transaction_type = 0;
            Temp_patry = 1;
            if (CustCat == 1)
            {
                Category = "Shipper";
            }
            else if (CustCat == 2)
            {
                Category = "Consignee";
            }
            else
            {
                Category = "NotifyParty";
            }

            var _with52 = objWK.MyCommand;
            _with52.Parameters.Clear();
            _with52.Transaction = TRAN;
            _with52.CommandType = CommandType.StoredProcedure;
            _with52.CommandText = objWK.MyUserName + ".TEMP_CUSTOMER_TBL_PKG.TEMP_CUSTOMER_TBL_IMP_INS";

            _with52.Parameters.Add("CUSTOMER_ID_IN", DSCust.Tables[0].Rows[0]["" + Category + "_ReferenceNumber"]).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("CUSTOMER_NAME_IN", DSCust.Tables[0].Rows[0]["" + Category + "_Name1"]).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("BUSINESS_TYPE_IN", Biztype).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("TEMP_PATRY_IN", Temp_patry).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("ACCOUNT_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("CUSTOMER_TYPE_FK_IN", CustCat).Direction = ParameterDirection.Input;
            //'Newd to save Shipper,Customer
            //'Contact Details
            _with52.Parameters.Add("ADM_ADDRESS_1_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine1"]).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("ADM_ADDRESS_2_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine2"]).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("ADM_ADDRESS_3_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine3"]).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("ADM_ZIP_CODE_IN", DSCust.Tables[0].Rows[0]["" + Category + "_PostalCode"]).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("ADM_CITY_IN", DSCust.Tables[0].Rows[0]["" + Category + "_City"]).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("ADM_CONTACT_PERSON_IN", DSCust.Tables[0].Rows[0]["" + Category + "_ContactPerson"]).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("ADM_PHONE_NO_1_IN", DSCust.Tables[0].Rows[0]["" + Category + "_PhoneNumber"]).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("ADM_FAX_NO_IN", DSCust.Tables[0].Rows[0]["" + Category + "_FaxNumber"]).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("ADM_PHONE_NO_2_IN", DSCust.Tables[0].Rows[0]["" + Category + "_CellNumber"]).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("ADM_EMAIL_ID_IN", DSCust.Tables[0].Rows[0]["" + Category + "_Email"]).Direction = ParameterDirection.Input;
            //'End
            _with52.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
            _with52.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
            try
            {
                _with52.ExecuteNonQuery();
                Custpk = Convert.ToInt32(_with52.Parameters["RETURN_VALUE"].Value);
            }
            catch (Exception ex)
            {
                return false;
            }
            return true;
        }
        #endregion

    }
}