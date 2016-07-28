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
    public class clsJobCardAirImport : CommonFeatures
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

        #region "Fetch Main Jobcard for export"

        /// <summary>
        /// Fetches the main job card data.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet FetchMainJobCardData(string jobCardPK = "0")
        {
            StringBuilder strSQL = new StringBuilder();
            strSQL.Append("SELECT");
            strSQL.Append("    job_imp.job_card_air_imp_pk,");
            strSQL.Append("    job_imp.jobcard_ref_no,");
            strSQL.Append("    job_imp.jobcard_date,");
            strSQL.Append("    job_imp.job_card_status,");
            strSQL.Append("    job_imp.job_card_closed_on,");
            strSQL.Append("    job_imp.WIN_XML_STATUS,");
            strSQL.Append("    job_imp.WIN_TOTAL_QTY,");
            strSQL.Append("    job_imp.WIN_REC_QTY,");
            strSQL.Append("    job_imp.WIN_BALANCE_QTY,");
            strSQL.Append("    job_imp.WIN_TOTAL_WT,");
            strSQL.Append("    job_imp.WIN_REC_WT,");
            strSQL.Append("    job_imp.WIN_BALANCE_WT,");
            strSQL.Append("    job_imp.remarks,");
            strSQL.Append("    job_imp.cust_customer_mst_fk,");
            //strSQL.Append(vbCrLf & "    cust.customer_id ""customer_id"",")
            //strSQL.Append(vbCrLf & "    cust.customer_name ""customer_name"",")
            strSQL.Append("     NVL((SELECT CMT.CUSTOMER_ID  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append("     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.CUST_CUSTOMER_MST_FK),");
            strSQL.Append("      (SELECT TMT.CUSTOMER_ID FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append("      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.CUST_CUSTOMER_MST_FK)) \"customer_id\",");
            strSQL.Append("     NVL((SELECT CMT.CUSTOMER_NAME  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append("     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.CUST_CUSTOMER_MST_FK),");
            strSQL.Append("      (SELECT TMT.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append("      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.CUST_CUSTOMER_MST_FK)) \"customer_name\",");
            strSQL.Append("    del_place_mst_fk,");
            strSQL.Append("    upper(del_place.place_name) \"DeliveryPlace\",");
            strSQL.Append("    port_mst_pol_fk,");
            strSQL.Append("    pol.port_id \"POL\",");
            strSQL.Append("    port_mst_pod_fk,");
            strSQL.Append("    pod.port_id \"POD\",");
            strSQL.Append("    job_imp.airline_mst_fk,");
            strSQL.Append("    airline.airline_id \"airline_id\",");
            strSQL.Append("    airline.airline_name \"airline_name\",");
            strSQL.Append("    flight_no ,");
            strSQL.Append("    job_imp.AIRLINE_SCHEDULE_TRN_FK,");
            strSQL.Append("    eta_date,");
            strSQL.Append("    etd_date,");
            strSQL.Append("    arrival_date,");
            strSQL.Append("    departure_date,");
            strSQL.Append("    shipper_cust_mst_fk,");
            //strSQL.Append(vbCrLf & "    shipper.customer_id ""Shipper"",")
            //strSQL.Append(vbCrLf & "    shipper.customer_name ""ShipperName"",")
            strSQL.Append("     NVL((SELECT CMT.CUSTOMER_ID  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append("     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.SHIPPER_CUST_MST_FK),");
            strSQL.Append("      (SELECT TMT.CUSTOMER_ID FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append("      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.SHIPPER_CUST_MST_FK)) \"Shipper\",");
            strSQL.Append("     NVL((SELECT CMT.CUSTOMER_NAME  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append("     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.SHIPPER_CUST_MST_FK),");
            strSQL.Append("      (SELECT TMT.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append("      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.SHIPPER_CUST_MST_FK)) \"ShipperName\",");
            strSQL.Append("    consignee_cust_mst_fk,");
            //strSQL.Append(vbCrLf & "    consignee.customer_id ""Consignee"",")
            //strSQL.Append(vbCrLf & "    consignee.customer_name ""ConsigneeName"",")
            strSQL.Append("     NVL((SELECT CMT.CUSTOMER_ID  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append("     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.CONSIGNEE_CUST_MST_FK),");
            strSQL.Append("      (SELECT TMT.CUSTOMER_ID FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append("      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.CONSIGNEE_CUST_MST_FK)) \"Consignee\",");
            strSQL.Append("     NVL((SELECT CMT.CUSTOMER_NAME  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append("     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.CONSIGNEE_CUST_MST_FK),");
            strSQL.Append("      (SELECT TMT.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append("      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.CONSIGNEE_CUST_MST_FK)) \"ConsigneeName\",");
            strSQL.Append("    notify1_cust_mst_fk,");
            //strSQL.Append(vbCrLf & "    notify1.customer_id ""Notify1"",")
            //strSQL.Append(vbCrLf & "    notify1.customer_name ""Notify1Name"",")
            strSQL.Append("     NVL((SELECT CMT.CUSTOMER_ID  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append("     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.NOTIFY1_CUST_MST_FK),");
            strSQL.Append("      (SELECT TMT.CUSTOMER_ID FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append("      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.NOTIFY1_CUST_MST_FK)) \"Notify1\",");
            strSQL.Append("     NVL((SELECT CMT.CUSTOMER_NAME  FROM CUSTOMER_MST_TBL CMT ");
            strSQL.Append("     WHERE CMT.CUSTOMER_MST_PK =  JOB_IMP.NOTIFY1_CUST_MST_FK),");
            strSQL.Append("      (SELECT TMT.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL TMT ");
            strSQL.Append("      WHERE TMT.CUSTOMER_MST_PK =  JOB_IMP.NOTIFY1_CUST_MST_FK)) \"Notify1Name\",");

            strSQL.Append("    notify2_cust_mst_fk,");
            strSQL.Append("    notify2.customer_id \"Notify2\",");
            strSQL.Append("    notify2.customer_name \"Notify2Name\",");
            strSQL.Append("    cb_agent_mst_fk,");
            strSQL.Append("    cbagnt.agent_id \"cbAgent\",");
            strSQL.Append("    cbagnt.agent_name \"cbAgentName\",");
            strSQL.Append("    cl_agent_mst_fk,");
            strSQL.Append("    clagnt.agent_id \"clAgent\",");
            strSQL.Append("    clagnt.agent_name \"clAgentName\",");
            strSQL.Append("    job_imp.version_no,");
            strSQL.Append("    ucr_no,");
            strSQL.Append("    goods_description,");
            strSQL.Append("    job_imp.del_address,");
            strSQL.Append("    hawb_ref_no,");
            strSQL.Append("    hawb_date,");
            strSQL.Append("    mawb_ref_no,");
            strSQL.Append("    mawb_date,");
            strSQL.Append("    marks_numbers,");
            strSQL.Append("    weight_mass,");
            strSQL.Append("    job_imp.cargo_move_fk,");
            strSQL.Append("    job_imp.pymt_type,");
            strSQL.Append("    job_imp.shipping_terms_mst_fk,");
            strSQL.Append("    job_imp.insurance_amt,");
            strSQL.Append("    job_imp.insurance_currency,");
            strSQL.Append("    job_imp.pol_agent_mst_fk,");
            strSQL.Append("    polagnt.agent_id \"polAgent\",");
            strSQL.Append("    polagnt.agent_name \"polAgentName\", ");
            strSQL.Append("    job_imp.commodity_group_fk,");
            strSQL.Append("    job_imp.commodity_group_fk,");
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
            strSQL.Append("    job_imp.da_number \"da_number\",");
            strSQL.Append("    job_imp.clearance_address, ");
            //JC_AUTO_MANUAL
            strSQL.Append("    job_imp.JC_AUTO_MANUAL , job_imp.HAWB_SURRDT HAWBSURRDT,job_imp.MAWB_SURRDT MAWBSURRDT, ");
            //
            strSQL.Append("    job_imp.sb_number,job_imp.sb_date, ");
            strSQL.Append("  curr.currency_id,");
            strSQL.Append("  curr.currency_mst_pk base_currency_fk,");
            strSQL.Append("  job_imp.LC_SHIPMENT,");
            strSQL.Append("  job_imp.Lc_Number,");
            strSQL.Append("  job_imp.Lc_Date,");
            strSQL.Append("  job_imp.Lc_Expires_On,");
            strSQL.Append("  job_imp.Lc_Cons_Bank,");
            strSQL.Append("  job_imp.Lc_Remarks,");
            strSQL.Append("  job_imp.Bro_Received,");
            strSQL.Append("  job_imp.Bro_Number,");
            strSQL.Append("  job_imp.Bro_Date,");
            strSQL.Append("  job_imp.Bro_Issuedby,");
            strSQL.Append("  job_imp.Bro_Remarks, ");
            strSQL.Append("  job_imp.CRQ_DATE,");
            strSQL.Append("  job_imp.CRQ,");
            strSQL.Append("  job_imp.PO_NUMBER,");
            strSQL.Append("  job_imp.PO_DATE,");
            strSQL.Append("  job_imp.ROUTING_INST,");
            strSQL.Append("  job_imp.PO_SEND_ON_DATE,");
            strSQL.Append("  job_imp.DO_STATUS, ");
            strSQL.Append("    NVL(job_imp.CHK_CSR,1) CHK_CSR,");
            //strSQL.Append(vbCrLf & "    NVL(EMP.EMPLOYEE_MST_PK,NVL(CONS_SE.EMPLOYEE_MST_PK,0)) SALES_EXEC_FK,")
            //strSQL.Append(vbCrLf & "    NVL(EMP.EMPLOYEE_ID,CONS_SE.EMPLOYEE_ID) SALES_EXEC_ID,")
            //strSQL.Append(vbCrLf & "    NVL(EMP.EMPLOYEE_NAME,CONS_SE.EMPLOYEE_NAME) SALES_EXEC_NAME, ")
            strSQL.Append("    NVL(EMP.EMPLOYEE_MST_PK,(SELECT NVL(CONS_SE.EMPLOYEE_MST_PK,0) FROM EMPLOYEE_MST_TBL CONS_SE,CUSTOMER_MST_TBL CONSIG ");
            strSQL.Append("    WHERE CONS_SE.EMPLOYEE_MST_PK(+)= CONSIG.REP_EMP_MST_FK ");
            strSQL.Append("  AND JOB_IMP.CUST_CUSTOMER_MST_FK = CONSIG.CUSTOMER_MST_PK)) SALES_EXEC_FK,");
            strSQL.Append("    NVL(EMP.EMPLOYEE_ID,(SELECT NVL(CONS_SE.EMPLOYEE_ID,0) FROM EMPLOYEE_MST_TBL CONS_SE,CUSTOMER_MST_TBL CONSIG ");
            strSQL.Append("    WHERE CONS_SE.EMPLOYEE_MST_PK(+)= CONSIG.REP_EMP_MST_FK ");
            strSQL.Append("  AND JOB_IMP.CUST_CUSTOMER_MST_FK = CONSIG.CUSTOMER_MST_PK)) SALES_EXEC_ID,");
            strSQL.Append("    NVL(EMP.EMPLOYEE_NAME,(SELECT NVL(CONS_SE.EMPLOYEE_NAME,0) FROM EMPLOYEE_MST_TBL CONS_SE,CUSTOMER_MST_TBL CONSIG ");
            strSQL.Append("    WHERE CONS_SE.EMPLOYEE_MST_PK(+)= CONSIG.REP_EMP_MST_FK ");
            strSQL.Append("  AND JOB_IMP.CUST_CUSTOMER_MST_FK = CONSIG.CUSTOMER_MST_PK)) SALES_EXEC_NAME,");
            strSQL.Append("    nvl(job_imp.chk_can,0) chk_can,nvl(job_imp.chk_do,0) chk_do,nvl(job_imp.chk_rec,0) chk_rec,nvl(job_imp.chk_pay,0) chk_pay,");
            strSQL.Append("    nvl(job_imp.cc_req,0) cc_req,nvl(job_imp.cc_ie,0) cc_ie,job_imp.LINE_BKG_NR,job_imp.LINE_BKG_DT,job_imp.LINER_TERMS_FK,job_imp.ONC_FK,job_imp.ONC_MOVE_FK,");
            strSQL.Append("    job_imp.CHA_AGENT_MST_FK, ");
            strSQL.Append("    CHAAGNT.VENDOR_ID \"CHAAgentID\",");
            strSQL.Append("    CHAAGNT.VENDOR_NAME \"CHAAgentName\", ");
            //'Win Intigration
            strSQL.Append("    job_imp.WIN_UNIQ_REF_ID,");
            strSQL.Append("    job_imp.WIN_SEND_USER_NAME,");
            strSQL.Append("    job_imp.WIN_SEND_SECRET_KEY,");
            strSQL.Append("    job_imp.WIN_MEM_JOBREF_NR,");
            strSQL.Append("    job_imp.RFS_DATE,");
            strSQL.Append("    job_imp.RFS,");
            strSQL.Append("    job_imp.WIN_QUOT_REF_NR,");
            strSQL.Append("    job_imp.WIN_INCO_PLACE,");
            strSQL.Append("    job_imp.WIN_AOO_PK, ");
            strSQL.Append("    job_imp.WIN_PICK_ONC_MOVE_FK, ");
            strSQL.Append("    job_imp.WIN_CONSOL_REF_NR");
            //'End
            strSQL.Append(" FROM ");
            strSQL.Append("    job_card_air_imp_tbl job_imp,");
            strSQL.Append("    port_mst_tbl POD,");
            strSQL.Append("    port_mst_tbl POL,");
            //strSQL.Append(vbCrLf & "    customer_mst_tbl cust,")
            //strSQL.Append(vbCrLf & "    customer_mst_tbl consignee,")
            //strSQL.Append(vbCrLf & "    customer_mst_tbl shipper,")
            //strSQL.Append(vbCrLf & "    customer_mst_tbl notify1,")
            strSQL.Append("    customer_mst_tbl notify2,");
            strSQL.Append("    place_mst_tbl del_place,");
            strSQL.Append("    airline_mst_tbl airline,");
            strSQL.Append("    agent_mst_tbl clagnt, ");
            strSQL.Append("    agent_mst_tbl cbagnt,");
            strSQL.Append("    agent_mst_tbl polagnt,");
            strSQL.Append("    commodity_group_mst_tbl comm, ");
            strSQL.Append("    vendor_mst_tbl  depot,");
            strSQL.Append("    vendor_mst_tbl  carrier,");
            strSQL.Append("    country_mst_tbl country,");
            strSQL.Append("     currency_type_mst_tbl   curr,");
            strSQL.Append("    EMPLOYEE_MST_TBL        EMP, ");
            //strSQL.Append(vbCrLf & "    EMPLOYEE_MST_TBL        CONS_SE,") 'CONSIGNEE SALES PERSON
            strSQL.Append("    VENDOR_MST_TBL          CHAAGNT ");
            strSQL.Append("  WHERE ");
            strSQL.Append("    job_imp.job_card_air_imp_pk          = " + jobCardPK);
            strSQL.Append("    AND job_imp.port_mst_pol_fk          =  pol.port_mst_pk");
            strSQL.Append("    AND job_imp.port_mst_pod_fk          =  pod.port_mst_pk");
            strSQL.Append("    AND job_imp.del_place_mst_fk         =  del_place.place_pk(+)");
            //strSQL.Append(vbCrLf & "    AND job_imp.cust_customer_mst_fk     =  cust.customer_mst_pk(+) ")
            strSQL.Append("    AND job_imp.airline_mst_fk           =  airline.airline_mst_pk");
            //strSQL.Append(vbCrLf & "    AND job_imp.shipper_cust_mst_fk      =  shipper.customer_mst_pk(+)")
            //strSQL.Append(vbCrLf & "    AND job_imp.consignee_cust_mst_fk    =  consignee.customer_mst_pk(+)")
            //strSQL.Append(vbCrLf & "    AND job_imp.notify1_cust_mst_fk      =  notify1.customer_mst_pk(+)")
            strSQL.Append("    AND job_imp.Notify2_Cust_Mst_Fk      =  notify2.customer_mst_pk(+)");
            strSQL.Append("    AND job_imp.cl_agent_mst_fk          =  clagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_imp.Cb_Agent_Mst_Fk          =  cbagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_imp.pol_agent_mst_fk         =  polagnt.agent_mst_pk(+)");
            strSQL.Append("    AND job_imp.commodity_group_fk       =  comm.commodity_group_pk(+)");

            //conditions are added after the UAT..
            strSQL.Append("    AND job_imp.transporter_depot_fk     =  depot.vendor_mst_pk(+)");
            strSQL.Append("    AND job_imp.transporter_carrier_fk   =  carrier.vendor_mst_pk(+)");
            strSQL.Append("    AND job_imp.country_origin_fk        =  country.country_mst_pk(+)");

            //strSQL.Append(vbCrLf & "    AND consignee.REP_EMP_MST_FK=CONS_SE.EMPLOYEE_MST_PK(+) ")
            strSQL.Append("    AND JOB_IMP.EXECUTIVE_MST_FK=EMP.EMPLOYEE_MST_PK(+) ");
            //added by surya prasad for introducing base currency
            strSQL.Append("     and curr.currency_mst_pk(+) = job_imp.base_currency_fk");
            strSQL.Append("     AND job_imp.CHA_AGENT_MST_FK = CHAAGNT.VENDOR_MST_PK(+) ");
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

        #region "Get container data"

        /// <summary>
        /// Gets the container data.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet GetContainerData(string jobCardPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT ");
            SQL.Append("      job_trn_cont_pk,");
            SQL.Append("      palette_size,");
            //SQL.Append(vbCrLf & "     '' commodity_name,")
            SQL.Append("       ROWTOCOL('SELECT CM.COMMODITY_NAME FROM COMMODITY_MST_TBL CM WHERE CM.COMMODITY_MST_PK IN(' ||");
            SQL.Append("                NVL(CONT_TRN.COMMODITY_MST_FKS, 0) || ')') commodity_name,");
            //SQL.Append(vbCrLf & "      pack_type_mst_fk,")
            SQL.Append("   (SELECT ROWTOCOL('SELECT PT.PACK_TYPE_DESC FROM PACK_TYPE_MST_TBL PT WHERE PT.PACK_TYPE_MST_PK IN (");
            SQL.Append("     SELECT DISTINCT JC.PACK_TYPE_FK FROM job_trn_cont JOB,JOB_TRN_COMMODITY JC ");
            SQL.Append("     WHERE JOB.job_trn_cont_pk=JC.JOB_TRN_CONT_FK ");
            SQL.Append("     AND JOB.job_trn_cont_pk='||CONT_TRN.job_trn_cont_pk||')') FROM DUAL) PACK_TYPE_MST_FK, ");
            SQL.Append("      pack_count,");
            SQL.Append("      gross_weight,");
            SQL.Append("      volume_in_cbm,");
            //SQL.Append(vbCrLf & "      gross_weight,")
            SQL.Append("      chargeable_weight,");
            //SQL.Append(vbCrLf & "      pack_type_mst_fk,")
            //SQL.Append(vbCrLf & "      pack_count,")
            //SQL.Append(vbCrLf & "      commodity_name,")
            SQL.Append("      to_char(load_date,Datetimeformat24) load_date,");
            SQL.Append("     CONT_TRN.COMMODITY_MST_FKS COMMODITY_MST_FK,'false' as \"Delete\" ,cont_trn.PREV_CONT_PK ");
            SQL.Append("FROM");
            SQL.Append("      job_trn_cont cont_trn,");
            SQL.Append("      JOB_CARD_TRN job_card,");
            SQL.Append("      pack_type_mst_tbl pack,");
            SQL.Append("      commodity_mst_tbl comm");
            SQL.Append("WHERE");
            SQL.Append("      cont_trn.job_card_trn_fk =" + jobCardPK);
            SQL.Append("      AND cont_trn.job_card_trn_fk = job_card.JOB_CARD_TRN_PK");
            SQL.Append("      AND cont_trn.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
            SQL.Append("      AND cont_trn.commodity_mst_fk = comm.commodity_mst_pk(+)");

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

        /// <summary>
        /// Gets the container data new.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet GetContainerDataNew(string jobCardPK = "0")
        {
            //'since Same function using in EDI that's y Created new

            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT ");
            SQL.Append("      job_trn_air_imp_cont_pk,");
            SQL.Append("      palette_size,");
            SQL.Append("      '' commodity_name,");
            SQL.Append("      pack_type_mst_fk,");
            SQL.Append("      pack_count,");
            SQL.Append("      gross_weight,");
            SQL.Append("      volume_in_cbm,");
            //SQL.Append(vbCrLf & "      gross_weight,")
            SQL.Append("      chargeable_weight,");
            //SQL.Append(vbCrLf & "      pack_type_mst_fk,")
            //SQL.Append(vbCrLf & "      pack_count,")
            //SQL.Append(vbCrLf & "      commodity_name,")
            SQL.Append("      to_char(load_date,Datetimeformat24) load_date,");
            SQL.Append("     CONT_TRN.COMMODITY_MST_FKS COMMODITY_MST_FK,'false' as \"Delete\" ");
            SQL.Append("FROM");
            SQL.Append("      job_trn_air_imp_cont cont_trn,");
            SQL.Append("      job_card_air_imp_tbl job_card,");
            SQL.Append("      pack_type_mst_tbl pack,");
            SQL.Append("      commodity_mst_tbl comm");
            SQL.Append("WHERE");
            SQL.Append("      cont_trn.job_card_air_imp_fk =" + jobCardPK);
            SQL.Append("      AND cont_trn.job_card_air_imp_fk = job_card.job_card_air_imp_pk");
            SQL.Append("      AND cont_trn.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
            SQL.Append("      AND cont_trn.commodity_mst_fk = comm.commodity_mst_pk(+)");

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

        //add by latha for fetching the login location id
        /// <summary>
        /// Fetchlocationids this instance.
        /// </summary>
        /// <returns></returns>
        public string fetchlocationid()
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();
            SQL.Append("SELECT ");
            SQL.Append("      l.location_id ");
            SQL.Append("      from location_mst_tbl l ");
            SQL.Append("      where l.location_mst_pk = " + LoggedIn_Loc_FK);
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

        #endregion "Get container data"

        #region "Frieght Element"

        /// <summary>
        /// Fetches the fret.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <returns></returns>
        public DataSet FetchFret(int jobcardpk)
        {
            string strsql = null;
            WorkFlow objWF = new WorkFlow();
            //strsql = "select * from  CONSOL_INVOICE_TRN_TBl where JOB_CARD_FK = " & jobcardpk & " AND FRT_OTH_ELEMENT = 1"
            //Modified by Snigdharani to get the actual invoiced frt elements... 24/12/2009
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT CONS.CONSOL_INVOICE_PK        INVPK,");
            sb.Append("       CONSTRN.CONSOL_INVOICE_TRN_PK TRNPK,");
            sb.Append("       CONSTRN.CURRENCY_MST_FK       CURRPK,");
            sb.Append("       CONSTRN.ELEMENT_AMT           AMT,");
            sb.Append("       CONSTRN.FRT_OTH_ELEMENT_FK    FRTPK");
            sb.Append("  FROM CONSOL_INVOICE_TBL CONS, CONSOL_INVOICE_TRN_TBL CONSTRN");
            sb.Append(" WHERE CONS.CONSOL_INVOICE_PK = CONSTRN.CONSOL_INVOICE_FK");
            sb.Append("   AND CONSTRN.JOB_CARD_FK = " + jobcardpk);
            sb.Append("   AND CONSTRN.FRT_OTH_ELEMENT = 1");
            sb.Append("   AND CONS.PROCESS_TYPE = 2");
            sb.Append("   AND CONS.BUSINESS_TYPE = 1");
            sb.Append(" UNION");
            sb.Append(" SELECT INV.INV_AGENT_PK        INVPK,");
            sb.Append("       INVTRN.INV_AGENT_TRN_PK TRNPK,");
            sb.Append("       INVTRN.CURRENCY_MST_FK          CURRPK,");
            sb.Append("       INVTRN.ELEMENT_AMT              AMT,");
            sb.Append("       INVTRN.COST_FRT_ELEMENT_FK      FRTPK");
            sb.Append("  FROM INV_AGENT_TBL INV, INV_AGENT_TRN_TBL INVTRN");
            sb.Append(" WHERE INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
            sb.Append("   AND INVTRN.COST_FRT_ELEMENT = 2");
            sb.Append("   AND INV.JOB_CARD_AIR_IMP_FK = " + jobcardpk);
            strsql = sb.ToString();
            try
            {
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

        #region "Get Freight data"

        /// <summary>
        /// Gets the FRT det.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet GetFrtDet(string jobCardPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");

            SQL.Append("   fd_trn.inv_cust_trn_air_imp_fk,");
            SQL.Append("   fd_trn.inv_agent_trn_air_imp_fk,");
            SQL.Append("   fd_trn.consol_invoice_trn_fk");

            SQL.Append("FROM");
            SQL.Append("   job_trn_air_imp_fd fd_trn,");
            SQL.Append("   job_card_air_imp_tbl job_card,");
            SQL.Append("   currency_type_mst_tbl curr,");
            SQL.Append("   freight_element_mst_tbl frt,");
            SQL.Append("   location_mst_tbl loc,");
            SQL.Append("   customer_mst_tbl cus");

            SQL.Append("WHERE");
            SQL.Append("   fd_trn.job_card_air_imp_fk = " + jobCardPK);
            SQL.Append("   AND fd_trn.job_card_air_imp_fk = job_card.job_card_air_imp_pk");
            SQL.Append("   AND fd_trn.currency_mst_fk = curr.currency_mst_pk(+)");
            SQL.Append("   AND fd_trn.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            SQL.Append("   AND fd_trn.frtpayer_cust_mst_fk = cus.customer_mst_pk(+)");
            SQL.Append("   AND NVL(fd_trn.SERVICE_TYPE_FLAG,0)<>1");
            SQL.Append("   AND fd_trn.location_mst_fk = loc.location_mst_pk(+)");

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

        /// <summary>
        /// Gets the freight data.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="BaseCurrFk">The base curr fk.</param>
        /// <returns></returns>
        public DataSet GetFreightData(string jobCardPK = "0", string BaseCurrFk = "0")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();
            SQL.Append(" SELECT QRY.* FROM (");
            SQL.Append("SELECT");
            SQL.Append("   job_trn_fd_pk,");
            SQL.Append("   frt.freight_element_id,");
            SQL.Append("   frt.freight_element_name,");
            SQL.Append("   frt.freight_element_mst_pk,");
            SQL.Append("   DECODE(fd_trn.basis,0,' ',1,'%',2,'Flat rate',3,'Kgs',4,'Unit') basis,");
            SQL.Append("   quantity,");
            SQL.Append("   DECODE(fd_trn.freight_type,1,'Prepaid',2,'Collect') freight_type,");
            SQL.Append("   fd_trn.location_mst_fk  \"location_fk\" ,");
            SQL.Append("   loc.location_id \"location_id\" ,");
            SQL.Append("   fd_trn.frtpayer_cust_mst_fk \"frtpayer_mst_fk\" ,");
            SQL.Append("   cus.customer_id \"frtpayer\",");
            SQL.Append("   fd_trn.currency_mst_fk,");
            ///'
            SQL.Append("   freight_amt,");
            if (Convert.ToInt32(BaseCurrFk) != 0)
            {
                SQL.Append("       NVL(GET_EX_RATE(fd_trn.CURRENCY_MST_FK, " + BaseCurrFk + ", job_card.JOBCARD_DATE),0) AS ROE,");
                SQL.Append("       (fd_trn.freight_amt *  NVL(GET_EX_RATE(fd_trn.CURRENCY_MST_FK, " + BaseCurrFk + ", job_card.JOBCARD_DATE),0)) total_amt,");
            }
            else
            {
                SQL.Append("   EXCHANGE_RATE \"ROE\",");
                SQL.Append("  (fd_trn.freight_amt*fd_trn.EXCHANGE_RATE) total_amt,");
            }
            SQL.Append("   '0' \"Delete\"");
            SQL.Append("FROM");
            SQL.Append("   job_trn_fd fd_trn,");
            SQL.Append("   JOB_CARD_TRN job_card,");
            SQL.Append("   currency_type_mst_tbl curr,");
            SQL.Append("    parameters_tbl prm,");
            SQL.Append("   freight_element_mst_tbl frt,");
            SQL.Append("   location_mst_tbl loc,");
            SQL.Append("   customer_mst_tbl cus");
            SQL.Append("WHERE");
            SQL.Append("   fd_trn.job_card_trn_fk = " + jobCardPK);
            SQL.Append("   AND fd_trn.job_card_trn_fk = job_card.JOB_CARD_TRN_PK");
            SQL.Append("   AND fd_trn.currency_mst_fk = curr.currency_mst_pk(+)");
            SQL.Append("   AND fd_trn.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            SQL.Append("   AND fd_trn.freight_element_mst_fk = prm.frt_afc_fk");
            SQL.Append("   AND fd_trn.frtpayer_cust_mst_fk = cus.customer_mst_pk(+)");
            SQL.Append("   AND NVL(fd_trn.SERVICE_TYPE_FLAG,0)<>1");
            SQL.Append("   AND fd_trn.location_mst_fk = loc.location_mst_pk(+)");
            SQL.Append(" union all ");
            SQL.Append("SELECT");
            SQL.Append("   job_trn_fd_pk,");
            SQL.Append("   frt.freight_element_id,");
            SQL.Append("   frt.freight_element_name,");
            SQL.Append("   frt.freight_element_mst_pk,");
            SQL.Append("   DECODE(fd_trn.basis,0,' ',1,'%',2,'Flat rate',3,'Kgs',4,'Unit') basis,");
            SQL.Append("   quantity,");
            SQL.Append("   DECODE(fd_trn.freight_type,1,'Prepaid',2,'Collect') freight_type,");
            SQL.Append("   fd_trn.location_mst_fk  \"location_fk\" ,");
            SQL.Append("   loc.location_id \"location_id\" ,");
            SQL.Append("   fd_trn.frtpayer_cust_mst_fk \"frtpayer_mst_fk\" ,");
            SQL.Append("   cus.customer_id \"frtpayer\",");
            SQL.Append("   fd_trn.currency_mst_fk,");
            ///
            SQL.Append("   freight_amt,");
            if (Convert.ToInt32(BaseCurrFk) != 0)
            {
                SQL.Append("       NVL(GET_EX_RATE(fd_trn.CURRENCY_MST_FK, " + BaseCurrFk + ", job_card.JOBCARD_DATE),0) AS ROE,");
                SQL.Append("       (fd_trn.freight_amt *  NVL(GET_EX_RATE(fd_trn.CURRENCY_MST_FK, " + BaseCurrFk + ", job_card.JOBCARD_DATE),0)) total_amt,");
            }
            else
            {
                SQL.Append("   EXCHANGE_RATE \"ROE\",");
                SQL.Append("  (fd_trn.freight_amt*fd_trn.EXCHANGE_RATE) total_amt,");
            }
            SQL.Append("   '0' \"Delete\"");
            SQL.Append("FROM");
            SQL.Append("   job_trn_fd fd_trn,");
            SQL.Append("   JOB_CARD_TRN job_card,");
            SQL.Append("   currency_type_mst_tbl curr,");
            SQL.Append("    parameters_tbl prm,");
            SQL.Append("   freight_element_mst_tbl frt,");
            SQL.Append("   location_mst_tbl loc,");
            SQL.Append("   customer_mst_tbl cus");
            SQL.Append("WHERE");
            SQL.Append("   fd_trn.job_card_trn_fk = " + jobCardPK);
            SQL.Append("   AND fd_trn.job_card_trn_fk = job_card.JOB_CARD_TRN_PK");
            SQL.Append("   AND fd_trn.currency_mst_fk = curr.currency_mst_pk(+)");
            SQL.Append("   AND fd_trn.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            SQL.Append("   AND fd_trn.freight_element_mst_fk not in prm.frt_afc_fk");
            SQL.Append("   AND fd_trn.frtpayer_cust_mst_fk = cus.customer_mst_pk(+)");
            SQL.Append("   AND NVL(fd_trn.SERVICE_TYPE_FLAG,0)<>1");
            SQL.Append("   AND fd_trn.location_mst_fk = loc.location_mst_pk(+)");
            SQL.Append("     ) QRY,FREIGHT_ELEMENT_MST_TBL FMT ");
            SQL.Append("     WHERE QRY.FREIGHT_ELEMENT_MST_PK=FMT.FREIGHT_ELEMENT_MST_PK");
            SQL.Append("             ORDER BY FMT.PREFERENCE");

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

        #endregion "Get Freight data"

        #region "Get transshipment details"

        /// <summary>
        /// Gets the transshipment data.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet GetTransshipmentData(string jobCardPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("   tp_trn.job_trn_air_imp_tp_pk,");
            SQL.Append("   tp_trn.transhipment_no,");
            SQL.Append("   tp_trn.port_mst_fk,");
            SQL.Append("   port.port_id,");
            SQL.Append("   port.port_name,");
            SQL.Append("   DECODE(tp_trn.WIN_TYPE,1,'Main Carriage',2,'On Carriage') WIN_TYPE,");
            SQL.Append("   (SELECT QDT.DD_ID FROM QFOR_DROP_DOWN_TBL QDT ");
            SQL.Append("   WHERE QDT.DD_FLAG = 'ACarriage_Mode_IMP_POD' ");
            SQL.Append("   AND QDT.DROPDOWN_PK = tp_trn.WIN_MODE) WIN_MODE,");
            SQL.Append("   tp_trn.airline_mst_fk,");
            SQL.Append("   tp_trn.flight_no,");
            SQL.Append("   tp_trn.eta_date,");
            SQL.Append("   tp_trn.etd_date,");
            SQL.Append("   '0' \"Delete\"");
            SQL.Append("FROM");
            SQL.Append("   job_trn_air_imp_tp tp_trn,");
            SQL.Append("   job_card_air_imp_tbl job_card,");
            SQL.Append("   port_mst_tbl port");
            SQL.Append("WHERE");
            SQL.Append("   tp_trn.job_card_air_imp_fk =" + jobCardPK);
            SQL.Append("   AND tp_trn.job_card_air_imp_fk = job_card.job_card_air_imp_pk");
            SQL.Append("   AND tp_trn.port_mst_fk = port.port_mst_pk(+)");

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

        #endregion "Get transshipment details"

        #region "Fetch PIA details"

        //added by manoharan 4/11/2006
        /// <summary>
        /// Gets the pia.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet GetPIA(string jobCardPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("   pia_trn.inv_cust_trn_air_imp_fk,");
            SQL.Append("   pia_trn.inv_agent_trn_air_imp_fk,");
            SQL.Append("   pia_trn.inv_supplier_fk");

            SQL.Append("FROM");
            SQL.Append("   job_trn_air_imp_pia pia_trn,");
            SQL.Append("   cost_element_mst_tbl cost_elmnt,");
            SQL.Append("   currency_type_mst_tbl curr,");
            SQL.Append("   job_card_air_imp_tbl job_card");
            SQL.Append("WHERE");
            SQL.Append("   pia_trn.job_card_air_imp_fk =" + jobCardPK);
            SQL.Append("   AND pia_trn.job_card_air_imp_fk = job_card.job_card_air_imp_pk");
            SQL.Append("   AND pia_trn.cost_element_mst_fk = cost_elmnt.cost_element_mst_pk(+)");
            SQL.Append("   AND pia_trn.currency_mst_fk     = curr.currency_mst_pk(+)");

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

        /// <summary>
        /// Gets the pia data.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet GetPIAData(string jobCardPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder SQL = new StringBuilder();

            SQL.Append("SELECT");
            SQL.Append("   pia_trn.job_trn_air_imp_pia_pk,");
            SQL.Append("   pia_trn.vendor_key,");
            SQL.Append("   cost_element_id,");
            SQL.Append("   pia_trn.invoice_number,");
            SQL.Append("   to_char(pia_trn.invoice_date, dateformat) invoice_date,");
            SQL.Append("   pia_trn.currency_mst_fk,");
            SQL.Append("   pia_trn.invoice_amt,");
            SQL.Append("   pia_trn.tax_percentage,");
            SQL.Append("   pia_trn.tax_amt,");
            SQL.Append("   pia_trn.estimated_amt,");
            SQL.Append("   invoice_amt - estimated_amt diff_amount,");
            SQL.Append("   pia_trn.vendor_mst_fk,");
            SQL.Append("   pia_trn.cost_element_mst_fk,");
            SQL.Append("   pia_trn.attached_file_name,'false' as \"Delete\", MJC_TRN_AIR_IMP_PIA_FK ");
            SQL.Append("FROM");
            SQL.Append("   job_trn_air_imp_pia pia_trn,");
            SQL.Append("   cost_element_mst_tbl cost_elmnt,");
            SQL.Append("   currency_type_mst_tbl curr,");
            SQL.Append("   job_card_air_imp_tbl job_card");
            SQL.Append("WHERE");
            SQL.Append("   pia_trn.job_card_air_imp_fk =" + jobCardPK);
            SQL.Append("   AND pia_trn.job_card_air_imp_fk = job_card.job_card_air_imp_pk");
            SQL.Append("   AND pia_trn.cost_element_mst_fk = cost_elmnt.cost_element_mst_pk(+)");
            SQL.Append("   AND pia_trn.currency_mst_fk     = curr.currency_mst_pk(+)");

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

        #endregion "Fetch PIA details"

        #region " Fetch Cost details data export"

        /// <summary>
        /// Fetches the cost detail.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <param name="basecurrency">The basecurrency.</param>
        /// <returns></returns>
        public DataSet FetchCostDetail(string jobCardPK = "0", int basecurrency = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                //'Commented By Koteshwari
                //strSQL.Append(vbCrLf & "SELECT")
                //strSQL.Append(vbCrLf & "    cost_ele.cost_element_name,")
                //strSQL.Append(vbCrLf & "    SUM(job_trn_pia.Estimated_Amt),")
                //strSQL.Append(vbCrLf & "    curr.currency_id,")
                //strSQL.Append(vbCrLf & "    SUM(job_trn_pia.Invoice_Amt),")
                //strSQL.Append(vbCrLf & "    SUM(job_trn_pia.Invoice_Amt - job_trn_pia.Estimated_Amt)")
                //strSQL.Append(vbCrLf & "    FROM")
                //strSQL.Append(vbCrLf & "    job_trn_air_imp_pia  job_trn_pia,")
                //strSQL.Append(vbCrLf & "    currency_type_mst_tbl curr,")
                //strSQL.Append(vbCrLf & "    cost_element_mst_tbl cost_ele,")
                //strSQL.Append(vbCrLf & "    JOB_CARD_TRN job_imp")
                //strSQL.Append(vbCrLf & "    WHERE")
                //strSQL.Append(vbCrLf & "    job_trn_pia.job_card_trn_fk= job_imp.JOB_CARD_TRN_PK")
                //strSQL.Append(vbCrLf & "    AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk")
                //strSQL.Append(vbCrLf & "    AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk")
                //strSQL.Append(vbCrLf & "    AND job_trn_pia.job_card_trn_fk =" + jobCardPK)
                //strSQL.Append(vbCrLf & "    GROUP BY cost_element_name,currency_id")
                //strSQL.Append(vbCrLf & "    ORDER BY cost_element_name")
                strSQL.Append("SELECT JEC.JOB_TRN_COST_PK,");
                strSQL.Append("       JEC.job_card_trn_fk,");
                strSQL.Append("       VMT.VENDOR_MST_PK,");
                strSQL.Append("       CMT.COST_ELEMENT_MST_PK,");
                strSQL.Append("       JEC.VENDOR_KEY,");
                strSQL.Append("       CMT.COST_ELEMENT_ID,");
                strSQL.Append("       CMT.COST_ELEMENT_NAME,");
                strSQL.Append("       DECODE(JEC.PTMT_TYPE,1,'Prepaid',2,'Collect')PTMT_TYPE,");
                strSQL.Append("       LMT.LOCATION_ID,");
                strSQL.Append("       CURR.CURRENCY_ID,");
                ///
                strSQL.Append("       JEC.ESTIMATED_COST,");
                //strSQL.Append("       CURR.CURRENCY_ID,")
                strSQL.Append("       ROUND(GET_EX_RATE_BUY(JEC.CURRENCY_MST_FK, " + basecurrency + ", round(TO_DATE(JOB.JOBCARD_DATE,DATEFORMAT) - .5)), 6) AS ROE,");
                strSQL.Append("       JEC.TOTAL_COST,");
                strSQL.Append("       '' DEL_FLAG,");
                strSQL.Append("       '' SEL_FLAG,");
                strSQL.Append("       JEC.LOCATION_MST_FK,");
                strSQL.Append("       JEC.CURRENCY_MST_FK");
                strSQL.Append("  FROM JOB_TRN_COST JEC,");
                strSQL.Append("       JOB_CARD_TRN  JOB,");
                strSQL.Append("       VENDOR_MST_TBL        VMT,");
                strSQL.Append("       COST_ELEMENT_MST_TBL  CMT,");
                strSQL.Append("       CURRENCY_TYPE_MST_TBL CURR,");
                strSQL.Append("       LOCATION_MST_TBL      LMT");
                strSQL.Append(" WHERE JEC.job_card_trn_fk = JOB.JOB_CARD_TRN_PK");
                strSQL.Append("   AND JEC.COST_ELEMENT_MST_FK = CMT.COST_ELEMENT_MST_PK");
                strSQL.Append("   AND JEC.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
                strSQL.Append("   AND JEC.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                strSQL.Append("   AND JEC.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                strSQL.Append("   AND JOB.JOB_CARD_TRN_PK = " + jobCardPK);
                strSQL.Append("   AND NVL(JEC.SERVICE_TYPE_FLAG,0)<>1 ");
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

        #endregion " Fetch Cost details data export"

        #endregion "GetMainBookingData"

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
        /// <param name="nLocationfk">The n locationfk.</param>
        /// <param name="PODetails">The po details.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <param name="dsDoc">The ds document.</param>
        /// <param name="IsWINSave">The is win save.</param>
        /// <param name="DSShipper">The ds shipper.</param>
        /// <param name="DSCons">The ds cons.</param>
        /// <param name="DSNotify">The ds notify.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet, DataSet dsContainerData, DataSet dsTPDetails, DataSet dsFreightDetails, DataSet dsPurchaseInventory, DataSet dsCostDetails, DataSet dsPickUpDetails, DataSet dsDropDetails, bool isEdting, string ucrNo,
        string jobCardRefNumber, string userLocation, string employeeID, long JobCardPK, DataSet dsOtherCharges, int nLocationfk = 0, string PODetails = "", DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null, DataSet dsDoc = null,
        int IsWINSave = 0, DataSet DSShipper = null, DataSet DSCons = null, DataSet DSNotify = null)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            DataSet dsTrackNTrace = new DataSet();
            int intPKVal = 0;
            int ShipperPK = 0;
            int ConsigneePK = 0;
            int NotifyPK = 0;
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

            OracleCommand insOtherChargesDetails = new OracleCommand();
            OracleCommand updOtherChargesDetails = new OracleCommand();
            OracleCommand delOtherChargesDetails = new OracleCommand();

            //Dim insIncomeChargeDetails As New OracleClient.OracleCommand
            //Dim updIncomeChargeDetails As New OracleClient.OracleCommand
            //Dim delIncomeChargeDetails As New OracleClient.OracleCommand

            //Dim insExpenseChargeDetails As New OracleClient.OracleCommand
            //Dim updExpenseChargeDetails As New OracleClient.OracleCommand
            //Dim delExpenseChargeDetails As New OracleClient.OracleCommand

            if (isEdting == false)
            {
                jobCardRefNumber = GenerateProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(userLocation), Convert.ToInt32(employeeID), System.DateTime.Now, "", "", "", M_LAST_MODIFIED_BY_FK);
            }

            //ucrNo = ucrNo & jobCardRefNumber

            try
            {
                //'For Customer Save
                if (IsWINSave == 1)
                {
                    if ((DSShipper != null))
                    {
                        if (!(SaveWINCustomerTemp(objWK, TRAN, DSShipper, ShipperPK, 1)))
                        {
                            arrMessage.Add("Error while saving Customer");
                            return arrMessage;
                        }
                    }
                    if ((DSCons != null))
                    {
                        if (!(SaveWINCustomerTemp(objWK, TRAN, DSCons, ConsigneePK, 2)))
                        {
                            arrMessage.Add("Error while saving Customer");
                            return arrMessage;
                        }
                    }
                    if ((DSNotify != null))
                    {
                        if (!(SaveWINCustomerTemp(objWK, TRAN, DSNotify, NotifyPK, 3)))
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
                dsTrackNTrace = dsContainerData.Copy();
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_CARD_AIR_IMP_TBL_INS";
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

                insCommand.Parameters.Add("AIRLINE_MST_FK_IN", OracleDbType.Int32, 10, "airline_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["AIRLINE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

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

                insCommand.Parameters.Add("UCR_NO_IN", OracleDbType.Varchar2, 40, "UCR_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["UCR_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WEIGHT_MASS_IN", OracleDbType.Varchar2, 2000, "weight_mass").Direction = ParameterDirection.Input;
                insCommand.Parameters["WEIGHT_MASS_IN"].SourceVersion = DataRowVersion.Current;

                //insCommand.Parameters.Add("UCR_NO_IN", ucrNo).Direction = ParameterDirection.Input

                insCommand.Parameters.Add("HAWB_REF_NO_IN", OracleDbType.Varchar2, 20, "hawb_ref_no").Direction = ParameterDirection.Input;
                insCommand.Parameters["HAWB_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("HAWB_DATE_IN", OracleDbType.Date, 20, "hawb_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["HAWB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MAWB_REF_NO_IN", OracleDbType.Varchar2, 20, "mawb_ref_no").Direction = ParameterDirection.Input;
                insCommand.Parameters["MAWB_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MAWB_DATE_IN", OracleDbType.Date, 20, "mawb_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["MAWB_DATE_IN"].SourceVersion = DataRowVersion.Current;

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

                insCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                //adding by thiyagarajan on 20/4/09
                insCommand.Parameters.Add("HAWBSURRDT_IN", OracleDbType.Date, 20, "HAWBSURRDT").Direction = ParameterDirection.Input;
                insCommand.Parameters["HAWBSURRDT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MAWBSURRDT_IN", OracleDbType.Date, 20, "MAWBSURRDT").Direction = ParameterDirection.Input;
                insCommand.Parameters["MAWBSURRDT_IN"].SourceVersion = DataRowVersion.Current;
                //end

                insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_PK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                //ADDED BY SURYA PRASAD FOR INTRODUCING BASE CURRENCY
                insCommand.Parameters.Add("BASE_CURRENCY_FK_IN", OracleDbType.Int32, 10, "base_currency_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["BASE_CURRENCY_FK_IN"].SourceVersion = DataRowVersion.Current;
                //end

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

                insCommand.Parameters.Add("CHK_CAN_IN", OracleDbType.Int32, 1, "chk_can").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_CAN_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CHK_DO_IN", OracleDbType.Int32, 1, "chk_do").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_DO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CHK_PAY_IN", OracleDbType.Int32, 1, "chk_pay").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_PAY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CHK_REC_IN", OracleDbType.Int32, 1, "chk_rec").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_REC_IN"].SourceVersion = DataRowVersion.Current;
                //'
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CRQ_Date"].ToString()))
                {
                    insCommand.Parameters.Add("CRQ_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("CRQ_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["CRQ_Date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["CRQ_DATE_IN"].SourceVersion = DataRowVersion.Current;
                insCommand.Parameters.Add("CRQ_IN", OracleDbType.Int32, 1, "CRQ").Direction = ParameterDirection.Input;
                //'
                if (PODetails.Length > 0)
                {
                    string[] PO_Details = null;
                    PO_Details = PODetails.Split('~');

                    insCommand.Parameters.Add("PO_NUMBER_IN", (string.IsNullOrEmpty(PO_Details[0].Trim()) ? "" : PO_Details[0])).Direction = ParameterDirection.Input;
                    insCommand.Parameters["PO_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("PO_DATE_IN", (string.IsNullOrEmpty(PO_Details[1].Trim()) ? "" : PO_Details[1])).Direction = ParameterDirection.Input;
                    insCommand.Parameters["PO_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("ROUTING_INST_IN", (string.IsNullOrEmpty(PO_Details[2].Trim()) ? "" : PO_Details[2])).Direction = ParameterDirection.Input;
                    insCommand.Parameters["ROUTING_INST_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("SENT_ON_IN", (string.IsNullOrEmpty(PO_Details[3].Trim()) ? "" : PO_Details[3])).Direction = ParameterDirection.Input;
                    insCommand.Parameters["SENT_ON_IN"].SourceVersion = DataRowVersion.Current;
                }
                else
                {
                    insCommand.Parameters.Add("PO_NUMBER_IN", "").Direction = ParameterDirection.Input;
                    insCommand.Parameters["PO_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("PO_DATE_IN", "").Direction = ParameterDirection.Input;
                    insCommand.Parameters["PO_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("ROUTING_INST_IN", "").Direction = ParameterDirection.Input;
                    insCommand.Parameters["ROUTING_INST_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("SENT_ON_IN", "").Direction = ParameterDirection.Input;
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

                insCommand.Parameters.Add("WIN_AOO_PK_IN", OracleDbType.Int32, 10, "WIN_AOO_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_AOO_PK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_PICK_ONC_MOVE_FK_IN", OracleDbType.Int32, 10, "WIN_PICK_ONC_MOVE_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_PICK_ONC_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIN_CONSOL_REF_NR_IN", OracleDbType.Varchar2, 20, "WIN_CONSOL_REF_NR").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIN_CONSOL_REF_NR_IN"].SourceVersion = DataRowVersion.Current;

                //'End
                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_CARD_AIR_IMP_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_CARD_AIR_IMP_TBL_UPD";
                var _with4 = _with3.Parameters;

                updCommand.Parameters.Add("JOB_CARD_AIR_IMP_PK_IN", OracleDbType.Int32, 10, "JOB_CARD_AIR_IMP_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_AIR_IMP_PK_IN"].SourceVersion = DataRowVersion.Current;

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

                updCommand.Parameters.Add("UCR_NO_IN", OracleDbType.Varchar2, 40, "UCR_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["UCR_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2, 25, "flight_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["FLIGHT_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AIRLINE_SCHEDULE_TRN_FK_IN", OracleDbType.Int32, 10, "AIRLINE_SCHEDULE_TRN_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["AIRLINE_SCHEDULE_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AIRLINE_MST_FK_IN", OracleDbType.Int32, 10, "airline_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["AIRLINE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("HAWB_REF_NO_IN", OracleDbType.Varchar2, 20, "hawb_ref_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["HAWB_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("HAWB_DATE_IN", OracleDbType.Date, 20, "hawb_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["HAWB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MAWB_REF_NO_IN", OracleDbType.Varchar2, 20, "mawb_ref_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["MAWB_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MAWB_DATE_IN", OracleDbType.Date, 20, "mawb_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["MAWB_DATE_IN"].SourceVersion = DataRowVersion.Current;

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

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "version_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

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

                updCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                //adding by thiyagarajan on 20/4/09
                updCommand.Parameters.Add("HAWBSURRDT_IN", OracleDbType.Date, 20, "HAWBSURRDT").Direction = ParameterDirection.Input;
                updCommand.Parameters["HAWBSURRDT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MAWBSURRDT_IN", OracleDbType.Date, 20, "MAWBSURRDT").Direction = ParameterDirection.Input;
                updCommand.Parameters["MAWBSURRDT_IN"].SourceVersion = DataRowVersion.Current;
                //end

                //Code Added By Anil on 17 Aug 09
                updCommand.Parameters.Add("SB_DATE_IN", OracleDbType.Date, 20, "sb_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["SB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SB_NO_IN", OracleDbType.Varchar2, 20, "sb_number").Direction = ParameterDirection.Input;
                updCommand.Parameters["SB_NO_IN"].SourceVersion = DataRowVersion.Current;
                //End By Anil

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

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
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CRQ_Date"].ToString()))
                {
                    updCommand.Parameters.Add("CRQ_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("CRQ_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["CRQ_Date"])).Direction = ParameterDirection.Input;
                }
                updCommand.Parameters["CRQ_DATE_IN"].SourceVersion = DataRowVersion.Current;
                updCommand.Parameters.Add("CRQ_IN", OracleDbType.Int32, 1, "CRQ").Direction = ParameterDirection.Input;

                if (PODetails.Length > 0)
                {
                    string[] PO_Details = null;
                    PO_Details = PODetails.Split('~');
                    updCommand.Parameters.Add("PO_NUMBER_IN", (string.IsNullOrEmpty(PO_Details[0].Trim()) ? "" : PO_Details[0])).Direction = ParameterDirection.Input;
                    updCommand.Parameters["PO_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    updCommand.Parameters.Add("PO_DATE_IN", (string.IsNullOrEmpty(PO_Details[1].Trim()) ? "" : PO_Details[1])).Direction = ParameterDirection.Input;
                    updCommand.Parameters["PO_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updCommand.Parameters.Add("ROUTING_INST_IN", (string.IsNullOrEmpty(PO_Details[2].Trim()) ? "" : PO_Details[2])).Direction = ParameterDirection.Input;
                    updCommand.Parameters["ROUTING_INST_IN"].SourceVersion = DataRowVersion.Current;

                    updCommand.Parameters.Add("SENT_ON_IN", (string.IsNullOrEmpty(PO_Details[3].Trim()) ? "" : PO_Details[3])).Direction = ParameterDirection.Input;
                    updCommand.Parameters["SENT_ON_IN"].SourceVersion = DataRowVersion.Current;
                }
                else
                {
                    updCommand.Parameters.Add("PO_NUMBER_IN", "").Direction = ParameterDirection.Input;
                    updCommand.Parameters["PO_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    updCommand.Parameters.Add("PO_DATE_IN", "").Direction = ParameterDirection.Input;
                    updCommand.Parameters["PO_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updCommand.Parameters.Add("ROUTING_INST_IN", "").Direction = ParameterDirection.Input;
                    updCommand.Parameters["ROUTING_INST_IN"].SourceVersion = DataRowVersion.Current;

                    updCommand.Parameters.Add("SENT_ON_IN", "").Direction = ParameterDirection.Input;
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

                updCommand.Parameters.Add("WIN_AOO_PK_IN", OracleDbType.Int32, 10, "WIN_AOO_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_AOO_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_PICK_ONC_MOVE_FK_IN", OracleDbType.Int32, 10, "WIN_PICK_ONC_MOVE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_PICK_ONC_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIN_CONSOL_REF_NR_IN", OracleDbType.Varchar2, 20, "WIN_CONSOL_REF_NR").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIN_CONSOL_REF_NR_IN"].SourceVersion = DataRowVersion.Current;
                //'End
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with5 = objWK.MyDataAdapter;

                _with5.InsertCommand = insCommand;
                _with5.InsertCommand.Transaction = TRAN;

                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;

                RecAfct = _with5.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    if (isEdting == false)
                    {
                        RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
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

                var _with6 = insContainerDetails;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_CONT_INS";
                var _with7 = _with6.Parameters;

                insContainerDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insContainerDetails.Parameters.Add("PALETTE_SIZE_IN", OracleDbType.Varchar2, 20, "palette_size").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PALETTE_SIZE_IN"].SourceVersion = DataRowVersion.Current;

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

                insContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Varchar2, 100, "commodity_mst_fk").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "load_date").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("PREV_CONT_PK_IN", OracleDbType.Int32, 10, "prev_cont_pk").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["PREV_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_IMP_CONT_PK").Direction = ParameterDirection.Output;
                insContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with8 = updContainerDetails;
                _with8.Connection = objWK.MyConnection;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_CONT_UPD";
                var _with9 = _with8.Parameters;

                updContainerDetails.Parameters.Add("JOB_TRN_AIR_IMP_CONT_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_cont_pk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["JOB_TRN_AIR_IMP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updContainerDetails.Parameters.Add("PALETTE_SIZE_IN", OracleDbType.Varchar2, 20, "palette_size").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["PALETTE_SIZE_IN"].SourceVersion = DataRowVersion.Current;

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

                updContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Varchar2, 100, "commodity_mst_fk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "load_date").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with10 = delContainerDetails;
                _with10.Connection = objWK.MyConnection;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_CONT_DEL";

                delContainerDetails.Parameters.Add("JOB_TRN_AIR_IMP_CONT_PK_IN", OracleDbType.Int32, 10, "job_trn_air_imp_cont_pk").Direction = ParameterDirection.Input;
                delContainerDetails.Parameters["JOB_TRN_AIR_IMP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

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
                    if (isEdting == false)
                    {
                        RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                        //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    }
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

                            if (CommodityGroup == 603)
                            {
                                arrMessage = SaveTransactionHZSpcl(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
                            }
                            else if (CommodityGroup == 601)
                            {
                                arrMessage = SaveTransactionReefer(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                }

                if ((dsTPDetails != null))
                {
                    var _with13 = insTPDetails;
                    _with13.Connection = objWK.MyConnection;
                    _with13.CommandType = CommandType.StoredProcedure;
                    _with13.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_TP_INS";
                    var _with14 = _with13.Parameters;

                    insTPDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                    insTPDetails.Parameters.Add("WIN_TYPE_IN", OracleDbType.Int32, 3, "WIN_TYPE").Direction = ParameterDirection.Input;
                    insTPDetails.Parameters["WIN_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    insTPDetails.Parameters.Add("WIN_MODE_IN", OracleDbType.Int32, 3, "WIN_MODE").Direction = ParameterDirection.Input;
                    insTPDetails.Parameters["WIN_MODE_IN"].SourceVersion = DataRowVersion.Current;

                    insTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_IMP_TP_PK").Direction = ParameterDirection.Output;
                    insTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with15 = updTPDetails;
                    _with15.Connection = objWK.MyConnection;
                    _with15.CommandType = CommandType.StoredProcedure;
                    _with15.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_TP_UPD";
                    var _with16 = _with15.Parameters;

                    updTPDetails.Parameters.Add("JOB_TRN_AIR_IMP_TP_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_tp_pk").Direction = ParameterDirection.Input;
                    updTPDetails.Parameters["JOB_TRN_AIR_IMP_TP_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updTPDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                    updTPDetails.Parameters.Add("WIN_TYPE_IN", OracleDbType.Int32, 3, "WIN_TYPE").Direction = ParameterDirection.Input;
                    updTPDetails.Parameters["WIN_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    updTPDetails.Parameters.Add("WIN_MODE_IN", OracleDbType.Int32, 3, "WIN_MODE").Direction = ParameterDirection.Input;
                    updTPDetails.Parameters["WIN_MODE_IN"].SourceVersion = DataRowVersion.Current;

                    updTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with17 = delTPDetails;
                    _with17.Connection = objWK.MyConnection;
                    _with17.CommandType = CommandType.StoredProcedure;
                    _with17.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_TP_DEL";

                    delTPDetails.Parameters.Add("JOB_TRN_AIR_IMP_TP_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_tp_pk").Direction = ParameterDirection.Input;
                    delTPDetails.Parameters["JOB_TRN_AIR_IMP_TP_PK_IN"].SourceVersion = DataRowVersion.Current;

                    delTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    delTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with18 = objWK.MyDataAdapter;

                    _with18.InsertCommand = insTPDetails;
                    _with18.InsertCommand.Transaction = TRAN;

                    _with18.UpdateCommand = updTPDetails;
                    _with18.UpdateCommand.Transaction = TRAN;

                    _with18.DeleteCommand = delTPDetails;
                    _with18.DeleteCommand.Transaction = TRAN;
                    RecAfct = _with18.Update(dsTPDetails.Tables[0]);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "Saved") > 0)
                        {
                            arrMessage.Clear();
                        }
                    }
                    if (arrMessage.Count > 0)
                    {
                        if (isEdting == false)
                        {
                            RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }

                //Manjunath for Cargo Pick up & Drop Address
                if ((dsPickUpDetails != null))
                {
                    var _with19 = insPickUpDetails;
                    _with19.Connection = objWK.MyConnection;
                    _with19.CommandType = CommandType.StoredProcedure;
                    _with19.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_INS";

                    _with19.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with19.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with19.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with19.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with19.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with19.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with19.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with19.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with19.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with19.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with19.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with19.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with19.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with19.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with19.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with19.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with19.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with19.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with19.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with19.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with19.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Output;
                    _with19.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with20 = updPickUpDetails;
                    _with20.Connection = objWK.MyConnection;
                    _with20.CommandType = CommandType.StoredProcedure;
                    _with20.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_UPD";

                    _with20.Parameters.Add("PICK_DROP_MST_PK_IN", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Input;
                    _with20.Parameters["PICK_DROP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with20.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with20.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with20.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with20.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with20.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with20.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with20.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with20.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with20.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with20.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with20.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with20.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with20.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with20.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with20.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with20.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with20.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with20.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with20.Parameters.Add("MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with20.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with20.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with21 = objWK.MyDataAdapter;

                    _with21.InsertCommand = insPickUpDetails;
                    _with21.InsertCommand.Transaction = TRAN;

                    _with21.UpdateCommand = updPickUpDetails;
                    _with21.UpdateCommand.Transaction = TRAN;

                    RecAfct = _with21.Update(dsPickUpDetails);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }

                if ((dsDropDetails != null))
                {
                    var _with22 = insDropDetails;
                    _with22.Connection = objWK.MyConnection;
                    _with22.CommandType = CommandType.StoredProcedure;
                    _with22.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_INS";

                    _with22.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with22.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with22.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with22.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with22.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with22.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with22.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with22.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with22.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with22.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with22.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with22.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with22.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with22.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with22.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with22.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with22.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with22.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with22.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with22.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with22.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Output;
                    _with22.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with23 = updDropDetails;
                    _with23.Connection = objWK.MyConnection;
                    _with23.CommandType = CommandType.StoredProcedure;
                    _with23.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_UPD";

                    _with23.Parameters.Add("PICK_DROP_MST_PK_IN", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Input;
                    _with23.Parameters["PICK_DROP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with23.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
                    _with23.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
                    _with23.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
                    _with23.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
                    _with23.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
                    _with23.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
                    _with23.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
                    _with23.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
                    _with23.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
                    _with23.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
                    _with23.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
                    _with23.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
                    _with23.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
                    _with23.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
                    _with23.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
                    _with23.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
                    _with23.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
                    _with23.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

                    _with23.Parameters.Add("MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                    _with23.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with23.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with24 = objWK.MyDataAdapter;

                    _with24.InsertCommand = insDropDetails;
                    _with24.InsertCommand.Transaction = TRAN;

                    _with24.UpdateCommand = updDropDetails;
                    _with24.UpdateCommand.Transaction = TRAN;

                    RecAfct = _with24.Update(dsDropDetails);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                //End Manjunath

                if ((dsFreightDetails != null))
                {
                    var _with25 = insFreightDetails;
                    _with25.Connection = objWK.MyConnection;
                    _with25.CommandType = CommandType.StoredProcedure;
                    _with25.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_FD_INS";
                    var _with26 = _with25.Parameters;

                    insFreightDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                    insFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_IMP_FD_PK").Direction = ParameterDirection.Output;
                    insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with27 = updFreightDetails;
                    _with27.Connection = objWK.MyConnection;
                    _with27.CommandType = CommandType.StoredProcedure;
                    _with27.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_FD_UPD";
                    var _with28 = _with27.Parameters;

                    updFreightDetails.Parameters.Add("JOB_TRN_AIR_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_fd_pk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["JOB_TRN_AIR_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                    updFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with29 = delFreightDetails;
                    _with29.Connection = objWK.MyConnection;
                    _with29.CommandType = CommandType.StoredProcedure;
                    _with29.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_FD_DEL";

                    delFreightDetails.Parameters.Add("JOB_TRN_AIR_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_fd_pk").Direction = ParameterDirection.Input;
                    delFreightDetails.Parameters["JOB_TRN_AIR_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

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
                        if (isEdting == false)
                        {
                            RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }

                if ((dsIncomeChargeDetails != null) & (dsExpenseChargeDetails != null))
                {
                    if (!SaveSecondaryServices(objWK, TRAN, (Int32)JobCardPK, dsIncomeChargeDetails, dsExpenseChargeDetails))
                    {
                        arrMessage.Add("Error while saving secondary service details");
                        return arrMessage;
                    }
                }

                if ((dsPurchaseInventory != null))
                {
                    var _with31 = insPurchaseInvDetails;
                    _with31.Connection = objWK.MyConnection;
                    _with31.CommandType = CommandType.StoredProcedure;
                    _with31.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_PIA_INS";
                    var _with32 = _with31.Parameters;

                    insPurchaseInvDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                    insPurchaseInvDetails.Parameters.Add("ATTACHED_FILE_NAME_IN", OracleDbType.Varchar2, 50, "attached_file_name").Direction = ParameterDirection.Input;
                    insPurchaseInvDetails.Parameters["ATTACHED_FILE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                    insPurchaseInvDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "vendor_mst_fk").Direction = ParameterDirection.Input;
                    insPurchaseInvDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insPurchaseInvDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "job_trn_air_IMP_pia_pk").Direction = ParameterDirection.Output;
                    insPurchaseInvDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with33 = updPurchaseInvDetails;
                    _with33.Connection = objWK.MyConnection;
                    _with33.CommandType = CommandType.StoredProcedure;
                    _with33.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_PIA_UPD";
                    var _with34 = _with33.Parameters;

                    updPurchaseInvDetails.Parameters.Add("JOB_TRN_AIR_IMP_PIA_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_pia_pk").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["JOB_TRN_AIR_IMP_PIA_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                    updPurchaseInvDetails.Parameters.Add("ATTACHED_FILE_NAME_IN", OracleDbType.Varchar2, 50, "attached_file_name").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["ATTACHED_FILE_NAME_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "vendor_mst_fk").Direction = ParameterDirection.Input;
                    updPurchaseInvDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updPurchaseInvDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updPurchaseInvDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with35 = delPurchaseInvDetails;
                    _with35.Connection = objWK.MyConnection;
                    _with35.CommandType = CommandType.StoredProcedure;
                    _with35.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_PIA_DEL";

                    delPurchaseInvDetails.Parameters.Add("JOB_TRN_AIR_IMP_PIA_PK_IN", OracleDbType.Int32, 10, "job_trn_air_imp_pia_pk").Direction = ParameterDirection.Input;
                    delPurchaseInvDetails.Parameters["JOB_TRN_AIR_IMP_PIA_PK_IN"].SourceVersion = DataRowVersion.Current;

                    delPurchaseInvDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    delPurchaseInvDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

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
                        if (isEdting == false)
                        {
                            RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                //'Added By Koteshwari on 27/4/2011
                if ((dsCostDetails != null))
                {
                    var _with37 = insCostDetails;
                    _with37.Connection = objWK.MyConnection;
                    _with37.CommandType = CommandType.StoredProcedure;
                    _with37.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_COST_INS";
                    insCostDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                    _with37.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_IMP_COST_PK").Direction = ParameterDirection.Output;
                    _with37.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with38 = updCostDetails;
                    _with38.Connection = objWK.MyConnection;
                    _with38.CommandType = CommandType.StoredProcedure;
                    _with38.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_COST_UPD";

                    _with38.Parameters.Add("JOB_TRN_AIR_IMP_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_AIR_IMP_COST_PK").Direction = ParameterDirection.Input;
                    _with38.Parameters["JOB_TRN_AIR_IMP_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with38.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                    _with38.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with38.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with39 = delCostDetails;
                    _with39.Connection = objWK.MyConnection;
                    _with39.CommandType = CommandType.StoredProcedure;
                    _with39.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_COST_DEL";

                    _with39.Parameters.Add("JOB_TRN_AIR_IMP_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_AIR_IMP_COST_PK").Direction = ParameterDirection.Input;
                    _with39.Parameters["JOB_TRN_AIR_IMP_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

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
                }
                //'End Koteshwari
                if ((dsOtherCharges != null))
                {
                    var _with41 = insOtherChargesDetails;
                    _with41.Connection = objWK.MyConnection;
                    _with41.CommandType = CommandType.StoredProcedure;
                    _with41.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_OTH_CHRG_INS";

                    _with41.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                    _with41.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "job_trn_air_IMP_oth_pk").Direction = ParameterDirection.Output;
                    _with41.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with42 = updOtherChargesDetails;
                    _with42.Connection = objWK.MyConnection;
                    _with42.CommandType = CommandType.StoredProcedure;
                    _with42.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_OTH_CHRG_UPD";

                    _with42.Parameters.Add("JOB_TRN_AIR_IMP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_oth_pk").Direction = ParameterDirection.Input;
                    _with42.Parameters["JOB_TRN_AIR_IMP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with42.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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
                    _with43.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_OTH_CHRG_DEL";

                    _with43.Parameters.Add("JOB_TRN_AIR_IMP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_oth_pk").Direction = ParameterDirection.Input;
                    _with43.Parameters["JOB_TRN_AIR_IMP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

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
                    if (isEdting == false)
                    {
                        RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                        //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    }
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    cls_JobCardView objJCFclLcl = new cls_JobCardView();
                    objJCFclLcl.CREATED_BY = Convert.ToInt64(M_CREATED_BY_FK);
                    objJCFclLcl.LAST_MODIFIED_BY = Convert.ToInt64(M_CREATED_BY_FK);
                    objJCFclLcl.ConfigurationPK = Convert.ToInt64(M_Configuration_PK);
                    //arrMessage = objJCFclLcl.SaveJobCardDoc(JobCardPK, TRAN, dsDoc, 1, 2);
                    //Biztype -2(Air),Process Type -2(Import)
                    if (arrMessage.Count > 0)
                    {
                        if (!(string.Compare(arrMessage[0].ToString(), "Saved") > 0))
                        {
                            TRAN.Rollback();
                            if (isEdting == false)
                            {
                                RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                                //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                            }
                            return arrMessage;
                        }
                    }
                    arrMessage = (ArrayList)SaveTrackAndTrace((Int32)JobCardPK, TRAN, M_DataSet, dsTrackNTrace, nLocationfk, isEdting);
                    if (arrMessage.Count > 0)
                    {
                        TRAN.Commit();
                        //Push to financial system if realtime is selected
                        if (JobCardPK > 0)
                        {
                            Quantum_QFOR.cls_Scheduler objSch = new Quantum_QFOR.cls_Scheduler();
                            ArrayList schDtls = null;
                            bool errGen = false;
                            if (objSch.GetSchedulerPushType() == true)
                            {
                                //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                                //try
                                //{
                                //    schDtls = objSch.FetchSchDtls();
                                //    //'Used to Fetch the Sch Dtls
                                //    objPush.UpdateCostCentre(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, schDtls[5].ToString().ToUpper(), schDtls[0].ToString().ToUpper(), , , JobCardPK);
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                                //    }
                                //}
                                //catch (Exception ex)
                                //{
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
                                //    }
                                //}
                            }
                        }
                        //*****************************************************************
                        return arrMessage;
                    }
                    else
                    {
                        if (isEdting == false)
                        {
                            RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                            //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                        }
                        TRAN.Rollback();
                    }
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                if (isEdting == false)
                {
                    RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
                throw oraexp;
            }
            catch (Exception ex)
            {
                if (isEdting == false)
                {
                    RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
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
                foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                {
                    int Frt_Pk = 0;
                    try
                    {
                        Frt_Pk = Convert.ToInt32(ri["JOB_TRN_AIR_IMP_FD_PK"]);
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
                        _with45.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_FD_UPD";
                        _with45.Parameters.Add("JOB_TRN_AIR_IMP_FD_PK_IN", ri["JOB_TRN_AIR_IMP_FD_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with45.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_FD_INS";
                        _with45.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }
                    _with45.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("BASIS_IN", getDefault(ri["BASIS"], "")).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("BASIS_FK_IN", getDefault(ri["BASIS_PK"], "")).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("EXCHANGE_RATE_IN", getDefault(ri["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("RATE_PERBASIS_IN", getDefault(ri["RATEPERBASIS"], "")).Direction = ParameterDirection.Input;
                    _with45.Parameters.Add("QUANTITY_IN", getDefault(ri["VOLUME"], "")).Direction = ParameterDirection.Input;
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
                            _with45.Parameters.Add("BIZ_IN", 1).Direction = ParameterDirection.Input;
                            _with45.Parameters.Add("PROCESS_IN", 2).Direction = ParameterDirection.Input;
                            _with45.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with45.ExecuteNonQuery();
                            Frt_Pk = Convert.ToInt32(_with45.Parameters["RETURN_VALUE"].Value);
                            ri["JOB_TRN_AIR_IMP_FD_PK"] = Frt_Pk;
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
                        Cost_Pk = Convert.ToInt32(re["JOB_TRN_AIR_IMP_COST_PK"]);
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
                        _with46.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_COST_UPD";
                        _with46.Parameters.Add("JOB_TRN_AIR_IMP_EST_PK_IN", re["JOB_TRN_AIR_IMP_COST_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with46.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_COST_INS";
                        _with46.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }
                    _with46.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
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
                    _with46.Parameters.Add("QUANTITY_IN", getDefault(re["VOLUME"], "")).Direction = ParameterDirection.Input;
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
                            _with46.Parameters.Add("BIZ_IN", 1).Direction = ParameterDirection.Input;
                            _with46.Parameters.Add("PROCESS_IN", 2).Direction = ParameterDirection.Input;
                            _with46.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with46.ExecuteNonQuery();
                            Cost_Pk = Convert.ToInt32(_with46.Parameters["RETURN_VALUE"].Value);
                            re["JOB_TRN_AIR_IMP_COST_PK"] = Cost_Pk;
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
            //Dim objwk As New Business.WorkFlow
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
                            SelectedFrtPks = ri["JOB_TRN_AIR_IMP_FD_PK"].ToString();
                        }
                        else
                        {
                            SelectedFrtPks += "," + ri["JOB_TRN_AIR_IMP_FD_PK"];
                        }
                    }
                    foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedCostPks))
                        {
                            SelectedCostPks = re["JOB_TRN_AIR_IMP_COST_PK"].ToString();
                        }
                        else
                        {
                            SelectedCostPks += "," + re["JOB_TRN_AIR_IMP_COST_PK"];
                        }
                    }

                    var _with47 = objWK.MyCommand;
                    _with47.Transaction = TRAN;
                    _with47.CommandType = CommandType.StoredProcedure;
                    _with47.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.DELETE_AIR_IMP_SEC_CHG_EXCEPT";
                    _with47.Parameters.Clear();
                    _with47.Parameters.Add("JOB_CARD_AIR_IMP_PK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with47.Parameters.Add("JOB_TRN_AIR_IMP_FD_PKS", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
                    _with47.Parameters.Add("JOB_TRN_AIR_IMP_COST_PKS", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
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
                        _with49.Add("PROCESS_TYPE_IN", 2).Direction = ParameterDirection.Input;
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
                    _with51.Add("PROCESS_TYPE_IN", 2).Direction = ParameterDirection.Input;
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

        #region "Track N Trace Save Functionality"

        /// <summary>
        /// Saves the track and trace.
        /// </summary>
        /// <param name="jobPk">The job pk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="dsContainerData">The ds container data.</param>
        /// <param name="nlocationfk">The nlocationfk.</param>
        /// <param name="IsEditing">if set to <c>true</c> [is editing].</param>
        /// <returns></returns>
        public object SaveTrackAndTrace(int jobPk, OracleTransaction TRAN, DataSet dsMain, DataSet dsContainerData, int nlocationfk, bool IsEditing)
        {
            try
            {
                int Cnt = 0;
                int POD = 0;
                var UpdCntrOnQuay = false;
                string strContData = null;
                System.DateTime ArrDate = default(System.DateTime);
                string Status = null;
                string PortDis = null;

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
                Status = "Flight Arrived At '" + PortDis + "' at '" + ArrDate + "' ";

                objTrackNTrace.DeleteOnSaveTraceExportOnATDLDUpd(jobPk, 1, 2, "Vessel Voyage", "CNT-DT-DATA-DEL-AIR-IMP", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                "Null");
                for (Cnt = 0; Cnt <= dsContainerData.Tables[0].Rows.Count - 1; Cnt++)
                {
                    if (!string.IsNullOrEmpty((dsContainerData.Tables[0].Rows[Cnt]["load_date"].ToString())))
                    {
                        UpdCntrOnQuay = true;
                        var _with52 = dsContainerData.Tables[0].Rows[Cnt];
                        // By Amit to display message as "Cargo Discharged at XXXX POD"
                        strContData = "Cargo Discharged at " + dsMain.Tables[0].Rows[0]["POD"] + "~" + _with52["load_date"];
                        //strContData = "Discharge Pallete " & .Item("palette_size") & " On  " & .Item("load_date")
                        arrMessage = objTrackNTrace.SaveTrackAndTraceImportJob(jobPk, 1, 2, strContData, "CNT-ON-QUAY-DT-UPD-AIR-IMP", nlocationfk, TRAN, "INS", CREATED_BY, "O");
                    }
                }
                //modified by thiyagarajan on 18/12/08 for import track n trace
                if (IsEditing == false)
                {
                    arrMessage = objTrackNTrace.SaveTrackAndTraceImportJob(jobPk, 1, 2, "Job Card", "JOB-INS-AIR-IMP", nlocationfk, TRAN, "INS", CREATED_BY, "O");
                    arrMessage = objTrackNTrace.SaveTrackAndTraceImportJobATA(jobPk, 1, 2, Status, "JOB-INS-AIR-IMP-ATA", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                    ArrDate);
                }
                else if (Convert.ToInt32(CheckATA(jobPk)) == 0)
                {
                    arrMessage = objTrackNTrace.SaveTrackAndTraceImportJobATA(jobPk, 1, 2, Status, "JOB-INS-AIR-IMP-ATA", nlocationfk, TRAN, "INS", CREATED_BY, "O",
                    ArrDate);
                }
                arrMessage.Clear();
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

        //The Below Lines are modified By Anand for Capturing Arrived At POD :12/11/08
        /// <summary>
        /// Checks the pod.
        /// </summary>
        /// <param name="POD">The pod.</param>
        /// <returns></returns>
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
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //created by thiyagarajan on 18/12/08 for import track n trace
        /// <summary>
        /// Checks the ata.
        /// </summary>
        /// <param name="jobpk">The jobpk.</param>
        /// <returns></returns>
        public string CheckATA(Int32 jobpk)
        {
            WorkFlow objWF = new WorkFlow();
            string sql = null;
            try
            {
                sql = "select count(*) from TRACK_N_TRACE_TBL TR WHERE TR.STATUS LIKE '%Flight Arrived At%' and TR.BIZ_TYPE=1 AND ";
                sql += " TR.PROCESS = 2 And TR.JOB_CARD_FK = " + jobpk;
                return objWF.ExecuteScaler(sql);
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

        #endregion "Track N Trace Save Functionality"

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
            strSQL.Append(" group by cargo_move_pk,cargo_move_code ");

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

            strSQL.Append(" SELECT 0 shipping_terms_mst_pk, 'Select' inco_code ");
            strSQL.Append(" FROM DUAL ");
            strSQL.Append(" UNION ");

            strSQL.Append(" SELECT ");
            strSQL.Append("      s.shipping_terms_mst_pk,");
            strSQL.Append("      s.inco_code ");
            strSQL.Append(" FROM");
            strSQL.Append("      shipping_terms_mst_tbl s ");
            strSQL.Append(" WHERE");
            strSQL.Append("      s.active_flag = 1 ");
            strSQL.Append(" group by shipping_terms_mst_pk,inco_code");

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

        #region " Fetch Revenue data export"

        /// <summary>
        /// Fetches the revenue data.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet FetchRevenueData(string jobCardPK = "0")
        {
            //Dim strSQL As StringBuilder = New StringBuilder
            //Dim objWF As New Business.WorkFlow
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
                var _with53 = objWF.MyCommand.Parameters;
                _with53.Add("JOBCARD_PK", jobCardPK).Direction = ParameterDirection.Input;
                _with53.Add("JOB_AIR_IMP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataSet("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_AIR_IMP");
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
            //Snigdharani - 10/11/2008 - making the values same as consolidation screen.
            try
            {
                DataSet DS = new DataSet();
                var _with54 = objWF.MyCommand.Parameters;
                _with54.Add("JCPK", jobCardPK).Direction = ParameterDirection.Input;
                _with54.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with54.Add("JOB_IMP_AIR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_COST_REVENUE_PROFIT", "FETCH_JOBCARD_IMP_AIR");
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
            //SQL.Append(vbCrLf & "           job_imp.jobcard_date,")
            //SQL.Append(vbCrLf & "           curr.currency_mst_pk,")
            //SQL.Append(vbCrLf & "           SUM(job_trn_pia.Estimated_Amt) EstimatedCost,")
            //SQL.Append(vbCrLf & "           SUM(job_trn_pia.Invoice_Amt) ActualCost")
            //SQL.Append(vbCrLf & "     FROM")
            //SQL.Append(vbCrLf & "           job_trn_air_imp_pia  job_trn_pia,")
            //SQL.Append(vbCrLf & "           currency_type_mst_tbl curr,")
            //SQL.Append(vbCrLf & "           cost_element_mst_tbl cost_ele,")
            //SQL.Append(vbCrLf & "           job_card_air_imp_tbl job_imp")
            //SQL.Append(vbCrLf & "     WHERE")
            //SQL.Append(vbCrLf & "           job_trn_pia.job_card_air_imp_fk = job_imp.job_card_air_imp_pk")
            //SQL.Append(vbCrLf & "           AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk")
            //SQL.Append(vbCrLf & "           AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk")
            //SQL.Append(vbCrLf & "           AND job_imp.job_card_air_imp_pk =" + jobCardPK)

            //'by Thiyagarajan on 28/3/08 for location based currency : PTS TASK GEN-FEB-003
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
            //SQL.Append(vbCrLf & "               exchange_rate_trn exch")
            //SQL.Append(vbCrLf & "            where")
            //SQL.Append(vbCrLf & "               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))")
            //SQL.Append(vbCrLf & "               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ")
            //SQL.Append(vbCrLf & "           )end ")
            //SQL.Append(vbCrLf & "   ),4)) ""Estimated Revenue""")
            //SQL.Append(vbCrLf & "FROM")
            //SQL.Append(vbCrLf & "   (SELECT")
            //SQL.Append(vbCrLf & "       job_imp.jobcard_date,")
            //SQL.Append(vbCrLf & "       curr.currency_mst_pk,")
            //SQL.Append(vbCrLf & "       sum(job_trn_fd.freight_amt) freight_amt")
            //SQL.Append(vbCrLf & "    FROM")
            //SQL.Append(vbCrLf & "       job_trn_air_imp_fd  job_trn_fd,")
            //SQL.Append(vbCrLf & "       currency_type_mst_tbl curr,")
            //SQL.Append(vbCrLf & "       job_card_air_imp_tbl job_imp")
            //SQL.Append(vbCrLf & "    WHERE")
            //SQL.Append(vbCrLf & "       job_trn_fd.job_card_air_imp_fk = job_imp.job_card_air_imp_pk")
            //SQL.Append(vbCrLf & "       AND job_trn_fd.currency_mst_fk =curr.currency_mst_pk")
            //SQL.Append(vbCrLf & "       AND job_imp.job_card_air_imp_pk =" + jobCardPK)

            //'by Thiyagarajan on 28/3/08 for location based currency : PTS TASK GEN-FEB-003
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
            //SQL.Append(vbCrLf & "       job_imp.jobcard_date,")
            //SQL.Append(vbCrLf & "       curr.currency_mst_pk,")
            //SQL.Append(vbCrLf & "       sum(job_trn_othr.amount) freight_amt")
            //SQL.Append(vbCrLf & "    FROM")
            //SQL.Append(vbCrLf & "       job_trn_air_imp_oth_chrg  job_trn_othr,")
            //SQL.Append(vbCrLf & "       currency_type_mst_tbl curr,")
            //SQL.Append(vbCrLf & "       job_card_air_imp_tbl job_imp")
            //SQL.Append(vbCrLf & "    WHERE")
            //SQL.Append(vbCrLf & "       job_trn_othr.job_card_air_imp_fk = job_imp.job_card_air_imp_pk")
            //SQL.Append(vbCrLf & "       AND job_trn_othr.currency_mst_fk =curr.currency_mst_pk")
            //SQL.Append(vbCrLf & "       AND job_imp.job_card_air_imp_pk =" + jobCardPK)

            //'by Thiyagarajan on 28/3/08 for location based currency : PTS TASK GEN-FEB-003
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
            //Try
            //    objWF.MyCommand.Parameters.Clear()
            //    With objWF.MyCommand.Parameters
            //        .Add("JOBCARD_PK", jobCardPK).Direction = ParameterDirection.Input
            //        '.Add("LocationsPk", LocationPk).Direction = ParameterDirection.Input 'add by Thiyagarajan for location based curr.
            //        'Commented By: Anand Reason: Location Based Currency Is not moved to eqa
            //        .Add("JOB_AIR_IMP_CUR", OracleClient.OracleDbType.RefCursor).Direction = ParameterDirection.Output
            //    End With
            //    'Return objWF.GetDataSet("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_SEA_EXP")
            //Catch sqlExp As Exception
            //    Throw sqlExp
            //End Try
            //oraReader = objWF.GetDataReader("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_AIR_IMP_ACTREV")
            //While oraReader.Read
            //    If Not (oraReader(0) Is "") Then
            //        actualRevenue = oraReader(0)
            //    End If
            //End While
            //oraReader.Close()
            //Try
            //    Return True
            //Catch sqlExp As OracleException
            //    ErrorMessage = sqlExp.Message
            //    Throw sqlExp
            //Catch exp As Exception
            //    ErrorMessage = exp.Message
            //    Throw exp
            //End Try
        }

        #endregion "GetRevenueDetails"

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
            sb.Append("  FROM JOB_CARD_AIR_IMP_TBL J");
            sb.Append(" WHERE J.JOB_CARD_AIR_IMP_PK = " + JobCardPk);
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

        #region " Fetch Airline data export"

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

        #endregion " Fetch Airline data export"

        #region " Fetch ROE data export"

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
                //modifying by thiyagarajan on 24/11/08 for location based currency task
                strSQL.Append("SELECT");
                strSQL.Append("    CURR.CURRENCY_MST_PK,");
                strSQL.Append("    CURR.CURRENCY_ID,");
                strSQL.Append("    ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",round(sysdate - .5)),4) AS ROE");
                strSQL.Append("FROM");
                strSQL.Append("    CURRENCY_TYPE_MST_TBL CURR");
                strSQL.Append("WHERE");
                strSQL.Append("    CURR.ACTIVE_FLAG = 1");
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

        #region " Fetch Weight and Volume"

        /// <summary>
        /// Fetches the weight and volume.
        /// </summary>
        /// <param name="jobcardNumber">The jobcard number.</param>
        /// <returns></returns>
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
                strSQL.Append("select sum(nvl(cont.gross_weight,0)),");
                strSQL.Append("           sum(nvl(cont.volume_in_cbm,0))");
                strSQL.Append("      from job_card_air_imp_tbl job, job_trn_air_imp_cont cont");
                strSQL.Append("     where job.job_card_air_imp_pk = " + jobcardNumber);
                strSQL.Append("   and job.job_card_air_imp_pk = cont.job_card_air_imp_fk");

                oraReader = objWF.GetDataReader(strSQL.ToString());

                while (oraReader.Read())
                {
                    if ((!object.ReferenceEquals(oraReader[0], "")))
                    {
                        strWeight = oraReader[0].ToString();
                    }

                    if ((!object.ReferenceEquals(oraReader[1], "")))
                    {
                        strVolume = oraReader[1].ToString();
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

        #endregion " Fetch Weight and Volume"

        #region "FillJobCardOtherChargesDataSet"

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
            strSQL.Append("         oth_chrg.inv_cust_trn_air_imp_fk,");
            strSQL.Append("         oth_chrg.inv_agent_trn_air_imp_fk,");
            strSQL.Append("         oth_chrg.consol_invoice_trn_fk");
            strSQL.Append("FROM");
            strSQL.Append("         job_trn_air_imp_oth_chrg oth_chrg,");
            strSQL.Append("         job_card_air_imp_tbl jobcard_mst,");
            strSQL.Append("         freight_element_mst_tbl frt,");
            strSQL.Append("         currency_type_mst_tbl curr,");
            //latha
            strSQL.Append("   location_mst_tbl loc,");
            strSQL.Append("   customer_mst_tbl cus");

            strSQL.Append("WHERE");
            strSQL.Append("         oth_chrg.job_card_air_imp_fk = jobcard_mst.job_card_air_imp_pk");
            strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.job_card_air_imp_fk    = " + pk);
            //latha
            strSQL.Append("   AND oth_chrg.frtpayer_cust_mst_fk = cus.customer_mst_pk(+) ");
            strSQL.Append("   AND oth_chrg.location_mst_fk = loc.location_mst_pk(+) ");

            //strSQL.Append(vbCrLf & "   AND jobcard_mst.cust_customer_mst_fk = cus.customer_mst_pk(+)")
            //strSQL.Append(vbCrLf & "   AND loc.location_mst_pk = " & LoggedIn_Loc_FK)
            strSQL.Append(" ORDER BY freight_element_id ");

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
            strSQL.Append("         oth_chrg.job_trn_air_imp_oth_pk,");
            strSQL.Append("         frt.freight_element_mst_pk,");
            strSQL.Append("         frt.freight_element_id,");
            strSQL.Append("         frt.freight_element_name,");

            //latha
            strSQL.Append("         curr.currency_mst_pk,");
            strSQL.Append("         DECODE(oth_chrg.freight_type,1,'Prepaid',2,'Collect') PaymentType, ");
            strSQL.Append("   oth_chrg.location_mst_fk  \"location_fk\" ,");
            strSQL.Append("   loc.location_id \"location_id\" ,");
            strSQL.Append("   oth_chrg.frtpayer_cust_mst_fk \"frtpayer_mst_fk\" ,");
            strSQL.Append("   cus.customer_id \"frtpayer\",");
            if (basecurrency != 0)
            {
                strSQL.Append("    ROUND(GET_EX_RATE(oth_chrg.currency_mst_fk," + basecurrency + ",round(sysdate - .5)),4) AS ROE ,");
            }
            else
            {
                strSQL.Append("         oth_chrg.EXCHANGE_RATE \"ROE\",");
            }
            strSQL.Append("         oth_chrg.amount amount,");
            strSQL.Append("         'false' \"Delete\"");
            strSQL.Append("FROM");
            strSQL.Append("         job_trn_air_imp_oth_chrg oth_chrg,");
            strSQL.Append("         job_card_air_imp_tbl jobcard_mst,");
            strSQL.Append("         freight_element_mst_tbl frt,");
            strSQL.Append("         currency_type_mst_tbl curr,");
            //latha
            strSQL.Append("   location_mst_tbl loc,");
            strSQL.Append("   customer_mst_tbl cus");
            strSQL.Append("WHERE");
            strSQL.Append("         oth_chrg.job_card_air_imp_fk = jobcard_mst.job_card_air_imp_pk");
            strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
            strSQL.Append("         AND oth_chrg.job_card_air_imp_fk    = " + pk);
            //latha
            strSQL.Append("   AND oth_chrg.frtpayer_cust_mst_fk = cus.customer_mst_pk(+) ");
            strSQL.Append("   AND oth_chrg.location_mst_fk = loc.location_mst_pk(+) ");

            //strSQL.Append(vbCrLf & "   AND jobcard_mst.cust_customer_mst_fk = cus.customer_mst_pk")
            //strSQL.Append(vbCrLf & "   AND loc.location_mst_pk = " & LoggedIn_Loc_FK)
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

        #endregion "FillJobCardOtherChargesDataSet"

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
                //SQL.Append(vbCrLf & "select i.inv_cust_air_imp_pk from inv_cust_air_imp_tbl i where i.job_card_air_imp_fk = " & jobCardPK)
                SQL.Append("SELECT CON.CONSOL_INVOICE_PK");
                SQL.Append("  FROM CONSOL_INVOICE_TBL CON, CONSOL_INVOICE_TRN_TBL CONTRN");
                SQL.Append(" WHERE CON.CONSOL_INVOICE_PK = CONTRN.CONSOL_INVOICE_FK");
                SQL.Append("  AND CON.BUSINESS_TYPE = 1");
                SQL.Append("  AND CON.PROCESS_TYPE = 2");
                SQL.Append("   AND CONTRN.JOB_CARD_FK = " + jobCardPK);
            }
            else if (invoiceType == 2)
            {
                SQL.Append("select i.inv_agent_pk from inv_agent_tbl i where i.CB_DP_LOAD_AGENT=1 AND  i.job_card_air_imp_fk = " + jobCardPK);
            }
            else if (invoiceType == 3)
            {
                SQL.Append("select i.inv_agent_pk from inv_agent_tbl i where  i.CB_DP_LOAD_AGENT=2 AND i.job_card_air_imp_fk = " + jobCardPK);
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

        #region "To Fetch Agent Detail"

        /// <summary>
        /// Get_s the agent_ detail.
        /// </summary>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet Get_Agent_Detail(int JobCardPK)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append(" SELECT AMST.AGENT_MST_PK,");
                strQuery.Append(" AMST.AGENT_ID,");
                strQuery.Append(" AMST.AGENT_NAME");
                strQuery.Append(" FROM AGENT_MST_TBL AMST,");
                strQuery.Append(" JOB_CARD_AIR_EXP_TBL EJOB,");
                strQuery.Append(" JOB_CARD_AIR_IMP_TBL IJOB,");
                strQuery.Append(" USER_MST_TBL UMST");
                strQuery.Append(" WHERE EJOB.JOBCARD_REF_NO = IJOB.JOBCARD_REF_NO");
                strQuery.Append(" AND AMST.LOCATION_MST_FK= UMST.DEFAULT_LOCATION_FK");
                strQuery.Append("  AND EJOB.CREATED_BY_FK = UMST.USER_MST_PK");
                strQuery.Append(" AND AMST.LOCATION_AGENT = 1");
                strQuery.Append(" AND IJOB.JOB_CARD_AIR_IMP_PK = '" + JobCardPK + "'");
                return objWF.GetDataSet(strQuery.ToString());
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
        /// Get_s the type of the j c_.
        /// </summary>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet Get_JC_Type(int JobCardPK)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append(" SELECT JC.JC_AUTO_MANUAL ");
                strQuery.Append(" FROM JOB_CARD_AIR_IMP_TBL JC");
                strQuery.Append(" WHERE JC.JOB_CARD_AIR_IMP_PK = '" + JobCardPK + "'");
                return objWF.GetDataSet(strQuery.ToString());
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

        #endregion "To Fetch Agent Detail"

        #region " Update Job Card data to Export Side"

        /// <summary>
        /// Saves the data to export.
        /// </summary>
        /// <param name="JobCardRefNo">The job card reference no.</param>
        /// <param name="ETADate">The eta date.</param>
        /// <param name="ArrivalDate">The arrival date.</param>
        /// <param name="ConsigneePK">The consignee pk.</param>
        /// <returns></returns>
        public object SaveDataToExport(string JobCardRefNo, string ETADate, string ArrivalDate, string ConsigneePK)
        {
            //* If user is updating Consignee,ETA & Arrival Date at import side *
            //* then it will update in Export Side also *
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL = " update job_card_air_exp_tbl job_exp ";
                strSQL = strSQL + " set job_exp.consignee_cust_mst_fk = " + ConsigneePK;
                //strSQL = strSQL & vbCrLf & ", job_exp.eta_date =  to_date('" & ETADate & "', 'dd/MM/yyyy HH24:Mi:ss')"
                //strSQL = strSQL & vbCrLf & ", job_exp.arrival_date = to_date('" & ArrivalDate & "', 'dd/MM/yyyy HH24:Mi:ss')"
                strSQL = strSQL + ", job_exp.eta_date =  to_date('" + ETADate + "', datetimeformat24)";
                strSQL = strSQL + ", job_exp.arrival_date = to_date('" + ArrivalDate + "', datetimeformat24)";
                strSQL = strSQL + " where job_exp.jobcard_ref_no in ( ";
                strSQL = strSQL + "  select j.jobcard_ref_no from job_card_air_imp_tbl jj,job_card_air_exp_tbl j ";
                strSQL = strSQL + " where jj.jobcard_ref_no = j.jobcard_ref_no  ";
                strSQL = strSQL + " and jj.jobcard_ref_no = '" + JobCardRefNo + "' )";
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

        #endregion " Update Job Card data to Export Side"

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
                sb.Append("   FROM JOB_CARD_AIR_IMP_TBL   JC,");
                sb.Append("        JOB_TRN_AIR_IMP_PIA    JP");
                sb.Append("  WHERE ");
                sb.Append("     JC.JOB_CARD_AIR_IMP_PK = " + jobCardID + "");
                sb.Append("    AND JC.JOB_CARD_AIR_IMP_PK = JP.JOB_CARD_AIR_IMP_FK(+)");

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
                sb.Append("   FROM JOB_CARD_AIR_IMP_TBL   JC,");
                sb.Append("        CONSOL_INVOICE_TRN_TBL CI,");
                sb.Append("        CONSOL_INVOICE_TBL     CON");
                sb.Append("  WHERE JC.JOB_CARD_AIR_IMP_PK = CI.JOB_CARD_FK(+)");
                sb.Append("    AND CON.CONSOL_INVOICE_PK = CI.CONSOL_INVOICE_FK");
                sb.Append("    AND JC.JOB_CARD_AIR_IMP_PK = " + jobCardID + "");
                sb.Append("    AND CON.BUSINESS_TYPE = 1");
                sb.Append("    AND CON.PROCESS_TYPE = 2");
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
                var _with55 = objWF.MyCommand.Parameters;
                _with55.Clear();
                _with55.Add("JOB_CARD_AIR_IMP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with55.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with55.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTotalAmt = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "INCOME_MAIN_AIR_IMP");
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }

            //Child Details
            try
            {
                var _with56 = objWF.MyCommand.Parameters;
                _with56.Clear();
                _with56.Add("JOB_CARD_AIR_IMP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with56.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with56.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtChargeDet = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "INCOME_CHILD_AIR_IMP");
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
                var _with57 = objWF.MyCommand.Parameters;
                _with57.Add("JOB_CARD_AIR_IMP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with57.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with57.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtTotalAmt = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "EXPENSE_MAIN_AIR_IMP");
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
                var _with58 = objWF.MyCommand.Parameters;
                _with58.Clear();
                _with58.Add("JOB_CARD_AIR_IMP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
                _with58.Add("BASE_CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with58.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtChargeDet = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "EXPENSE_CHILD_AIR_IMP");
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

        #region "Fetch Special req PK"

        /// <summary>
        /// Gets the spec pk.
        /// </summary>
        /// <param name="Contpk">The contpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public object GetSpecPK(string Contpk, int BizType, int ProcessType)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                sb.Append("SELECT COUNT(*)");
                sb.Append(" FROM JOB_TRN_SEA_EXP_SPL_REQ J");
                sb.Append(" WHERE J.JOB_TRN_SEA_EXP_CONT_FK = " + Contpk);
                sb.Append(" AND J.BIZ_TYPE = " + BizType);
                sb.Append(" AND J.PROCESS_TYPE = " + ProcessType);

                return (objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "Fetch Special req PK"

        #region "Temp Customer Save"

        /// <summary>
        /// Saves the win customer temporary.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="DSCust">The ds customer.</param>
        /// <param name="Custpk">The custpk.</param>
        /// <param name="CustCat">The customer cat.</param>
        /// <returns></returns>
        public bool SaveWINCustomerTemp(WorkFlow objWK, OracleTransaction TRAN, DataSet DSCust, int Custpk, int CustCat)
        {
            int Biztype = 0;
            int Transaction_type = 0;
            int IsReconciled = 0;
            string Category = null;
            Biztype = 3;
            Transaction_type = 0;
            IsReconciled = 0;
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

            var _with59 = objWK.MyCommand;
            _with59.Parameters.Clear();
            _with59.Transaction = TRAN;
            _with59.CommandType = CommandType.StoredProcedure;
            _with59.CommandText = objWK.MyUserName + ".TEMP_CUSTOMER_TBL_PKG.TEMP_CUSTOMER_TBL_INS";

            //.Parameters.Add("CUSTOMER_ID_IN", DSCust.Tables(0).Rows(0).Item("" & Category & "_ReferenceNumber")).Direction = ParameterDirection.Input
            _with59.Parameters.Add("CUSTOMER_ID_IN", "").Direction = ParameterDirection.Input;
            _with59.Parameters.Add("CUSTOMER_NAME_IN", DSCust.Tables[0].Rows[0]["" + Category + "_Name1"]).Direction = ParameterDirection.Input;
            _with59.Parameters.Add("ACTIVE_FLAG_IN", 1).Direction = ParameterDirection.Input;
            _with59.Parameters.Add("CREDIT_LIMIT_IN", "").Direction = ParameterDirection.Input;
            _with59.Parameters.Add("CREDIT_DAYS_IN", "").Direction = ParameterDirection.Input;
            _with59.Parameters.Add("SECURITY_CHK_REQD_IN", "").Direction = ParameterDirection.Input;
            _with59.Parameters.Add("ACCOUNT_NO_IN", "").Direction = ParameterDirection.Input;
            _with59.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
            _with59.Parameters.Add("BUSINESS_TYPE_IN", Biztype).Direction = ParameterDirection.Input;
            _with59.Parameters.Add("CUSTOMER_TYPE_FK_IN", CustCat).Direction = ParameterDirection.Input;
            //'Newd to save Shipper,Customer
            _with59.Parameters.Add("VAT_NO_IN", "").Direction = ParameterDirection.Input;
            _with59.Parameters.Add("TRANSACTION_TYPE_IN", Transaction_type).Direction = ParameterDirection.Input;
            _with59.Parameters.Add("RECONCILE_STATUS_IN", IsReconciled).Direction = ParameterDirection.Input;
            _with59.Parameters.Add("RECONCILED_BY_IN", "").Direction = ParameterDirection.Input;
            _with59.Parameters.Add("PERMANENT_CUST_MST_FK_IN", "").Direction = ParameterDirection.Input;
            //'Contact Details
            _with59.Parameters.Add("ADM_ADDRESS_1_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine1"]).Direction = ParameterDirection.Input;
            if (DSCust.Tables[0].Columns.Contains("" + Category + "_AddressLine2"))
            {
                _with59.Parameters.Add("ADM_ADDRESS_2_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine2"]).Direction = ParameterDirection.Input;
            }
            else
            {
                _with59.Parameters.Add("ADM_ADDRESS_2_IN", "").Direction = ParameterDirection.Input;
            }
            if (DSCust.Tables[0].Columns.Contains("" + Category + "_AddressLine3"))
            {
                _with59.Parameters.Add("ADM_ADDRESS_3_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine3"]).Direction = ParameterDirection.Input;
            }
            else
            {
                _with59.Parameters.Add("ADM_ADDRESS_3_IN", "").Direction = ParameterDirection.Input;
            }
            if (DSCust.Tables[0].Columns.Contains("" + Category + "_PostalCode"))
            {
                _with59.Parameters.Add("ADM_ZIP_CODE_IN", DSCust.Tables[0].Rows[0]["" + Category + "_PostalCode"]).Direction = ParameterDirection.Input;
            }
            else
            {
                _with59.Parameters.Add("ADM_ZIP_CODE_IN", "").Direction = ParameterDirection.Input;
            }

            _with59.Parameters.Add("ADM_CITY_IN", DSCust.Tables[0].Rows[0]["" + Category + "_City"]).Direction = ParameterDirection.Input;

            if (DSCust.Tables[0].Columns.Contains("" + Category + "_ContactPerson"))
            {
                _with59.Parameters.Add("ADM_CONTACT_PERSON_IN", DSCust.Tables[0].Rows[0]["" + Category + "_ContactPerson"]).Direction = ParameterDirection.Input;
            }
            else
            {
                _with59.Parameters.Add("ADM_CONTACT_PERSON_IN", "").Direction = ParameterDirection.Input;
            }

            if (DSCust.Tables[0].Columns.Contains("" + Category + "_PhoneNumber"))
            {
                _with59.Parameters.Add("ADM_PHONE_NO_1_IN", DSCust.Tables[0].Rows[0]["" + Category + "_PhoneNumber"]).Direction = ParameterDirection.Input;
            }
            else
            {
                _with59.Parameters.Add("ADM_PHONE_NO_1_IN", "").Direction = ParameterDirection.Input;
            }
            if (DSCust.Tables[0].Columns.Contains("" + Category + "_FaxNumber"))
            {
                _with59.Parameters.Add("ADM_FAX_NO_IN", DSCust.Tables[0].Rows[0]["" + Category + "_FaxNumber"]).Direction = ParameterDirection.Input;
            }
            else
            {
                _with59.Parameters.Add("ADM_FAX_NO_IN", "").Direction = ParameterDirection.Input;
            }
            if (DSCust.Tables[0].Columns.Contains("" + Category + "_CellNumber"))
            {
                _with59.Parameters.Add("ADM_PHONE_NO_2_IN", DSCust.Tables[0].Rows[0]["" + Category + "_CellNumber"]).Direction = ParameterDirection.Input;
            }
            else
            {
                _with59.Parameters.Add("ADM_PHONE_NO_2_IN", "").Direction = ParameterDirection.Input;
            }
            if (DSCust.Tables[0].Columns.Contains("" + Category + "_Email"))
            {
                _with59.Parameters.Add("ADM_EMAIL_ID_IN", DSCust.Tables[0].Rows[0]["" + Category + "_Email"]).Direction = ParameterDirection.Input;
            }
            else
            {
                _with59.Parameters.Add("ADM_EMAIL_ID_IN", "").Direction = ParameterDirection.Input;
            }

            //'End
            _with59.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
            _with59.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
            try
            {
                _with59.ExecuteNonQuery();
                Custpk = Convert.ToInt32(_with59.Parameters["RETURN_VALUE"].Value);
            }
            catch (Exception ex)
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Saves the win customer.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="DSCust">The ds customer.</param>
        /// <param name="Custpk">The custpk.</param>
        /// <param name="CustCat">The customer cat.</param>
        /// <returns></returns>
        public bool SaveWINCustomer(WorkFlow objWK, OracleTransaction TRAN, DataSet DSCust, int Custpk, int CustCat)
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

            var _with60 = objWK.MyCommand;
            _with60.Parameters.Clear();
            _with60.Transaction = TRAN;
            _with60.CommandType = CommandType.StoredProcedure;
            _with60.CommandText = objWK.MyUserName + ".TEMP_CUSTOMER_TBL_PKG.TEMP_CUSTOMER_TBL_IMP_INS";

            _with60.Parameters.Add("CUSTOMER_ID_IN", DSCust.Tables[0].Rows[0]["" + Category + "_ReferenceNumber"]).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("CUSTOMER_NAME_IN", DSCust.Tables[0].Rows[0]["" + Category + "_Name1"]).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("BUSINESS_TYPE_IN", Biztype).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("TEMP_PATRY_IN", Temp_patry).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("ACCOUNT_NO_IN", "").Direction = ParameterDirection.Input;
            _with60.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("CUSTOMER_TYPE_FK_IN", CustCat).Direction = ParameterDirection.Input;
            //'Newd to save Shipper,Customer
            //'Contact Details
            _with60.Parameters.Add("ADM_ADDRESS_1_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine1"]).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("ADM_ADDRESS_2_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine2"]).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("ADM_ADDRESS_3_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine3"]).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("ADM_ZIP_CODE_IN", DSCust.Tables[0].Rows[0]["" + Category + "_PostalCode"]).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("ADM_CITY_IN", DSCust.Tables[0].Rows[0]["" + Category + "_City"]).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("ADM_CONTACT_PERSON_IN", DSCust.Tables[0].Rows[0]["" + Category + "_ContactPerson"]).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("ADM_PHONE_NO_1_IN", DSCust.Tables[0].Rows[0]["" + Category + "_PhoneNumber"]).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("ADM_FAX_NO_IN", DSCust.Tables[0].Rows[0]["" + Category + "_FaxNumber"]).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("ADM_PHONE_NO_2_IN", DSCust.Tables[0].Rows[0]["" + Category + "_CellNumber"]).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("ADM_EMAIL_ID_IN", DSCust.Tables[0].Rows[0]["" + Category + "_Email"]).Direction = ParameterDirection.Input;
            //'End
            _with60.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
            _with60.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
            try
            {
                _with60.ExecuteNonQuery();
                Custpk = Convert.ToInt32(_with60.Parameters["RETURN_VALUE"].Value);
            }
            catch (Exception ex)
            {
                return false;
            }
            return true;
        }

        #endregion "Temp Customer Save"

        #region "Save Special Requirement"

        /// <summary>
        /// Saves the win special req.
        /// </summary>
        /// <param name="dsSepcialReq">The ds sepcial req.</param>
        /// <returns></returns>
        public ArrayList SaveWinSpecialReq(DataSet dsSepcialReq)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            objWK.MyCommand.Transaction = TRAN;
            arrMessage.Clear();
            int rowCnt = 0;
            if (dsSepcialReq.Tables[0].Rows.Count > 0)
            {
                try
                {
                    for (rowCnt = 0; rowCnt <= dsSepcialReq.Tables[0].Rows.Count - 1; rowCnt++)
                    {
                        int CntType = 0;
                        CntType = Convert.ToInt32(dsSepcialReq.Tables[0].Rows[rowCnt]["CONTAINER_TYPE_MST_FK"]);
                        string strSql = null;
                        string drCntKind = null;
                        strSql = "SELECT C.CONTAINER_KIND FROM CONTAINER_TYPE_MST_TBL C WHERE C.CONTAINER_TYPE_MST_PK= " + CntType + "";

                        var _with61 = objWK.MyCommand;
                        _with61.Parameters.Clear();
                        _with61.CommandType = CommandType.Text;
                        _with61.CommandText = strSql;
                        drCntKind = Convert.ToString(_with61.ExecuteScalar());
                        objWK.MyCommand.Parameters.Clear();
                        //if (CommodityGroup == "HAZARDOUS")
                        //{
                        //    arrMessage = SaveTransactionHZSpcl(objWK.MyCommand, objWK.MyUserName, getDefault(dsSepcialReq.Tables[0].Rows[rowCnt]["SPCL_REQ"], ""), dsSepcialReq.Tables[0].Rows[rowCnt]["JOB_CARD_TRN_CONT_PK"]);
                        //}
                        //else if (CommodityGroup == "REEFER")
                        //{
                        //    arrMessage = SaveTransactionReefer(objWK.MyCommand, objWK.MyUserName, getDefault(dsSepcialReq.Tables[0].Rows[rowCnt]["SPCL_REQ"], ""), dsSepcialReq.Tables[0].Rows[rowCnt]["JOB_CARD_TRN_CONT_PK"]);
                        //}
                    }
                    if (string.Compare(arrMessage[0].ToString(), "Saved") > 0)
                    {
                        arrMessage.Clear();
                        TRAN.Commit();
                        arrMessage.Add("All data saved successfully");
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
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
            return new ArrayList();
            // End
        }

        #endregion "Save Special Requirement"

        #region "Container Int32"

        /// <summary>
        /// Checks the valid cont.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="ContNr">The cont nr.</param>
        /// <returns></returns>
        public int CheckValidCont(int JobPK, string ContNr)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append(" SELECT COUNT (*) FROM JOB_CARD_AIR_EXP_TBL JSE,JOB_TRN_AIR_EXP_CONT JSCONT ");
            sb.Append(" WHERE JSE.JOB_CARD_AIR_EXP_PK = JSCONT.JOB_CARD_AIR_EXP_FK");
            sb.Append(" AND JSE.JOB_CARD_AIR_EXP_PK =" + JobPK);
            sb.Append(" AND UPPER(JSCONT.CONTAINER_NUMBER) = UPPER('" + ContNr + "')");
            return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
        }

        #endregion "Container Int32"

        #region "Update Job Card with WIN Activities"

        /// <summary>
        /// Updates the win details.
        /// </summary>
        /// <param name="ATADt">The ata dt.</param>
        /// <param name="ATATime">The ata time.</param>
        /// <param name="RecQty">The record qty.</param>
        /// <param name="RecWt">The record wt.</param>
        /// <param name="JobPK">The job pk.</param>
        /// <returns></returns>
        public ArrayList UpdateWinDetails(string ATADt = "", string ATATime = "", int RecQty = 0, double RecWt = 0.0, int JobPK = 0)
        {
            string UpdQuery = null;
            int i = 0;
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            arrMessage.Clear();
            try
            {
                //UPDATE JC Fields
                UpdQuery = string.Empty;
                UpdQuery += " UPDATE JOB_CARD_AIR_EXP_TBL JSE SET JSE.ARRIVAL_DATE = '" + ATADt + "',";
                UpdQuery += " JSE.WIN_REC_QTY =" + RecQty + ",JSE.WIN_REC_WT =" + RecWt;
                UpdQuery += " WHERE JSE.JOB_CARD_AIR_EXP_PK = " + JobPK;
                objWK.ExecuteCommands(UpdQuery);
                arrMessage.Add("All Data Saved Successfully");
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
            }
            return arrMessage;
        }

        #endregion "Update Job Card with WIN Activities"
    }
}