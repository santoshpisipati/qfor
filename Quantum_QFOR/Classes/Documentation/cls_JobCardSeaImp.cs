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
    public class clsJobCardSeaImp : CommonFeatures
	{
        /// <summary>
        /// The object track n trace
        /// </summary>
        cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
        #region "Property"
        /// <summary>
        /// The object vessel voyage
        /// </summary>
        cls_SeaBookingEntry objVesselVoyage = new cls_SeaBookingEntry();
        /// <summary>
        /// The COM GRP
        /// </summary>
        string ComGrp;
        /// <summary>
        /// Gets or sets the commodity group.
        /// </summary>
        /// <value>
        /// The commodity group.
        /// </value>
        public string CommodityGroup {
			get { return ComGrp; }
			set { ComGrp = value; }
		}
        #endregion

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
			strSQL.Append("    job_imp.job_card_sea_imp_pk,");
			strSQL.Append("    job_imp.jobcard_ref_no,");
			strSQL.Append("    job_imp.jobcard_date,");
			strSQL.Append("    job_imp.job_card_status,");
			strSQL.Append("    job_imp.job_card_closed_on,");
			strSQL.Append("    job_imp.WIN_TOTAL_QTY,");
			strSQL.Append("    job_imp.WIN_REC_QTY,");
			strSQL.Append("    job_imp.WIN_BALANCE_QTY,");
			strSQL.Append("    job_imp.WIN_TOTAL_WT,");
			strSQL.Append("    job_imp.WIN_REC_WT,");
			strSQL.Append("    job_imp.WIN_BALANCE_WT,");
			strSQL.Append("    job_imp.remarks,");
			strSQL.Append("    job_imp.cargo_type,");
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
			//Modified by Faheem
			//strSQL.Append(vbCrLf & "    job_imp.del_place_mst_fk,")
			//strSQL.Append(vbCrLf & "    upper(del_place.place_name) ""DeliveryPlace"",")
			strSQL.Append("    CASE WHEN JOB_IMP.CARGO_TYPE = 1 THEN JOB_IMP.PFD_FK");
			strSQL.Append("    ELSE JOB_IMP.DEL_PLACE_MST_FK END DEL_PLACE_MST_FK,");
			strSQL.Append("    CASE WHEN JOB_IMP.CARGO_TYPE = 1 THEN UPPER(PFD.PORT_ID)");
			strSQL.Append("    ELSE UPPER(DEL_PLACE.PLACE_NAME) END \"DeliveryPlace\",");
			//End
			strSQL.Append("    job_imp.port_mst_pol_fk,");
			strSQL.Append("    pol.port_id \"POL\",");
			strSQL.Append("    job_imp.port_mst_pod_fk,");
			strSQL.Append("    pod.port_id \"POD\",");
			strSQL.Append("    job_imp.operator_mst_fk,");
			strSQL.Append("    oprator.operator_id \"operator_id\",");
			strSQL.Append("    oprator.operator_name \"operator_name\",");
			strSQL.Append("    VVT.VOYAGE_TRN_PK \"VoyagePK\",");
			strSQL.Append("    V.VESSEL_ID \"vessel_id\",   ");
			strSQL.Append("    V.VESSEL_NAME \"VESSEL_NAME\",");
			strSQL.Append("    VVT.VOYAGE \"VOYAGE\",");
			//strSQL.Append(vbCrLf & "    job_imp.eta_date,")
			//strSQL.Append(vbCrLf & "    job_imp.etd_date,")
			//strSQL.Append(vbCrLf & "    job_imp.arrival_date,")
			//strSQL.Append(vbCrLf & "    job_imp.departure_date,")
			strSQL.Append("    TO_CHAR(job_imp.eta_date,DATETIMEFORMAT24)eta_date , ");
			strSQL.Append("    TO_CHAR(job_imp.etd_date,DATETIMEFORMAT24) etd_date, ");
			strSQL.Append("    TO_CHAR(job_imp.arrival_date,DATETIMEFORMAT24) arrival_date, ");
			strSQL.Append("    TO_CHAR(job_imp.departure_date,DATETIMEFORMAT24) departure_date, ");

			strSQL.Append("    job_imp.shipper_cust_mst_fk,");
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
			strSQL.Append("    job_imp.consignee_cust_mst_fk,");
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
			strSQL.Append("    job_imp.cb_agent_mst_fk,");
			strSQL.Append("    cbagnt.agent_id \"cbAgent\",");
			strSQL.Append("    cbagnt.agent_name \"cbAgentName\",");
			strSQL.Append("    job_imp.cl_agent_mst_fk,");
			strSQL.Append("    clagnt.agent_id \"clAgent\",");
			strSQL.Append("    clagnt.agent_name \"clAgentName\",");
			strSQL.Append("    job_imp.version_no,");
			strSQL.Append("    job_imp.ucr_no,");
			strSQL.Append("    job_imp.goods_description,");
			strSQL.Append("    job_imp.del_address,");
			strSQL.Append("    job_imp.hbl_ref_no,");
			strSQL.Append("    job_imp.hbl_date,");
			strSQL.Append("    job_imp.mbl_ref_no,");
			strSQL.Append("    job_imp.mbl_date,");
			strSQL.Append("    job_imp.marks_numbers,");
			strSQL.Append("    job_imp.weight_mass,");
			strSQL.Append("    job_imp.cargo_move_fk,");
			strSQL.Append("    job_imp.pymt_type,");
			strSQL.Append("    job_imp.shipping_terms_mst_fk,");
			strSQL.Append("    job_imp.insurance_amt,");
			strSQL.Append("    job_imp.insurance_currency,");
			strSQL.Append("    job_imp.pol_agent_mst_fk,");
			strSQL.Append("    polagnt.agent_id \"polAgent\",");
			strSQL.Append("    polagnt.agent_name \"polAgentName\", ");
			strSQL.Append("    job_imp.commodity_group_fk,");
			strSQL.Append("    comm.commodity_group_desc,");
			//fields are added after the UAT..
			strSQL.Append("    depot.vendor_id \"depot_id\",");
			strSQL.Append("    depot.vendor_name \"depot_name\",");
			strSQL.Append("    depot.vendor_mst_pk \"depot_pk\",");
			strSQL.Append("    carrier.customer_id \"carrier_id\",");
			strSQL.Append("    carrier.customer_name \"carrier_name\",");
			strSQL.Append("    carrier.customer_mst_pk \"carrier_pk\",");
			strSQL.Append("    country.country_id \"country_id\",");
			strSQL.Append("    country.country_name \"country_name\",");
			strSQL.Append("    country.country_mst_pk \"country_mst_pk\",");
			strSQL.Append("    job_imp.da_number \"da_number\",");
			// strSQL.Append(vbCrLf & "    job_imp.hbl_exp_tbl_fk, ")
			//strSQL.Append(vbCrLf & "    job_imp.mbl_exp_tbl_fk, ")
			strSQL.Append("    job_imp.clearance_address, ");
			//' strSQL.Append(vbCrLf & "    job_imp.JC_AUTO_MANUAL ")
			//adding by thiyagarajan on 26/3/09
			strSQL.Append("    job_imp.JC_AUTO_MANUAL,job_imp.HBL_SURRENDERED HBLSURR,job_imp.MBL_SURRENDERED MBLSURR,");
			strSQL.Append("    job_imp.MBLSURRDT,job_imp.HBLSURRDT, ");
			//adding by thiyagarajan on 16/4/09
			//end

			//Code Added By Anil on 17 Aug 09
			strSQL.Append("    job_imp.sb_number,job_imp.sb_date, ");
			//End By Anil

			//added by surya prasad for introducing base currency.
			strSQL.Append("   curr.currency_id,");
			strSQL.Append("   curr.currency_mst_pk base_currency_fk,");
			//end
			//'
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
			strSQL.Append("  job_imp.Bro_Remarks,");
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
			//strSQL.Append(vbCrLf & "    NVL(EMP.EMPLOYEE_NAME,CONS_SE.EMPLOYEE_NAME) SALES_EXEC_NAME,")
			strSQL.Append("    NVL(EMP.EMPLOYEE_MST_PK,(SELECT NVL(CONS_SE.EMPLOYEE_MST_PK,0) FROM EMPLOYEE_MST_TBL CONS_SE,CUSTOMER_MST_TBL CONSIG ");
			strSQL.Append("    WHERE CONS_SE.EMPLOYEE_MST_PK(+)= CONSIG.REP_EMP_MST_FK ");
			strSQL.Append("  AND JOB_IMP.CUST_CUSTOMER_MST_FK = CONSIG.CUSTOMER_MST_PK)) SALES_EXEC_FK,");
			strSQL.Append("    NVL(EMP.EMPLOYEE_ID,(SELECT NVL(CONS_SE.EMPLOYEE_ID,0) FROM EMPLOYEE_MST_TBL CONS_SE,CUSTOMER_MST_TBL CONSIG ");
			strSQL.Append("    WHERE CONS_SE.EMPLOYEE_MST_PK(+)= CONSIG.REP_EMP_MST_FK ");
			strSQL.Append("  AND JOB_IMP.CUST_CUSTOMER_MST_FK = CONSIG.CUSTOMER_MST_PK)) SALES_EXEC_ID,");
			strSQL.Append("    NVL(EMP.EMPLOYEE_NAME,(SELECT NVL(CONS_SE.EMPLOYEE_NAME,0) FROM EMPLOYEE_MST_TBL CONS_SE,CUSTOMER_MST_TBL CONSIG ");
			strSQL.Append("    WHERE CONS_SE.EMPLOYEE_MST_PK(+)= CONSIG.REP_EMP_MST_FK ");
			strSQL.Append("  AND JOB_IMP.CUST_CUSTOMER_MST_FK = CONSIG.CUSTOMER_MST_PK)) SALES_EXEC_NAME,");
			//'For ImportXM JC its crashing :Demo
			strSQL.Append("    NVL(job_imp.chk_can,0) chk_can,NVL(job_imp.chk_do,0) chk_do,NVL(job_imp.chk_rec,0) chk_rec,NVL(job_imp.chk_pay,0) chk_pay,");
			strSQL.Append("    NVL(job_imp.cc_req,0) cc_req,NVL(job_imp.cc_ie,0) cc_ie,job_imp.LINE_BKG_NR,job_imp.LINE_BKG_DT,job_imp.LINER_TERMS_FK,job_imp.ONC_FK,job_imp.ONC_MOVE_FK,");
			strSQL.Append("    job_imp.cha_agent_mst_fk,");
			strSQL.Append("    CHAAGNT.VENDOR_ID \"CHAAgentID\",");
			strSQL.Append("    CHAAGNT.VENDOR_NAME \"CHAAgentName\", ");
			strSQL.Append("    job_imp.WIN_UNIQ_REF_ID, ");
			strSQL.Append("    job_imp.WIN_SEND_USER_NAME,");
			strSQL.Append("    job_imp.WIN_SEND_SECRET_KEY, ");
			strSQL.Append("    job_imp.WIN_MEM_JOBREF_NR, ");
			//strSQL.Append(vbCrLf & "    job_imp.LINER_TERMS_FK,")
			strSQL.Append("    job_imp.RFS_DATE,");
			strSQL.Append("    job_imp.RFS,");
			strSQL.Append("    job_imp.WIN_QUOT_REF_NR,");
			strSQL.Append("    job_imp.WIN_INCO_PLACE,");
			strSQL.Append("    job_imp.POO_FK, ");
			strSQL.Append("    job_imp.WIN_CONSOL_REF_NR,");
			strSQL.Append("    job_imp.WIN_PICK_ONC_MOVE_FK,");
			strSQL.Append("    job_imp.WIN_XML_STATUS,");
			strSQL.Append("    job_imp.WIN_CUTTOFF_DT,");
			strSQL.Append("    job_imp.WIN_CUTTOFF_TIME");
			strSQL.Append("  FROM ");
			strSQL.Append("    job_card_sea_imp_tbl job_imp,");
			strSQL.Append("    port_mst_tbl POD,");
			strSQL.Append("    port_mst_tbl POL,");
			strSQL.Append("    port_mst_tbl PFD,");
			//FAHEEM
			//strSQL.Append(vbCrLf & "    port_mst_tbl POO,")
			//strSQL.Append(vbCrLf & "    customer_mst_tbl cust,")
			//strSQL.Append(vbCrLf & "    customer_mst_tbl consignee,")
			//strSQL.Append(vbCrLf & "    customer_mst_tbl shipper,")
			//strSQL.Append(vbCrLf & "    customer_mst_tbl notify1,")
			strSQL.Append("    customer_mst_tbl notify2,");
			strSQL.Append("    place_mst_tbl del_place,");
			strSQL.Append("    operator_mst_tbl oprator,");
			strSQL.Append("    agent_mst_tbl clagnt, ");
			strSQL.Append("    agent_mst_tbl cbagnt,");
			strSQL.Append("    agent_mst_tbl polagnt,");
			strSQL.Append("    commodity_group_mst_tbl comm, ");
			strSQL.Append("    VESSEL_VOYAGE_TBL V, ");
			strSQL.Append("    VESSEL_VOYAGE_TRN VVT, ");
			strSQL.Append("    vendor_mst_tbl  depot,");
			strSQL.Append("    customer_mst_tbl  carrier,");
			strSQL.Append("    country_mst_tbl country,");
			strSQL.Append("    currency_type_mst_tbl curr,");
			strSQL.Append("    EMPLOYEE_MST_TBL        EMP, ");
			//strSQL.Append(vbCrLf & "    EMPLOYEE_MST_TBL        CONS_SE, ") 'CONSIGNEE SALES PERSON
			strSQL.Append("    VENDOR_MST_TBL          CHAAGNT ");
			strSQL.Append(" WHERE ");
			strSQL.Append("    job_imp.job_card_sea_imp_pk          = " + jobCardPK);
			strSQL.Append("    AND job_imp.port_mst_pol_fk          =  pol.port_mst_pk");
			strSQL.Append("    AND PFD.PORT_MST_PK(+) = JOB_IMP.PFD_FK ");
			//strSQL.Append(vbCrLf & "    AND POO.PORT_MST_PK(+) = JOB_IMP.POO_FK ")
			strSQL.Append("    AND job_imp.port_mst_pod_fk          =  pod.port_mst_pk");
			strSQL.Append("    AND job_imp.del_place_mst_fk         =  del_place.place_pk(+)");
			//strSQL.Append(vbCrLf & "    AND job_imp.cust_customer_mst_fk     =  cust.customer_mst_pk(+) ")
			strSQL.Append("    AND job_imp.operator_mst_fk          =  oprator.operator_mst_pk");
			//strSQL.Append(vbCrLf & "    AND job_imp.shipper_cust_mst_fk      =  shipper.customer_mst_pk(+)")
			//strSQL.Append(vbCrLf & "    AND job_imp.consignee_cust_mst_fk    =  consignee.customer_mst_pk(+)")
			//strSQL.Append(vbCrLf & "    AND job_imp.notify1_cust_mst_fk      =  notify1.customer_mst_pk(+)")
			strSQL.Append("    AND job_imp.Notify2_Cust_Mst_Fk      =  notify2.customer_mst_pk(+)");
			strSQL.Append("    AND job_imp.cl_agent_mst_fk          =  clagnt.agent_mst_pk(+)");
			strSQL.Append("    AND job_imp.Cb_Agent_Mst_Fk          =  cbagnt.agent_mst_pk(+)");
			strSQL.Append("    AND job_imp.pol_agent_mst_fk         =  polagnt.agent_mst_pk(+)");
			strSQL.Append("    AND job_imp.commodity_group_fk       =  comm.commodity_group_pk(+)");
			strSQL.Append("    AND job_imp.CHA_AGENT_MST_FK = CHAAGNT.VENDOR_MST_PK(+) ");

			//conditions are added after the UAT..
			strSQL.Append("    AND job_imp.transporter_depot_fk     =  depot.vendor_mst_pk(+)");
			//strSQL.Append(vbCrLf & "    AND job_imp.transporter_carrier_fk   =  carrier.vendor_mst_pk(+)")
			strSQL.Append("    AND job_imp.transporter_carrier_fk   =  carrier.customer_mst_pk(+)");
			//Modified by Mayur for Consignee
			strSQL.Append("    AND job_imp.country_origin_fk        =  country.country_mst_pk(+)");
			strSQL.Append("    AND VVT.VESSEL_VOYAGE_TBL_FK         = V.VESSEL_VOYAGE_TBL_PK(+)");
			strSQL.Append("    AND JOB_IMP.VOYAGE_TRN_FK            = VVT.VOYAGE_TRN_PK(+)");

			// strSQL.Append(vbCrLf & "    AND consignee.REP_EMP_MST_FK=CONS_SE.EMPLOYEE_MST_PK(+) ")
			strSQL.Append("    AND JOB_IMP.EXECUTIVE_MST_FK=EMP.EMPLOYEE_MST_PK(+) ");
			//added by surya prasad for introducing base currency.
			strSQL.Append("     and curr.currency_mst_pk(+) = job_imp.BASE_CURRENCY_MST_FK");
			//end
			WorkFlow objWF = new WorkFlow();
			try {
				return objWF.GetDataSet(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
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
			try {
				return objWF.ExecuteScaler(SQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
        #endregion

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
			SQL.Append("      container_number,");
			SQL.Append("      container_type_mst_fk,");
			//SQL.Append(vbCrLf & "      container_number,")
			SQL.Append("      seal_number,");
			SQL.Append("      DECODE(CONT_TRN.CONTAINER_OWNER_TYPE_FK,1,'COC',2,'SOC') CONTAINER_OWNER_TYPE_FK,");
			//SQL.Append(vbCrLf & "    ' ' fetch_comm,")
			SQL.Append("       (SELECT REPLACE(ROWTOCOL('SELECT DISTINCT CMD.COMMODITY_NAME||'';''");
			SQL.Append("           FROM job_trn_cont JOB_CONT, JOB_TRN_COMMODITY JCD,COMMODITY_MST_TBL CMD");
			SQL.Append("          WHERE JOB_CONT.job_trn_cont_pk = JCD.JOB_TRN_CONT_FK AND JCD.COMMODITY_MST_FK=CMD.COMMODITY_MST_PK ");
			SQL.Append("          AND JOB_CONT.job_trn_cont_pk='||CONT_TRN.job_trn_cont_pk),';,',';') FROM DUAL) FETCH_COMM,");
			SQL.Append("      pack_count,");
			SQL.Append("      net_weight,");
			//SQL.Append(vbCrLf & "      volume_in_cbm,")
			SQL.Append("      gross_weight,");
			SQL.Append("      volume_in_cbm,");
			//SQL.Append(vbCrLf & "      net_weight,")
			SQL.Append("      chargeable_weight,");
			//SQL.Append("            CASE WHEN JOB_CARD.CARGO_TYPE=1 THEN ")
			SQL.Append("   (SELECT ROWTOCOL('SELECT PT.PACK_TYPE_DESC FROM PACK_TYPE_MST_TBL PT WHERE PT.PACK_TYPE_MST_PK IN (");
			SQL.Append("     SELECT DISTINCT JC.PACK_TYPE_FK FROM job_trn_cont JOB,JOB_TRN_COMMODITY JC ");
			SQL.Append("     WHERE JOB.job_trn_cont_pk=JC.JOB_TRN_CONT_FK ");
			SQL.Append("     AND JOB.job_trn_cont_pk='||CONT_TRN.job_trn_cont_pk||')') FROM DUAL) PACK_TYPE_MST_FK, ");
			//SQL.Append("       ELSE TO_CHAR(CONT_TRN.PACK_TYPE_MST_FK) END PACK_TYPE_MST_FK,")
			//SQL.Append(vbCrLf & "      pack_type_mst_fk,")
			//SQL.Append(vbCrLf & "      pack_count,")
			SQL.Append("      commodity_name,");
			//SQL.Append(vbCrLf & "    ' ' fetch_comm,") 'added by Prakash chandra  for implementing multiple commodities
			//SQL.Append(vbCrLf & "      to_char(gen_land_date,dateformat) gen_land_date,")
			SQL.Append("      TO_CHAR(load_date,DATETIMEFORMAT24) gen_land_date,");
			SQL.Append("      container_type_mst_id,");
			SQL.Append("      commodity_mst_fk,");

			SQL.Append("     COMMODITY_MST_FKS,cont_trn.prev_cont_pk ");
			//added By prakash chandra on 6/1/2009 for implementing multiple commodities
			SQL.Append("FROM");
			SQL.Append("      job_trn_cont cont_trn,");
			SQL.Append("      JOB_CARD_TRN job_card,");
			SQL.Append("      container_type_mst_tbl cont,");
			SQL.Append("      pack_type_mst_tbl pack,");
			SQL.Append("      commodity_mst_tbl comm");
			SQL.Append("WHERE");
			SQL.Append("      cont_trn.job_card_trn_fk =" + jobCardPK);
			SQL.Append("      AND cont_trn.job_card_trn_fk = job_card.JOB_CARD_TRN_PK");
			SQL.Append("      AND cont_trn.container_type_mst_fk = cont.container_type_mst_pk(+)");
			SQL.Append("      AND cont_trn.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
			SQL.Append("      AND cont_trn.commodity_mst_fk = comm.commodity_mst_pk(+)");

			try {
				return objWF.GetDataSet(SQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

        /// <summary>
        /// Gets the container data win.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet GetContainerDataWIN(string jobCardPK = "0")
		{

			WorkFlow objWF = new WorkFlow();
			StringBuilder SQL = new StringBuilder();

			SQL.Append("SELECT ");
			SQL.Append("      job_card_sea_imp_cont_pk,");
			SQL.Append("      container_number,");
			SQL.Append("      container_type_mst_fk,");
			//SQL.Append(vbCrLf & "      container_number,")
			SQL.Append("      seal_number,");
			SQL.Append("      DECODE(CONT_TRN.CONTAINER_OWNER_TYPE_FK,1,'COC',2,'SOC') CONTAINER_OWNER_TYPE_FK,");
			SQL.Append("    ' ' fetch_comm,");
			//'Commented Because fetched column value is null error is coming while Upload
			//SQL.Append(vbCrLf & "       (SELECT REPLACE(ROWTOCOL('SELECT DISTINCT CMD.COMMODITY_NAME||'';''") 
			//SQL.Append(vbCrLf & "           FROM JOB_TRN_SEA_IMP_CONT JOB_CONT, JOBCARD_COMMODITY_DTL_IMP JCD,COMMODITY_MST_TBL CMD")
			//SQL.Append(vbCrLf & "          WHERE JOB_CONT.JOB_CARD_SEA_IMP_CONT_PK = JCD.JOB_TRN_CONT_IMP_FK AND JCD.COMMODITY_MST_FK=CMD.COMMODITY_MST_PK ")
			//SQL.Append(vbCrLf & "          AND JOB_CONT.JOB_CARD_SEA_IMP_CONT_PK='||CONT_TRN.JOB_CARD_SEA_IMP_CONT_PK),';,',';') FROM DUAL) FETCH_COMM,")
			SQL.Append("      pack_count,");
			SQL.Append("      net_weight,");
			//SQL.Append(vbCrLf & "      volume_in_cbm,")
			SQL.Append("      gross_weight,");
			SQL.Append("      volume_in_cbm,");
			//SQL.Append(vbCrLf & "      net_weight,")
			SQL.Append("      chargeable_weight,");
			//SQL.Append("            CASE WHEN JOB_CARD.CARGO_TYPE=1 THEN ")
			SQL.Append("   (SELECT ROWTOCOL('SELECT PT.PACK_TYPE_DESC FROM PACK_TYPE_MST_TBL PT WHERE PT.PACK_TYPE_MST_PK IN (");
			SQL.Append("     SELECT DISTINCT JC.PACK_TYPE_FK FROM JOB_TRN_SEA_IMP_CONT JOB,JOBCARD_COMMODITY_DTL_IMP JC ");
			SQL.Append("     WHERE JOB.JOB_CARD_SEA_IMP_CONT_PK=JC.JOB_TRN_CONT_IMP_FK ");
			SQL.Append("     AND JOB.JOB_CARD_SEA_IMP_CONT_PK='||CONT_TRN.JOB_CARD_SEA_IMP_CONT_PK||')') FROM DUAL) PACK_TYPE_MST_FK, ");
			//SQL.Append("       ELSE TO_CHAR(CONT_TRN.PACK_TYPE_MST_FK) END PACK_TYPE_MST_FK,")
			//SQL.Append(vbCrLf & "      pack_type_mst_fk,")
			//SQL.Append(vbCrLf & "      pack_count,")
			SQL.Append("      commodity_name,");
			//SQL.Append(vbCrLf & "    ' ' fetch_comm,") 'added by Prakash chandra  for implementing multiple commodities
			//SQL.Append(vbCrLf & "      to_char(gen_land_date,dateformat) gen_land_date,")
			SQL.Append("      TO_CHAR(gen_land_date,DATETIMEFORMAT24) gen_land_date,");
			SQL.Append("      container_type_mst_id,");
			SQL.Append("      commodity_mst_fk,");

			SQL.Append("     COMMODITY_MST_FKS");
			//added By prakash chandra on 6/1/2009 for implementing multiple commodities
			SQL.Append("FROM");
			SQL.Append("      job_trn_sea_imp_cont cont_trn,");
			SQL.Append("      job_card_sea_imp_tbl job_card,");
			SQL.Append("      container_type_mst_tbl cont,");
			SQL.Append("      pack_type_mst_tbl pack,");
			SQL.Append("      commodity_mst_tbl comm");
			SQL.Append("WHERE");
			SQL.Append("      cont_trn.job_card_sea_imp_fk =" + jobCardPK);
			SQL.Append("      AND cont_trn.job_card_sea_imp_fk = job_card.job_card_sea_imp_pk");
			SQL.Append("      AND cont_trn.container_type_mst_fk = cont.container_type_mst_pk(+)");
			SQL.Append("      AND cont_trn.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
			SQL.Append("      AND cont_trn.commodity_mst_fk = comm.commodity_mst_pk(+)");

			try {
				return objWF.GetDataSet(SQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

        #endregion

        #region "Get Freight data"
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

			SQL.Append("SELECT");
			SQL.Append("   job_trn_fd_pk,");
			SQL.Append("   container_type_mst_fk,");
			SQL.Append("   frt.freight_element_id,");
			SQL.Append("   frt.freight_element_name,");
			SQL.Append("   frt.freight_element_mst_pk,");
			SQL.Append("   fd_trn.basis,");
			SQL.Append("   fd_trn.quantity,");
			SQL.Append("   DECODE(fd_trn.freight_type,1,'Prepaid',2,'Collect') freight_type,");
			//latha
			SQL.Append("   fd_trn.location_mst_fk  \"location_fk\" ,");
			SQL.Append("   loc.location_id \"location_id\" ,");

			SQL.Append("   fd_trn.frtpayer_cust_mst_fk \"frtpayer_mst_fk\" ,");
			SQL.Append("   cus.customer_id \"frtpayer\",");
			SQL.Append("   fd_trn.currency_mst_fk,");
			SQL.Append("   fd_trn.freight_amt,");
			//SQL.Append(vbCrLf & "   fd_trn.currency_mst_fk,")
			if (Convert.ToInt32(BaseCurrFk) != 0) {
				SQL.Append("        NVL(GET_EX_RATE(fd_trn.CURRENCY_MST_FK, " + BaseCurrFk + ", job_card.JOBCARD_DATE),0) AS ROE,");
				SQL.Append("    (fd_trn.FREIGHT_AMT* NVL(GET_EX_RATE(fd_trn.CURRENCY_MST_FK, " + BaseCurrFk + ", job_card.JOBCARD_DATE),0)) total_amt,");
			} else {
				SQL.Append("   fd_trn.exchange_rate roe,");
				SQL.Append("   (fd_trn.freight_amt*fd_trn.exchange_rate) total_amt,");
			}
			SQL.Append("   '0' \"Delete\",frt.CREDIT");
			SQL.Append("FROM");
			SQL.Append("   job_trn_fd fd_trn,");
			SQL.Append("   JOB_CARD_TRN job_card,");
			SQL.Append("   currency_type_mst_tbl curr,");
			SQL.Append("   freight_element_mst_tbl frt,");
			SQL.Append("    parameters_tbl prm,");
			SQL.Append("   container_type_mst_tbl cont,");
			//latha
			SQL.Append("   location_mst_tbl loc,");
			SQL.Append("   customer_mst_tbl cus");

			SQL.Append("WHERE");
			SQL.Append("   fd_trn.job_card_trn_fk = " + jobCardPK);
			SQL.Append("   AND fd_trn.job_card_trn_fk = job_card.JOB_CARD_TRN_PK");
			SQL.Append("   AND fd_trn.container_type_mst_fk = cont.container_type_mst_pk(+)");
			SQL.Append("   AND fd_trn.currency_mst_fk = curr.currency_mst_pk(+)");
			SQL.Append("   AND fd_trn.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
			SQL.Append("   AND fd_trn.frtpayer_cust_mst_fk = cus.customer_mst_pk(+)");
			SQL.Append("   AND fd_trn.location_mst_fk = loc.location_mst_pk(+)");
			SQL.Append("   AND NVL(fd_trn.SERVICE_TYPE_FLAG,0)<>1");
			SQL.Append("   ORDER BY CONT.PREFERENCES,FRT.PREFERENCE ");

			try {
				return objWF.GetDataSet(SQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        /// <summary>
        /// Gets the FRT data.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet GetFrtData(string jobCardPK = "0")
		{

			WorkFlow objWF = new WorkFlow();
			StringBuilder SQL = new StringBuilder();
			SQL.Append("SELECT");
			SQL.Append("   fd_trn.inv_cust_trn_sea_imp_fk,");
			SQL.Append("   fd_trn.inv_agent_trn_sea_imp_fk,");
			SQL.Append("   fd_trn.consol_invoice_trn_fk");
			SQL.Append(" FROM");
			SQL.Append("   job_trn_sea_imp_fd fd_trn,");
			SQL.Append("   job_card_sea_imp_tbl job_card,");
			SQL.Append("   currency_type_mst_tbl curr,");
			SQL.Append("   freight_element_mst_tbl frt,");
			SQL.Append("   container_type_mst_tbl cont,");
			SQL.Append("   location_mst_tbl loc,");
			SQL.Append("   customer_mst_tbl cus");
			SQL.Append(" WHERE");
			SQL.Append("   fd_trn.job_card_sea_imp_fk = " + jobCardPK);
			SQL.Append("   AND fd_trn.job_card_sea_imp_fk = job_card.job_card_sea_imp_pk");
			SQL.Append("   AND fd_trn.container_type_mst_fk = cont.container_type_mst_pk(+)");
			SQL.Append("   AND fd_trn.currency_mst_fk = curr.currency_mst_pk(+)");
			SQL.Append("   AND fd_trn.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
			SQL.Append("   AND fd_trn.frtpayer_cust_mst_fk = cus.customer_mst_pk(+)");
			SQL.Append("   AND fd_trn.location_mst_fk = loc.location_mst_pk(+)");
			SQL.Append("   AND NVL(fd_trn.SERVICE_TYPE_FLAG,0)<>1");
			try {
				return objWF.GetDataSet(SQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

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
			SQL.Append("   tp_trn.job_trn_sea_imp_tp_pk,");
			SQL.Append("   tp_trn.transhipment_no,");
			SQL.Append("   tp_trn.port_mst_fk,");
			SQL.Append("   port.port_id,");
			SQL.Append("   port.port_name,");
			SQL.Append("   DECODE(tp_trn.WIN_TYPE,1,'Main Carriage',2,'On Carriage') WIN_TYPE,");
			SQL.Append("   (SELECT QDT.DD_ID FROM QFOR_DROP_DOWN_TBL QDT ");
			SQL.Append("   WHERE QDT.DD_FLAG = 'Carriage_Mode_IMP_POD' ");
			SQL.Append("   AND QDT.DROPDOWN_PK = tp_trn.WIN_MODE) WIN_MODE,");
			SQL.Append("   tp_trn.vessel_name,");
			SQL.Append("   tp_trn.voyage,");
			SQL.Append("   TO_CHAR(tp_trn.eta_date,'" + dateFormat + "') eta_date,");
			SQL.Append("   TO_CHAR(tp_trn.etd_date,'" + dateFormat + "') etd_date,");
			SQL.Append("   '0' \"Delete\",");
			SQL.Append("   tp_trn.voyage_trn_fk \"GridVoyagePK\" ");
			SQL.Append("FROM");
			SQL.Append("   job_trn_sea_imp_tp tp_trn,");
			SQL.Append("   job_card_sea_imp_tbl job_card,");
			SQL.Append("   vessel_voyage_trn vvt,");
			SQL.Append("   port_mst_tbl port");
			SQL.Append("WHERE");
			SQL.Append("   tp_trn.job_card_sea_imp_fk =" + jobCardPK);
			SQL.Append("   AND tp_trn.job_card_sea_imp_fk = job_card.job_card_sea_imp_pk");
			SQL.Append("   AND tp_trn.port_mst_fk = port.port_mst_pk(+)");
			SQL.Append("   and tp_trn.voyage_trn_fk = vvt.voyage_trn_pk(+)");

			try {
				return objWF.GetDataSet(SQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch PIA details"
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
			SQL.Append("   pia_trn.job_trn_sea_imp_pia_pk,");
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
			SQL.Append("   pia_trn.vendor_mst_fk, ");
			SQL.Append("   pia_trn.cost_element_mst_fk,");
			SQL.Append("   pia_trn.attached_file_name,'false' as \"Delete\", MJC_TRN_SEA_IMP_PIA_FK");
			SQL.Append("FROM");
			SQL.Append("   job_trn_sea_imp_pia pia_trn,");
			SQL.Append("   cost_element_mst_tbl cost_elmnt,");
			SQL.Append("   currency_type_mst_tbl curr,");
			SQL.Append("   job_card_sea_imp_tbl job_card");
			SQL.Append("WHERE");
			SQL.Append("   pia_trn.job_card_sea_imp_fk =" + jobCardPK);
			SQL.Append("   AND pia_trn.job_card_sea_imp_fk = job_card.job_card_sea_imp_pk");
			SQL.Append("   AND pia_trn.cost_element_mst_fk = cost_elmnt.cost_element_mst_pk(+)");
			SQL.Append("   AND pia_trn.currency_mst_fk     = curr.currency_mst_pk(+)");

			try {
				return objWF.GetDataSet(SQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
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
			SQL.Append("   pia_trn.invoice_sea_tbl_fk,");
			SQL.Append("   pia_trn.inv_agent_trn_sea_imp_fk,");
			SQL.Append("   pia_trn.inv_supplier_fk");
			SQL.Append("FROM");
			SQL.Append("   job_trn_sea_imp_pia pia_trn,");
			SQL.Append("   cost_element_mst_tbl cost_elmnt,");
			SQL.Append("   currency_type_mst_tbl curr,");
			SQL.Append("   job_card_sea_imp_tbl job_card");
			SQL.Append("WHERE");
			SQL.Append("   pia_trn.job_card_sea_imp_fk =" + jobCardPK);
			SQL.Append("   AND pia_trn.job_card_sea_imp_fk = job_card.job_card_sea_imp_pk");
			SQL.Append("   AND pia_trn.cost_element_mst_fk = cost_elmnt.cost_element_mst_pk(+)");
			SQL.Append("   AND pia_trn.currency_mst_fk     = curr.currency_mst_pk(+)");

			try {
				return objWF.GetDataSet(SQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

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
			try {
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
				strSQL.Append("       JEC.ESTIMATED_COST,");
				strSQL.Append("       ROUND(GET_EX_RATE_BUY(JEC.CURRENCY_MST_FK, " + basecurrency + ", round(TO_DATE(JOB.JOBCARD_DATE,DATEFORMAT) - .5)), 6) AS ROE,");
				strSQL.Append("       JEC.TOTAL_COST,");
				strSQL.Append("       ''DEL_FLAG,");
				strSQL.Append("       'true' SEL_FLAG,");
				strSQL.Append("       JEC.LOCATION_MST_FK,");
				strSQL.Append("       JEC.CURRENCY_MST_FK,");
				strSQL.Append("(SELECT IT.JOB_TRN_EST_FK");
				strSQL.Append("          FROM INV_SUPPLIER_TRN_TBL IT, INV_SUPPLIER_TBL I");
				strSQL.Append("         WHERE IT.JOB_TRN_EST_FK = JEC.JOB_TRN_COST_PK");
				strSQL.Append("           AND I.INV_SUPPLIER_PK = IT.INV_SUPPLIER_TBL_FK");
				strSQL.Append("           AND I.BUSINESS_TYPE = 2");
				strSQL.Append("           AND I.PROCESS_TYPE = 2) JOB_TRN_EST_FK");
				strSQL.Append("  FROM JOB_TRN_COST   JEC,");
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
				strSQL.Append("   AND NVL(JEC.SERVICE_TYPE_FLAG,0) <>1 ");
				strSQL.Append("   ORDER BY CMT.PREFERENCE");
				return objWF.GetDataSet(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
        /// <summary>
        /// Fetches the cost det.
        /// </summary>
        /// <param name="jobcardpk">The jobcardpk.</param>
        /// <returns></returns>
        public DataSet FetchCostDet(int jobcardpk)
		{
			try {
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
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #endregion

        // By Amit to Ftech the Vessel/Voyage Detail
        // On 29-March-07 for EFS must have
        #region "Fetch Vessel/Voyage Detail"
        /// <summary>
        /// Fetches the voyage detail.
        /// </summary>
        /// <param name="VoyagePk">The voyage pk.</param>
        /// <returns></returns>
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

			try {
				return objWF.GetDataSet(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion
        // End

        // By Amit to Fetch Agent Detail & JCType
        // On 04-April-07 for 
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
			try {
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
				strQuery.Append(" AND AMST.LOCATION_AGENT = 1" );
				strQuery.Append(" AND IJOB.JOB_CARD_SEA_IMP_PK = '" + JobCardPK + "'");
				return objWF.GetDataSet(strQuery.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
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
			try {
				strQuery.Append(" SELECT JC.JC_AUTO_MANUAL " );
				strQuery.Append(" FROM JOB_CARD_SEA_IMP_TBL JC" );
				strQuery.Append(" WHERE JC.JOB_CARD_SEA_IMP_PK = '" + JobCardPK + "'");
				return objWF.GetDataSet(strQuery.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion
        // End

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
        /// <param name="Update">if set to <c>true</c> [update].</param>
        /// <param name="isEdting">if set to <c>true</c> [is edting].</param>
        /// <param name="ucrNo">The ucr no.</param>
        /// <param name="jobCardRefNumber">The job card reference number.</param>
        /// <param name="userLocation">The user location.</param>
        /// <param name="employeeID">The employee identifier.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsOtherCharges">The ds other charges.</param>
        /// <param name="PODetails">The po details.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <param name="dsDoc">The ds document.</param>
        /// <param name="IsWINSave">The is win save.</param>
        /// <param name="DSShipper">The ds shipper.</param>
        /// <param name="DSCons">The ds cons.</param>
        /// <param name="DSNotify">The ds notify.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet M_DataSet, DataSet dsContainerData, DataSet dsTPDetails, DataSet dsFreightDetails, DataSet dsPurchaseInventory, DataSet dsCostDetails, DataSet dsPickUpDetails, DataSet dsDropDetails, bool Update, bool isEdting,
		object ucrNo, string jobCardRefNumber, string userLocation, string employeeID, long JobCardPK, DataSet dsOtherCharges, string PODetails = "", DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null, DataSet dsDoc = null,
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
			// To save in Vessel/Voyage
			//
			arrMessage.Clear();
			//If Update = True Then
			//    strVoyagepk = 0
			//End If
			if (Convert.ToString(strVoyagepk) == "0" & !string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["VESSEL_ID"].ToString())) {
				//strVoyagepk = 0
				TRAN1 = objWK.MyConnection.BeginTransaction();
				objWK.MyCommand.Transaction = TRAN1;
				objWK.MyCommand.Connection = objWK.MyConnection;

				arrMessage = objVesselVoyage.SaveVesselMaster(strVoyagepk, getDefault(M_DataSet.Tables[0].Rows[0]["VESSEL_NAME"], "").ToString(), Convert.ToInt64(getDefault(M_DataSet.Tables[0].Rows[0]["OPERATOR_MST_FK"], 0)), getDefault(M_DataSet.Tables[0].Rows[0]["VESSEL_ID"], "").ToString(), getDefault(M_DataSet.Tables[0].Rows[0]["VOYAGE"], "").ToString(), objWK.MyCommand, Convert.ToInt64(getDefault(M_DataSet.Tables[0].Rows[0]["PORT_MST_POL_FK"], 0)), Convert.ToString(M_DataSet.Tables[0].Rows[0]["PORT_MST_POD_FK"]), DateTime.MinValue, Convert.ToDateTime(getDefault(Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["ETD_DATE"], DateTime.MinValue)), null)),
				DateTime.MinValue, Convert.ToDateTime(getDefault(Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["ETA_DATE"], DateTime.MinValue)), null)), Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["departure_date"], null)), Convert.ToDateTime(getDefault(M_DataSet.Tables[0].Rows[0]["arrival_date"], null)));
				M_DataSet.Tables[0].Rows[0]["VOYAGEPK"] = strVoyagepk;
				if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0)) {
					TRAN1.Rollback();
					return arrMessage;
				} else {
					TRAN1.Commit();
					arrMessage.Clear();
				}
			}
			///

			TRAN = objWK.MyConnection.BeginTransaction();
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

			OracleCommand insPurchaseInvDetails = new OracleCommand();
			OracleCommand updPurchaseInvDetails = new OracleCommand();
			OracleCommand delPurchaseInvDetails = new OracleCommand();

			OracleCommand insCostDetails = new OracleCommand();
			//'Added By Koteshwari 
			OracleCommand updCostDetails = new OracleCommand();
			OracleCommand delCostDetails = new OracleCommand();

			//Dim insIncomeChargeDetails As New OracleClient.OracleCommand
			//Dim updIncomeChargeDetails As New OracleClient.OracleCommand
			//Dim delIncomeChargeDetails As New OracleClient.OracleCommand

			//Dim insExpenseChargeDetails As New OracleClient.OracleCommand
			//Dim updExpenseChargeDetails As New OracleClient.OracleCommand
			//Dim delExpenseChargeDetails As New OracleClient.OracleCommand

			OracleCommand insOtherChargesDetails = new OracleCommand();
			OracleCommand updOtherChargesDetails = new OracleCommand();
			OracleCommand delOtherChargesDetails = new OracleCommand();

			DataSet dsTrackNtrace = new DataSet();

			if (isEdting == false) {
				jobCardRefNumber = GenerateProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt64(userLocation), Convert.ToInt64(employeeID), System.DateTime.Now,"" , "","" , M_LAST_MODIFIED_BY_FK);
			}

			// ucrNo = ucrNo & jobCardRefNumber

			try {
				//'For Customer Save
				if (IsWINSave == 1) {
					if ((DSShipper != null)) {
						if (!(SaveWINCustomerTemp(objWK, TRAN, DSShipper, ShipperPK, 1))) {
							arrMessage.Add("Error while saving Customer");
							return arrMessage;
						}
					}
					if ((DSCons != null)) {
						if (!(SaveWINCustomerTemp(objWK, TRAN, DSCons, ConsigneePK, 2))) {
							arrMessage.Add("Error while saving Customer");
							return arrMessage;
						}
					}
					if ((DSNotify != null)) {
						if (!(SaveWINCustomerTemp(objWK, TRAN, DSNotify, NotifyPK, 3))) {
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
				_with1.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_CARD_SEA_IMP_TBL_INS";
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

				if (ConsigneePK > 0 & IsWINSave == 1) {
					insCommand.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", ConsigneePK).Direction = ParameterDirection.Input;
				} else {
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

				//insCommand.Parameters.Add("VOYAGE_FK_IN", OracleClient.OracleDbType.Varchar2, 10, "VoyagePK").Direction = ParameterDirection.Input
				insCommand.Parameters.Add("VOYAGE_FK_IN", getDefault(strVoyagepk, "")).Direction = ParameterDirection.Input;
				insCommand.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

				//insCommand.Parameters.Add("ETA_DATE_IN", OracleClient.OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input
				//insCommand.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current

				if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["ETA_DATE"].ToString())) {
					insCommand.Parameters.Add("ETA_DATE_IN", "").Direction = ParameterDirection.Input;
				} else {
					insCommand.Parameters.Add("ETA_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["ETA_DATE"])).Direction = ParameterDirection.Input;
				}
				insCommand.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

				//insCommand.Parameters.Add("ETD_DATE_IN", OracleClient.OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input
				//insCommand.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current

				if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["ETD_DATE"].ToString())) {
					insCommand.Parameters.Add("ETD_DATE_IN", "").Direction = ParameterDirection.Input;
				} else {
					insCommand.Parameters.Add("ETD_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["ETD_DATE"])).Direction = ParameterDirection.Input;
				}
				insCommand.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

				//insCommand.Parameters.Add("ARRIVAL_DATE_IN", OracleClient.OracleDbType.Date, 20, "arrival_date").Direction = ParameterDirection.Input
				//insCommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current

				if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["arrival_date"].ToString())) {
					insCommand.Parameters.Add("ARRIVAL_DATE_IN", "").Direction = ParameterDirection.Input;
				} else {
					insCommand.Parameters.Add("ARRIVAL_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["arrival_date"])).Direction = ParameterDirection.Input;
				}
				insCommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current;

				//insCommand.Parameters.Add("DEPARTURE_DATE_IN", OracleClient.OracleDbType.Date, 20, "departure_date").Direction = ParameterDirection.Input
				//insCommand.Parameters["DEPARTURE_DATE_IN"].SourceVersion = DataRowVersion.Current


				if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["departure_date"].ToString())) {
					insCommand.Parameters.Add("DEPARTURE_DATE_IN", "").Direction = ParameterDirection.Input;
				} else {
					insCommand.Parameters.Add("DEPARTURE_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["departure_date"])).Direction = ParameterDirection.Input;
				}
				insCommand.Parameters["DEPARTURE_DATE_IN"].SourceVersion = DataRowVersion.Current;

				if (ShipperPK > 0 & IsWINSave == 1) {
					insCommand.Parameters.Add("SHIPPER_CUST_MST_FK_IN", ShipperPK).Direction = ParameterDirection.Input;
				} else {
					insCommand.Parameters.Add("SHIPPER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "shipper_cust_mst_fk").Direction = ParameterDirection.Input;
					insCommand.Parameters["SHIPPER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (ConsigneePK > 0 & IsWINSave == 1) {
					insCommand.Parameters.Add("CONSIGNEE_CUST_MST_FK_IN", ConsigneePK).Direction = ParameterDirection.Input;
				} else {
					insCommand.Parameters.Add("CONSIGNEE_CUST_MST_FK_IN", OracleDbType.Int32, 10, "consignee_cust_mst_fk").Direction = ParameterDirection.Input;
					insCommand.Parameters["CONSIGNEE_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (NotifyPK > 0 & IsWINSave == 1) {
					insCommand.Parameters.Add("NOTIFY1_CUST_MST_FK_IN", NotifyPK).Direction = ParameterDirection.Input;
				} else {
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

				insCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
				insCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("CONFIG_PK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

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
				//'
				insCommand.Parameters.Add("CHK_CAN_IN", OracleDbType.Int32, 1, "chk_can").Direction = ParameterDirection.Input;
				insCommand.Parameters["CHK_CAN_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CHK_DO_IN", OracleDbType.Int32, 1, "chk_do").Direction = ParameterDirection.Input;
				insCommand.Parameters["CHK_DO_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CHK_PAY_IN", OracleDbType.Int32, 1, "chk_pay").Direction = ParameterDirection.Input;
				insCommand.Parameters["CHK_PAY_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CHK_REC_IN", OracleDbType.Int32, 1, "chk_rec").Direction = ParameterDirection.Input;
				insCommand.Parameters["CHK_REC_IN"].SourceVersion = DataRowVersion.Current;
				//'
				if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CRQ_Date"].ToString())) {
					insCommand.Parameters.Add("CRQ_DATE_IN", "").Direction = ParameterDirection.Input;
				} else {
					insCommand.Parameters.Add("CRQ_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["CRQ_Date"])).Direction = ParameterDirection.Input;
				}
				insCommand.Parameters["CRQ_DATE_IN"].SourceVersion = DataRowVersion.Current;
				insCommand.Parameters.Add("CRQ_IN", OracleDbType.Int32, 1, "CRQ").Direction = ParameterDirection.Input;

				if (PODetails.Length > 0) {
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

				} else {
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
				//-----------------------------------------------------------------------------------

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
				_with3.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_CARD_SEA_IMP_TBL_UPD";
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

				if (ConsigneePK > 0 & IsWINSave == 1) {
					updCommand.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", ConsigneePK).Direction = ParameterDirection.Input;
				} else {
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

				//updCommand.Parameters.Add("VOYAGE_FK_IN", OracleClient.OracleDbType.Varchar2, 10, "VoyagePK").Direction = ParameterDirection.Input
				updCommand.Parameters.Add("VOYAGE_FK_IN", getDefault(strVoyagepk, "")).Direction = ParameterDirection.Input;
				updCommand.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("ETA_DATE_IN", OracleDbType.Date, 20, "eta_date").Direction = ParameterDirection.Input;
				updCommand.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("ETD_DATE_IN", OracleDbType.Date, 20, "etd_date").Direction = ParameterDirection.Input;
				updCommand.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("ARRIVAL_DATE_IN", OracleDbType.Date, 20, "arrival_date").Direction = ParameterDirection.Input;
				updCommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("DEPARTURE_DATE_IN", OracleDbType.Date, 20, "departure_date").Direction = ParameterDirection.Input;
				updCommand.Parameters["DEPARTURE_DATE_IN"].SourceVersion = DataRowVersion.Current;

				if (ShipperPK > 0 & IsWINSave == 1) {
					updCommand.Parameters.Add("SHIPPER_CUST_MST_FK_IN", ShipperPK).Direction = ParameterDirection.Input;
				} else {
					updCommand.Parameters.Add("SHIPPER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "shipper_cust_mst_fk").Direction = ParameterDirection.Input;
					updCommand.Parameters["SHIPPER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (ConsigneePK > 0 & IsWINSave == 1) {
					updCommand.Parameters.Add("CONSIGNEE_CUST_MST_FK_IN", ConsigneePK).Direction = ParameterDirection.Input;
				} else {
					updCommand.Parameters.Add("CONSIGNEE_CUST_MST_FK_IN", OracleDbType.Int32, 10, "consignee_cust_mst_fk").Direction = ParameterDirection.Input;
					updCommand.Parameters["CONSIGNEE_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
				}

				if (NotifyPK > 0 & IsWINSave == 1) {
					updCommand.Parameters.Add("NOTIFY1_CUST_MST_FK_IN", NotifyPK).Direction = ParameterDirection.Input;
				} else {
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

				updCommand.Parameters.Add("CHA_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "cha_agent_mst_fk").Direction = ParameterDirection.Input;
				updCommand.Parameters["CHA_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

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
				if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["CRQ_Date"].ToString())) {
					updCommand.Parameters.Add("CRQ_DATE_IN", "").Direction = ParameterDirection.Input;
				} else {
					updCommand.Parameters.Add("CRQ_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["CRQ_Date"])).Direction = ParameterDirection.Input;
				}
				updCommand.Parameters["CRQ_DATE_IN"].SourceVersion = DataRowVersion.Current;
				updCommand.Parameters.Add("CRQ_IN", OracleDbType.Int32, 1, "CRQ").Direction = ParameterDirection.Input;

				if (PODetails.Length > 0) {
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
				} else {
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

				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					if (isEdting == false) {
						RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
						//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
					}
					return arrMessage;
				} else {
					if (isEdting == false) {
						JobCardPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
						HttpContext.Current.Session.Add("JobCardPK", JobCardPK);
					}
				}


				var _with6 = insContainerDetails;
				_with6.Connection = objWK.MyConnection;
				_with6.CommandType = CommandType.StoredProcedure;
				_with6.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_CONT_INS";
				var _with7 = _with6.Parameters;

				insContainerDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

				insContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 16, "container_number").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "seal_number").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("CONTAINER_OWNER_TYPE_FK_IN", OracleDbType.Int32, 10, "CONTAINER_OWNER_TYPE_FK").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["CONTAINER_OWNER_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "volume_in_cbm").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("NET_WEIGHT_IN", OracleDbType.Int32, 10, "net_weight").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "chargeable_weight").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "pack_type_mst_fk").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "pack_count").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "commodity_mst_fk").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
				//Added By Prakash chandra on 6/1/2009 for pts: multiple commodity selection 

				insContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Varchar2, 200, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("GEN_LAND_DATE_IN", OracleDbType.Date, 20, "gen_land_date").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["GEN_LAND_DATE_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("PREV_CONT_PK_IN", OracleDbType.Int32, 10, "prev_cont_pk").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["PREV_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_CARD_SEA_IMP_CONT_PK").Direction = ParameterDirection.Output;
				insContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;




				var _with8 = updContainerDetails;
				_with8.Connection = objWK.MyConnection;
				_with8.CommandType = CommandType.StoredProcedure;
				_with8.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_CONT_UPD";
				var _with9 = _with8.Parameters;

				updContainerDetails.Parameters.Add("JOB_TRN_SEA_IMP_CONT_PK_IN", OracleDbType.Int32, 10, "job_card_sea_imp_cont_pk").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["JOB_TRN_SEA_IMP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

				updContainerDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

				updContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 16, "container_number").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

				updContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "seal_number").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

				updContainerDetails.Parameters.Add("CONTAINER_OWNER_TYPE_FK_IN", OracleDbType.Int32, 10, "CONTAINER_OWNER_TYPE_FK").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["CONTAINER_OWNER_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

				updContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", OracleDbType.Int32, 10, "volume_in_cbm").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

				updContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", OracleDbType.Int32, 10, "gross_weight").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

				updContainerDetails.Parameters.Add("NET_WEIGHT_IN", OracleDbType.Int32, 10, "net_weight").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

				updContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", OracleDbType.Int32, 10, "chargeable_weight").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

				updContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "pack_type_mst_fk").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updContainerDetails.Parameters.Add("PACK_COUNT_IN", OracleDbType.Int32, 6, "pack_count").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

				updContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "commodity_mst_fk").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
				//Added By Prakash chandra on 6/1/2009 for pts: multiple commodity selection 

				updContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Varchar2, 200, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;

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

				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					if (isEdting == false) {
						RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
						//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
					}
					return arrMessage;
				}
				objWK.MyCommand.Transaction = TRAN;
				// Amit 22-Dec-06 TaskID "DOC-DEC-001"
				int rowCnt = 0;
				if (dsContainerData.Tables[0].Rows.Count > 0 & dsContainerData.Tables[0].Columns.Contains("strSpclReq")) {
					try {
						for (rowCnt = 0; rowCnt <= dsContainerData.Tables[0].Rows.Count - 1; rowCnt++) {
							int CntType = 0;
							CntType = Convert.ToInt32(((System.Data.DataRow)dsContainerData.Tables[0].Rows[rowCnt]).ItemArray[2]);
							string strSql = null;
							string drCntKind = null;
							strSql = "SELECT C.CONTAINER_KIND FROM CONTAINER_TYPE_MST_TBL C WHERE C.CONTAINER_TYPE_MST_PK= " + CntType + "";

							var _with11 = objWK.MyCommand;
							_with11.Parameters.Clear();
							_with11.CommandType = CommandType.Text;
							_with11.CommandText = strSql;
							drCntKind = Convert.ToString(_with11.ExecuteScalar());
							objWK.MyCommand.Parameters.Clear();
							if (CommodityGroup == "HAZARDOUS") {
								if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5") {
									arrMessage = SaveTransactionODC(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
								} else {
									arrMessage = SaveTransactionHZSpcl(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
								}

							} else if (CommodityGroup == "REEFER") {
								if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5") {
									arrMessage = SaveTransactionODC(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
								} else {
									arrMessage = SaveTransactionReefer(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
								}

							} else if (CommodityGroup == "ODC") {
								if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5") {
                                    arrMessage = SaveTransactionReefer(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
                                }
							} else {
								if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5") {
                                    arrMessage = SaveTransactionReefer(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsContainerData.Tables[0].Rows[rowCnt]["strSpclReq"], "")), Convert.ToInt64(dsContainerData.Tables[0].Rows[rowCnt][0]));
                                }
							}
						}
					} catch (Exception ex) {
					}
				}
				// End

				if ((dsTPDetails != null)) {
					if (dsTPDetails.Tables.Count > 0) {
						var _with12 = insTPDetails;
						_with12.Connection = objWK.MyConnection;
						_with12.CommandType = CommandType.StoredProcedure;
						_with12.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_TP_INS";
						var _with13 = _with12.Parameters;

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


						var _with14 = updTPDetails;
						_with14.Connection = objWK.MyConnection;
						_with14.CommandType = CommandType.StoredProcedure;
						_with14.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_TP_UPD";
						var _with15 = _with14.Parameters;

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


						var _with16 = delTPDetails;
						_with16.Connection = objWK.MyConnection;
						_with16.CommandType = CommandType.StoredProcedure;
						_with16.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_TP_DEL";

						delTPDetails.Parameters.Add("JOB_TRN_SEA_IMP_TP_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_tp_pk").Direction = ParameterDirection.Input;
						delTPDetails.Parameters["JOB_TRN_SEA_IMP_TP_PK_IN"].SourceVersion = DataRowVersion.Current;

						delTPDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
						delTPDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


						var _with17 = objWK.MyDataAdapter;

						_with17.InsertCommand = insTPDetails;
						_with17.InsertCommand.Transaction = TRAN;

						_with17.UpdateCommand = updTPDetails;
						_with17.UpdateCommand.Transaction = TRAN;

						_with17.DeleteCommand = delTPDetails;
						_with17.DeleteCommand.Transaction = TRAN;
						RecAfct = _with17.Update(dsTPDetails.Tables[0]);

						if (arrMessage.Count >= 1) {
							if (string.Compare(Convert.ToString(arrMessage[0]).ToLower(), "saved") > 0) {
								arrMessage.Clear();
							}
						}

						if (arrMessage.Count > 0) {
							TRAN.Rollback();
							if (isEdting == false) {
								RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);

								//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
							}
							return arrMessage;
						}
					}
				}
				//Manjunath for Cargo Pick up & Drop Address
				if ((dsPickUpDetails != null)) {
					if (dsPickUpDetails.Tables.Count > 0) {
						var _with18 = insPickUpDetails;
						_with18.Connection = objWK.MyConnection;
						_with18.CommandType = CommandType.StoredProcedure;
						_with18.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_INS";

						_with18.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

						_with18.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
						_with18.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
						_with18.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
						_with18.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
						_with18.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
						_with18.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
						_with18.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
						_with18.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
						_with18.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
						_with18.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
						_with18.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
						_with18.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
						_with18.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
						_with18.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
						_with18.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
						_with18.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
						_with18.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
						_with18.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

						_with18.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

						_with18.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Output;
						_with18.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


						var _with19 = updPickUpDetails;
						_with19.Connection = objWK.MyConnection;
						_with19.CommandType = CommandType.StoredProcedure;
						_with19.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_UPD";

						_with19.Parameters.Add("PICK_DROP_MST_PK_IN", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Input;
						_with19.Parameters["PICK_DROP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

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

						_with19.Parameters.Add("MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

						_with19.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
						_with19.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

						var _with20 = objWK.MyDataAdapter;

						_with20.InsertCommand = insPickUpDetails;
						_with20.InsertCommand.Transaction = TRAN;

						_with20.UpdateCommand = updPickUpDetails;
						_with20.UpdateCommand.Transaction = TRAN;

						RecAfct = _with20.Update(dsPickUpDetails);

						if (arrMessage.Count > 0) {
							TRAN.Rollback();
							return arrMessage;
						}
					}
				}

				if ((dsDropDetails != null)) {
					if (dsDropDetails.Tables.Count > 0) {
						var _with21 = insDropDetails;
						_with21.Connection = objWK.MyConnection;
						_with21.CommandType = CommandType.StoredProcedure;
						_with21.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_INS";

						_with21.Parameters.Add("JOB_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

						_with21.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32, 1, "BIZ_TYPE").Direction = ParameterDirection.Input;
						_with21.Parameters["BIZ_TYPE_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Int32, 1, "PROCESS_TYPE").Direction = ParameterDirection.Input;
						_with21.Parameters["PROCESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("PICKUP_DROP_TYPE_IN", OracleDbType.Int32, 1, "PICKUP_DROP_TYPE").Direction = ParameterDirection.Input;
						_with21.Parameters["PICKUP_DROP_TYPE_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("PICK_DROP_NAME1_IN", OracleDbType.Varchar2, 50, "COMP_NAME").Direction = ParameterDirection.Input;
						_with21.Parameters["PICK_DROP_NAME1_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("PICK_DROP_NAME2_IN", OracleDbType.Varchar2, 50, "COMP_NAME1").Direction = ParameterDirection.Input;
						_with21.Parameters["PICK_DROP_NAME2_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("PICK_DROP_ADDRESS1_IN", OracleDbType.Varchar2, 50, "ADDRESS1").Direction = ParameterDirection.Input;
						_with21.Parameters["PICK_DROP_ADDRESS1_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("PICK_DROP_ADDRESS2_IN", OracleDbType.Varchar2, 50, "ADDRESS2").Direction = ParameterDirection.Input;
						_with21.Parameters["PICK_DROP_ADDRESS2_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("PICK_DROP_ADDRESS3_IN", OracleDbType.Varchar2, 50, "ADDRESS3").Direction = ParameterDirection.Input;
						_with21.Parameters["PICK_DROP_ADDRESS3_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("STATE_IN", OracleDbType.Varchar2, 50, "STATE").Direction = ParameterDirection.Input;
						_with21.Parameters["STATE_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("CITY_IN", OracleDbType.Varchar2, 30, "CITY").Direction = ParameterDirection.Input;
						_with21.Parameters["CITY_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("POST_CODE_IN", OracleDbType.Varchar2, 10, "POST_CODE").Direction = ParameterDirection.Input;
						_with21.Parameters["POST_CODE_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("COUNTRY_MST_PK_IN", OracleDbType.Int32, 10, "COUNTRY_MST_PK").Direction = ParameterDirection.Input;
						_with21.Parameters["COUNTRY_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("CONT_PERSON_IN", OracleDbType.Varchar2, 50, "CONT_PERSON").Direction = ParameterDirection.Input;
						_with21.Parameters["CONT_PERSON_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("PHONE_NR_IN", OracleDbType.Varchar2, 25, "PHONE_NR").Direction = ParameterDirection.Input;
						_with21.Parameters["PHONE_NR_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("FAX_NR_IN", OracleDbType.Varchar2, 25, "FAX_NR").Direction = ParameterDirection.Input;
						_with21.Parameters["FAX_NR_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("CELL_NR_IN", OracleDbType.Varchar2, 25, "CELL_NR").Direction = ParameterDirection.Input;
						_with21.Parameters["CELL_NR_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("EMAIL_IN", OracleDbType.Varchar2, 100, "EMAIL").Direction = ParameterDirection.Input;
						_with21.Parameters["EMAIL_IN"].SourceVersion = DataRowVersion.Current;

						_with21.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

						_with21.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Output;
						_with21.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


						var _with22 = updDropDetails;
						_with22.Connection = objWK.MyConnection;
						_with22.CommandType = CommandType.StoredProcedure;
						_with22.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_PICKUP_TRN_UPD";

						_with22.Parameters.Add("PICK_DROP_MST_PK_IN", OracleDbType.Int32, 10, "PICK_DROP_MST_PK").Direction = ParameterDirection.Input;
						_with22.Parameters["PICK_DROP_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

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

						_with22.Parameters.Add("MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

						_with22.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
						_with22.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

						var _with23 = objWK.MyDataAdapter;

						_with23.InsertCommand = insDropDetails;
						_with23.InsertCommand.Transaction = TRAN;

						_with23.UpdateCommand = updDropDetails;
						_with23.UpdateCommand.Transaction = TRAN;

						RecAfct = _with23.Update(dsDropDetails);

						if (arrMessage.Count > 0) {
							TRAN.Rollback();
							return arrMessage;
						}
					}
				}
				//End Manjunath
				if ((dsFreightDetails != null)) {
					var _with24 = insFreightDetails;
					_with24.Connection = objWK.MyConnection;
					_with24.CommandType = CommandType.StoredProcedure;
					_with24.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_INS";
					var _with25 = _with24.Parameters;

					insFreightDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

					insFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
					insFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

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

					insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 100, "roe").Direction = ParameterDirection.Input;
					insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;
					//'SURCHARGE
					insFreightDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 50, "SURCHARGE").Direction = ParameterDirection.Input;
					insFreightDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;
					//'SURCHARGE

					insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_IMP_FD_PK").Direction = ParameterDirection.Output;
					insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;




					var _with26 = updFreightDetails;
					_with26.Connection = objWK.MyConnection;
					_with26.CommandType = CommandType.StoredProcedure;
					_with26.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_UPD";
					var _with27 = _with26.Parameters;

					updFreightDetails.Parameters.Add("JOB_TRN_SEA_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_fd_pk").Direction = ParameterDirection.Input;
					updFreightDetails.Parameters["JOB_TRN_SEA_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

					updFreightDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

					updFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
					updFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

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

					//'SURCHARGE
					updFreightDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
					updFreightDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;
					//'SURCHARGE

					updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
					updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

					var _with28 = delFreightDetails;
					_with28.Connection = objWK.MyConnection;
					_with28.CommandType = CommandType.StoredProcedure;
					_with28.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_DEL";

					delFreightDetails.Parameters.Add("JOB_TRN_SEA_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_fd_pk").Direction = ParameterDirection.Input;
					delFreightDetails.Parameters["JOB_TRN_SEA_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

					delFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
					delFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

					var _with29 = objWK.MyDataAdapter;

					_with29.InsertCommand = insFreightDetails;
					_with29.InsertCommand.Transaction = TRAN;

					_with29.UpdateCommand = updFreightDetails;
					_with29.UpdateCommand.Transaction = TRAN;

					_with29.DeleteCommand = delFreightDetails;
					_with29.DeleteCommand.Transaction = TRAN;

					//RecAfct = .Update(dsFreightDetails)
					RecAfct = _with29.Update(dsFreightDetails.Tables[0]);

					if (arrMessage.Count > 0) {
						TRAN.Rollback();
						if (isEdting == false) {
							RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
							//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
						}
						return arrMessage;
					}
				}

				if ((dsPurchaseInventory != null)) {
					var _with30 = insPurchaseInvDetails;
					_with30.Connection = objWK.MyConnection;
					_with30.CommandType = CommandType.StoredProcedure;
					_with30.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_PIA_INS";
					var _with31 = _with30.Parameters;

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


					var _with32 = updPurchaseInvDetails;
					_with32.Connection = objWK.MyConnection;
					_with32.CommandType = CommandType.StoredProcedure;
					_with32.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_PIA_UPD";
					var _with33 = _with32.Parameters;

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

					var _with34 = delPurchaseInvDetails;
					_with34.Connection = objWK.MyConnection;
					_with34.CommandType = CommandType.StoredProcedure;
					_with34.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_PIA_DEL";

					delPurchaseInvDetails.Parameters.Add("JOB_TRN_SEA_IMP_PIA_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_pia_pk").Direction = ParameterDirection.Input;
					delPurchaseInvDetails.Parameters["JOB_TRN_SEA_IMP_PIA_PK_IN"].SourceVersion = DataRowVersion.Current;

					delPurchaseInvDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
					delPurchaseInvDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


					var _with35 = objWK.MyDataAdapter;

					_with35.InsertCommand = insPurchaseInvDetails;
					_with35.InsertCommand.Transaction = TRAN;

					_with35.UpdateCommand = updPurchaseInvDetails;
					_with35.UpdateCommand.Transaction = TRAN;

					_with35.DeleteCommand = delPurchaseInvDetails;
					_with35.DeleteCommand.Transaction = TRAN;

					RecAfct = _with35.Update(dsPurchaseInventory.Tables[0]);

					if (arrMessage.Count > 0) {
						TRAN.Rollback();
						if (isEdting == false) {
                            RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                        }
						return arrMessage;
					}
				}
				//'Added By Koteshwari on 25/4/2011
				if ((dsCostDetails != null)) {
					var _with36 = insCostDetails;
					_with36.Connection = objWK.MyConnection;
					_with36.CommandType = CommandType.StoredProcedure;
					_with36.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_INS";
					var _with37 = _with36.Parameters;
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

					var _with38 = updCostDetails;
					_with38.Connection = objWK.MyConnection;
					_with38.CommandType = CommandType.StoredProcedure;
					_with38.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_UPD";
					var _with39 = _with38.Parameters;

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

					var _with40 = delCostDetails;
					_with40.Connection = objWK.MyConnection;
					_with40.CommandType = CommandType.StoredProcedure;
					_with40.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_DEL";

					delCostDetails.Parameters.Add("JOB_TRN_IMP_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_SEA_IMP_COST_PK").Direction = ParameterDirection.Input;
					delCostDetails.Parameters["JOB_TRN_IMP_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

					delCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "CLng(RETURN_VALUE)").Direction = ParameterDirection.Output;
					delCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

					var _with41 = objWK.MyDataAdapter;

					_with41.InsertCommand = insCostDetails;
					_with41.InsertCommand.Transaction = TRAN;

					_with41.UpdateCommand = updCostDetails;
					_with41.UpdateCommand.Transaction = TRAN;

					_with41.DeleteCommand = delCostDetails;
					_with41.DeleteCommand.Transaction = TRAN;

					RecAfct = _with41.Update(dsCostDetails.Tables[0]);

					if (arrMessage.Count > 0) {
						TRAN.Rollback();
						return arrMessage;
					}
				}
				//'End Koteshwari
				if ((dsIncomeChargeDetails != null) & (dsExpenseChargeDetails != null)) {
					if (!SaveSecondaryServices(objWK, TRAN, Convert.ToInt32(JobCardPK), dsIncomeChargeDetails, dsExpenseChargeDetails)) {
						arrMessage.Add("Error while saving secondary service details");
						return arrMessage;
					}
				}

				if ((dsOtherCharges != null)) {
					var _with42 = insOtherChargesDetails;
					_with42.Connection = objWK.MyConnection;
					_with42.CommandType = CommandType.StoredProcedure;
					_with42.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_OTH_CHRG_INS";
					var _with43 = _with42.Parameters;

					insOtherChargesDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

					insOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
					insOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

					insOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
					insOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
					//latha

					insOtherChargesDetails.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "PaymentType").Direction = ParameterDirection.Input;
					insOtherChargesDetails.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;


					insOtherChargesDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
					insOtherChargesDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

					insOtherChargesDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "Frtpayer_mst_fk").Direction = ParameterDirection.Input;
					insOtherChargesDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
					//end

					insOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
					insOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

					insOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
					insOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

					insOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "job_trn_sea_imp_oth_pk").Direction = ParameterDirection.Output;
					insOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;




					var _with44 = updOtherChargesDetails;
					_with44.Connection = objWK.MyConnection;
					_with44.CommandType = CommandType.StoredProcedure;
					_with44.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_OTH_CHRG_UPD";
					var _with45 = _with44.Parameters;

					updOtherChargesDetails.Parameters.Add("JOB_TRN_SEA_IMP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_oth_pk").Direction = ParameterDirection.Input;
					updOtherChargesDetails.Parameters["JOB_TRN_SEA_IMP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

					updOtherChargesDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

					updOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
					updOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

					updOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
					updOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
					//latha

					updOtherChargesDetails.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "PaymentType").Direction = ParameterDirection.Input;
					updOtherChargesDetails.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

					updOtherChargesDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
					updOtherChargesDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

					updOtherChargesDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_mst_fk").Direction = ParameterDirection.Input;
					updOtherChargesDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
					//end

					updOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
					updOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

					updOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
					updOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

					updOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
					updOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

					var _with46 = delOtherChargesDetails;
					_with46.Connection = objWK.MyConnection;
					_with46.CommandType = CommandType.StoredProcedure;
					_with46.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_OTH_CHRG_DEL";

					delOtherChargesDetails.Parameters.Add("JOB_TRN_SEA_IMP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_oth_pk").Direction = ParameterDirection.Input;
					delOtherChargesDetails.Parameters["JOB_TRN_SEA_IMP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

					delOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
					delOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

					var _with47 = objWK.MyDataAdapter;

					_with47.InsertCommand = insOtherChargesDetails;
					_with47.InsertCommand.Transaction = TRAN;

					_with47.UpdateCommand = updOtherChargesDetails;
					_with47.UpdateCommand.Transaction = TRAN;

					_with47.DeleteCommand = delOtherChargesDetails;
					_with47.DeleteCommand.Transaction = TRAN;

					RecAfct = _with47.Update(dsOtherCharges);

				}
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					if (isEdting == false) {
						RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
						//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
					}
					return arrMessage;
				} else {
					cls_JobCardView objJCFclLcl = new cls_JobCardView();
					objJCFclLcl.CREATED_BY = Convert.ToInt64(M_CREATED_BY_FK);
					objJCFclLcl.LAST_MODIFIED_BY = Convert.ToInt64(M_CREATED_BY_FK);
					objJCFclLcl.ConfigurationPK = Convert.ToInt64(M_Configuration_PK);
					arrMessage = (ArrayList)objJCFclLcl.SaveJobCardDoc(Convert.ToString(JobCardPK), TRAN, dsDoc, 2, 2);
					//Biztype -2(Sea),Process Type -2(Import)
					if (arrMessage.Count > 0) {
						if (!(string.Compare(arrMessage[0].ToString(), "Saved") > 0)) {
							TRAN.Rollback();
							if (isEdting == false) {
								RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
								//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
							}
							return arrMessage;
						}
					}
					arrMessage = (ArrayList)SaveTrackAndTrace(Convert.ToInt32(JobCardPK), TRAN, dsTrackNtrace, Convert.ToInt32(userLocation), isEdting, M_DataSet);
					//'If arrMessage.Count > 0 Then
					if (string.Compare(arrMessage[0].ToString(), "Saved") > 0) {
						TRAN.Commit();
						//Push to financial system if realtime is selected
						if (JobCardPK > 0) {
							cls_Scheduler objSch = new cls_Scheduler();
							ArrayList schDtls = null;
							bool errGen = false;
							if (objSch.GetSchedulerPushType() == true) {
								//QFSIService.serFinApp objPush = new QFSIService.serFinApp();
								//try {
								//	schDtls = objSch.FetchSchDtls();
								//	//'Used to Fetch the Sch Dtls
								//	objPush.UpdateCostCentre(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, schDtls[5].ToString().ToUpper(), schDtls[0].ToString().ToUpper(),"" ,"" , JobCardPK);
								//	if (ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
								//		objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
								//	}
								//} catch (Exception ex) {
								//	if (ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
								//		objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
								//	}
								//}
							}
						}
						//*****************************************************************
						return arrMessage;
					} else {
						TRAN.Rollback();
						if (isEdting == false) {
							RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
							//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
						}
					}
					return arrMessage;
				}

			} catch (OracleException oraexp) {
				if (isEdting == false) {
					RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
					//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
				}
				throw oraexp;
			} catch (Exception ex) {
				if (isEdting == false) {
                    RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
        /// <summary>
        /// Saves the transaction odc.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="strSpclRequest">The string SPCL request.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        private ArrayList SaveTransactionODC(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
		{
			if (!string.IsNullOrEmpty(strSpclRequest)) {
				arrMessage.Clear();
				string[] strParam = null;
				int Process = 0;
				int BizTYpe = 0;
				Process = 2;
				BizTYpe = 2;
				strParam = strSpclRequest.Split('~');
				try {
					var _with48 = SCM;
					_with48.CommandType = CommandType.StoredProcedure;
					_with48.CommandText = UserName + ".JOB_CARD_SPCL_REQ_PKG.JOB_TRN_SEA_ODC_SPL_REQ_INS";
					var _with49 = _with48.Parameters;
					_with49.Clear();
					//BKG_TRN_SEA_FK_IN()
					_with49.Add("JOB_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
					//LENGTH_IN()
					_with49.Add("LENGTH_IN", getDefault(strParam[0], "")).Direction = ParameterDirection.Input;
					//LENGTH_UOM_MST_FK_IN()
					_with49.Add("LENGTH_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
					//HEIGHT_IN()
					_with49.Add("HEIGHT_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
					//HEIGHT_UOM_MST_FK_IN()
					_with49.Add("HEIGHT_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
					//WIDTH_IN()
					_with49.Add("WIDTH_IN", getDefault(strParam[1], 0)).Direction = ParameterDirection.Input;
					//WIDTH_UOM_MST_FK_IN()
					_with49.Add("WIDTH_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
					//WEIGHT_IN()
					_with49.Add("WEIGHT_IN", getDefault(strParam[3], "")).Direction = ParameterDirection.Input;
					//WEIGHT_UOM_MST_FK_IN()
					_with49.Add("WEIGHT_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
					//VOLUME_IN()
					_with49.Add("VOLUME_IN", "").Direction = ParameterDirection.Input;
					//VOLUME_UOM_MST_FK_IN()
					_with49.Add("VOLUME_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
					//SLOT_LOSS_IN()
					_with49.Add("SLOT_LOSS_IN", "").Direction = ParameterDirection.Input;
					//LOSS_QUANTITY_IN()
					_with49.Add("LOSS_QUANTITY_IN", "").Direction = ParameterDirection.Input;
					//APPR_REQ_IN()
					_with49.Add("APPR_REQ_IN", "").Direction = ParameterDirection.Input;
					if (Convert.ToBoolean(strParam[4]) == true) {
						_with49.Add("STOWAGE_IN", 1).Direction = ParameterDirection.Input;
					} else {
						_with49.Add("STOWAGE_IN", 2).Direction = ParameterDirection.Input;
					}
					_with49.Add("HAND_INST_IN", (string.IsNullOrEmpty(strParam[6]) ? "" : strParam[6])).Direction = ParameterDirection.Input;
					_with49.Add("LASH_INST_IN", (string.IsNullOrEmpty(strParam[7]) ? "" : strParam[7])).Direction = ParameterDirection.Input;
					_with49.Add("PROCESS_TYPE_IN", Process).Direction = ParameterDirection.Input;
					_with49.Add("BIZ_TYPE_IN", BizTYpe).Direction = ParameterDirection.Input;
					//RETURN_VALUE()
					_with49.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
					_with48.ExecuteNonQuery();
					arrMessage.Add("All data saved successfully");
					return arrMessage;
				} catch (OracleException oraexp) {
					arrMessage.Add(oraexp.Message);
					return arrMessage;
				} catch (Exception ex) {
					arrMessage.Add(ex.Message);
					return arrMessage;
				}
			}
			arrMessage.Add("All data saved successfully");
			return arrMessage;
		}
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
			try {
				if (!string.IsNullOrEmpty(strSpclRequest)) {
					arrMessage.Clear();
					string[] strParam = null;
					int Process = 0;
					int BizTYpe = 0;
					Process = 2;
					BizTYpe = 2;
					strParam = strSpclRequest.Split('~');
					try {
						var _with50 = SCM;
						_with50.CommandType = CommandType.StoredProcedure;
						_with50.CommandText = UserName + ".JOB_CARD_SPCL_REQ_PKG.JOB_TRN_SEA_HAZ_SPL_REQ_INS";
						var _with51 = _with50.Parameters;
						_with51.Clear();
						//BKG_TRN_SEA_FK_IN()
						_with51.Add("JOB_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
						//OUTER_PACK_TYPE_MST_FK_IN()
						_with51.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], "")).Direction = ParameterDirection.Input;
						//INNER_PACK_TYPE_MST_FK_IN()
						_with51.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], "")).Direction = ParameterDirection.Input;
						//MIN_TEMP_IN()
						_with51.Add("MIN_TEMP_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
						//MIN_TEMP_UOM_IN()
						_with51.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? "" : strParam[3]), 0)).Direction = ParameterDirection.Input;
						//MAX_TEMP_IN()
						_with51.Add("MAX_TEMP_IN", getDefault(strParam[4], "")).Direction = ParameterDirection.Input;
						//MAX_TEMP_UOM_IN()
						_with51.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "" : strParam[5]), 0)).Direction = ParameterDirection.Input;
						//FLASH_PNT_TEMP_IN()
						_with51.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
						//FLASH_PNT_TEMP_UOM_IN()
						_with51.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "" : strParam[7]), 0)).Direction = ParameterDirection.Input;
						//IMDG_CLASS_CODE_IN()
						_with51.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
						//UN_NO_IN()
						_with51.Add("UN_NO_IN", getDefault(strParam[9], "")).Direction = ParameterDirection.Input;
						//IMO_SURCHARGE_IN()
						_with51.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
						//SURCHARGE_AMT_IN()
						_with51.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
						//IS_MARINE_POLLUTANT_IN()
						_with51.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
						//EMS_NUMBER_IN()
						_with51.Add("EMS_NUMBER_IN", getDefault(strParam[13], "")).Direction = ParameterDirection.Input;
						//RETURN_VALUE()
						_with51.Add("PROPER_SHIPPING_NAME_IN", getDefault(strParam[14], "")).Direction = ParameterDirection.Input;
						_with51.Add("PACK_CLASS_TYPE_IN", getDefault(strParam[15], "")).Direction = ParameterDirection.Input;
						_with51.Add("PROCESS_TYPE_IN", Process).Direction = ParameterDirection.Input;
						_with51.Add("BIZ_TYPE_IN", BizTYpe).Direction = ParameterDirection.Input;
						_with51.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
						_with50.ExecuteNonQuery();
						arrMessage.Add("All data saved successfully");
						return arrMessage;
					} catch (OracleException oraexp) {
						arrMessage.Add(oraexp.Message);
						return arrMessage;
					} catch (Exception ex) {
						arrMessage.Add(ex.Message);
						return arrMessage;
					}
				}
				arrMessage.Add("All data saved successfully");
				return arrMessage;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
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

			if (!string.IsNullOrEmpty(strSpclRequest)) {
				arrMessage.Clear();
				string[] strParam = null;
				int Process = 0;
				int BizTYpe = 0;
				Process = 2;
				BizTYpe = 2;
				strParam = strSpclRequest.Split('~');
				try {
					var _with52 = SCM;
					_with52.CommandType = CommandType.StoredProcedure;
					_with52.CommandText = UserName + ".JOB_CARD_SPCL_REQ_PKG.JOB_TRN_SEA_REF_SPL_REQ_INS";
					var _with53 = _with52.Parameters;
					_with53.Clear();
					//BOOKING_TRN_SEA_FK_IN()
					_with53.Add("JOB_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
					//VENTILATION_IN()
					_with53.Add("VENTILATION_IN", strParam[0]).Direction = ParameterDirection.Input;
					//AIR_COOL_METHOD_IN()
					_with53.Add("AIR_COOL_METHOD_IN", strParam[1]).Direction = ParameterDirection.Input;
					//HUMIDITY_FACTOR_IN()
					_with53.Add("HUMIDITY_FACTOR_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
					//IS_PERSHIABLE_GOODS_IN()
					_with53.Add("IS_PERSHIABLE_GOODS_IN", strParam[3]).Direction = ParameterDirection.Input;
					//MIN_TEMP_IN()
					_with53.Add("MIN_TEMP_IN", getDefault(strParam[4], "")).Direction = ParameterDirection.Input;
					//MIN_TEMP_UOM_IN()
					_with53.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "": strParam[5]), 0)).Direction = ParameterDirection.Input;
					//MAX_TEMP_IN()
					_with53.Add("MAX_TEMP_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
					//MAX_TEMP_UOM_IN()
					_with53.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "" : strParam[7]), 0)).Direction = ParameterDirection.Input;
					//PACK_TYPE_MST_FK_IN()
					_with53.Add("PACK_TYPE_MST_FK_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
					//PACK_COUNT_IN()
					_with53.Add("PACK_COUNT_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
					_with53.Add("REF_VENTILATION_IN", getDefault(strParam[10], "")).Direction = ParameterDirection.Input;
					//'BIZ and Process Type
					_with53.Add("PROCESS_TYPE_IN", Process).Direction = ParameterDirection.Input;
					_with53.Add("BIZ_TYPE_IN", BizTYpe).Direction = ParameterDirection.Input;

					//RETURN_VALUE()
					_with53.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
					_with52.ExecuteNonQuery();
					arrMessage.Add("All data saved successfully");
					return arrMessage;
				} catch (OracleException oraexp) {
					arrMessage.Add(oraexp.Message);
					return arrMessage;
				} catch (Exception ex) {
					arrMessage.Add(ex.Message);
					return arrMessage;
				}
			}
			arrMessage.Add("All data saved successfully");
			return arrMessage;
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
			if ((dsIncomeChargeDetails != null)) {
				//----------------------------------Income Charge Details----------------------------------
				foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows) {
					int Frt_Pk = 0;
					try {
						Frt_Pk = Convert.ToInt32(ri["JOB_TRN_SEA_IMP_FD_PK"]);
					} catch (Exception ex) {
						Frt_Pk = 0;
					}
					var _with54 = objWK.MyCommand;
					_with54.Parameters.Clear();
					_with54.Transaction = TRAN;
					_with54.CommandType = CommandType.StoredProcedure;
					if (Frt_Pk > 0) {
						_with54.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_UPD";
						_with54.Parameters.Add("JOB_TRN_SEA_IMP_FD_PK_IN", ri["JOB_TRN_SEA_IMP_FD_PK"]).Direction = ParameterDirection.Input;
					} else {
						_with54.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_INS";
						_with54.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
					}
					_with54.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
					_with54.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", "").Direction = ParameterDirection.Input;
					_with54.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
					_with54.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
					_with54.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
					_with54.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
					_with54.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
					_with54.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
					_with54.Parameters.Add("BASIS_IN", getDefault(ri["BASIS"], "")).Direction = ParameterDirection.Input;
					_with54.Parameters.Add("BASIS_FK_IN", getDefault(ri["BASIS_PK"], "")).Direction = ParameterDirection.Input;
					_with54.Parameters.Add("EXCHANGE_RATE_IN", getDefault(ri["ROE"], 1)).Direction = ParameterDirection.Input;
					_with54.Parameters.Add("RATE_PERBASIS_IN", getDefault(ri["RATEPERBASIS"], "")).Direction = ParameterDirection.Input;
					_with54.Parameters.Add("SURCHARGE_IN", "").Direction = ParameterDirection.Input;
					_with54.Parameters.Add("QUANTITY_IN", getDefault(ri["VOLUME"], "")).Direction = ParameterDirection.Input;
					_with54.Parameters.Add("SERVICE_MST_FK_IN", ri["SERVICE_MST_PK"]).Direction = ParameterDirection.Input;
					_with54.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
					try {
						_with54.ExecuteNonQuery();
						if (Frt_Pk == 0) {
							_with54.Parameters.Clear();
							_with54.CommandType = CommandType.StoredProcedure;
							_with54.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_FRT_SEQ_CURRVAL";
							_with54.Parameters.Add("BIZ_IN", 2).Direction = ParameterDirection.Input;
							_with54.Parameters.Add("PROCESS_IN", 2).Direction = ParameterDirection.Input;
							_with54.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
							_with54.ExecuteNonQuery();
							Frt_Pk = Convert.ToInt32(_with54.Parameters["RETURN_VALUE"].Value);
							ri["JOB_TRN_SEA_IMP_FD_PK"] = Frt_Pk;
						}
					} catch (Exception ex) {
						return false;
					}
				}
			}
			//----------------------------------Expense Charge Details----------------------------------
			if ((dsExpenseChargeDetails != null)) {
				foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows) {
					int Cost_Pk = 0;
					try {
						Cost_Pk = Convert.ToInt32(re["JOB_TRN_SEA_IMP_COST_PK"]);
					} catch (Exception ex) {
						Cost_Pk = 0;
					}
					var _with55 = objWK.MyCommand;
					_with55.Parameters.Clear();
					_with55.Transaction = TRAN;
					_with55.CommandType = CommandType.StoredProcedure;
					if (Cost_Pk > 0) {
						_with55.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_UPD";
						_with55.Parameters.Add("JOB_TRN_IMP_EST_PK_IN", re["JOB_TRN_SEA_IMP_COST_PK"]).Direction = ParameterDirection.Input;
					} else {
						_with55.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_INS";
						_with55.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
					}
					_with55.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("VENDOR_MST_FK_IN", re["SUPPLIER_MST_PK"]).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("COST_ELEMENT_FK_IN", re["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("LOCATION_FK_IN", re["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("VENDOR_KEY_IN", re["SUPPLIER_MST_ID"]).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("PTMT_TYPE_IN", re["PTMT_TYPE"]).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("CURRENCY_MST_FK_IN", re["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("ESTIMATED_COST_IN", re["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("TOTAL_COST_IN", re["TOTAL_COST"]).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("BASIS_FK_IN", re["DD_VALUE"]).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("RATEPERBASIS_IN", re["RATEPERBASIS"]).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("QUANTITY_IN", getDefault(re["VOLUME"], "")).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("EXCHANGE_RATE_IN", getDefault(re["ROE"], 1)).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("EXT_INT_FLAG_IN", getDefault(re["EXT_INT_FLAG"], 2)).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("SERVICE_MST_FK_IN", re["SERVICE_MST_FK"]).Direction = ParameterDirection.Input;
					_with55.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
					try {
						_with55.ExecuteNonQuery();
						if (Cost_Pk == 0) {
							_with55.Parameters.Clear();
							_with55.CommandType = CommandType.StoredProcedure;
							_with55.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_COST_SEQ_CURRVAL";
							_with55.Parameters.Add("BIZ_IN", 2).Direction = ParameterDirection.Input;
							_with55.Parameters.Add("PROCESS_IN", 2).Direction = ParameterDirection.Input;
							_with55.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
							_with55.ExecuteNonQuery();
							Cost_Pk = Convert.ToInt32(_with55.Parameters["RETURN_VALUE"].Value);
							re["JOB_TRN_SEA_IMP_COST_PK"] = Cost_Pk;
						}
					} catch (Exception ex) {
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
			if (JobCardPK > 0) {
				//objwk.OpenConnection()
				//Dim TRAN As OracleTransaction
				//TRAN = objwk.MyConnection.BeginTransaction()
				try {
					if ((dsIncomeChargeDetails != null)) {
						foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows) {
							if (string.IsNullOrEmpty(SelectedFrtPks)) {
								SelectedFrtPks = Convert.ToString(ri["JOB_TRN_SEA_IMP_FD_PK"]);
							} else {
								SelectedFrtPks += "," + ri["JOB_TRN_SEA_IMP_FD_PK"];
							}
						}
						foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows) {
							if (string.IsNullOrEmpty(SelectedCostPks)) {
								SelectedCostPks = Convert.ToString(re["JOB_TRN_SEA_IMP_COST_PK"]);
							} else {
								SelectedCostPks += "," + re["JOB_TRN_SEA_IMP_COST_PK"];
							}
						}

						var _with56 = objWK.MyCommand;
						_with56.Transaction = TRAN;
						_with56.CommandType = CommandType.StoredProcedure;
						_with56.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.DELETE_SEA_IMP_SEC_CHG_EXCEPT";
						_with56.Parameters.Clear();
						_with56.Parameters.Add("JOB_CARD_SEA_IMP_PK_IN", JobCardPK).Direction = ParameterDirection.Input;
						_with56.Parameters.Add("JOB_TRN_SEA_IMP_FD_PKS", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
						_with56.Parameters.Add("JOB_TRN_SEA_IMP_COST_PKS", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
						_with56.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
						_with56.ExecuteNonQuery();
					}
					//TRAN.Commit()
				} catch (OracleException oraexp) {
					TRAN.Rollback();
					//Throw oraexp
				} catch (Exception ex) {
					TRAN.Rollback();
					//Throw ex
				} finally {
					//objwk.CloseConnection()
				}
			}
            return false;
		}
        #endregion

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
			sb.Append("  FROM JOB_CARD_SEA_IMP_TBL J");
			sb.Append(" WHERE J.JOB_CARD_SEA_IMP_PK = " + JobCardPk);
			sb.Append("   AND J.COLLECTION_STATUS = 1");
			sb.Append("   AND J.PAYEMENT_STATUS = 1");
			sb.Append("");
			try {
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}

        #endregion

        #region "Track N Trace Save Functionality"
        /// <summary>
        /// Saves the track and trace.
        /// </summary>
        /// <param name="jobPk">The job pk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="dsContainerData">The ds container data.</param>
        /// <param name="nlocationfk">The nlocationfk.</param>
        /// <param name="IsEditing">if set to <c>true</c> [is editing].</param>
        /// <param name="dsMain">The ds main.</param>
        /// <returns></returns>
        public object SaveTrackAndTrace(int jobPk, OracleTransaction TRAN, DataSet dsContainerData, int nlocationfk, bool IsEditing, DataSet dsMain)
		{
			int Cnt = 0;
			int POD = 0;
			var UpdCntrOnQuay = false;
			string strContData = null;
			System.DateTime ArrDate = default(System.DateTime);
			string Status = null;
			string PortDis = null;

			try {
				//The Below Lines are modified By Anand for Capturing Arrived At POD :12/11/08

				if (string.IsNullOrEmpty((dsMain.Tables[0].Rows[0]["ARRIVAL_DATE"].ToString()))) {
					ArrDate = System.DateTime.Now;
				} else {
					ArrDate = Convert.ToDateTime(dsMain.Tables[0].Rows[0]["ARRIVAL_DATE"]);
				}

				if (string.IsNullOrEmpty((dsMain.Tables[0].Rows[0]["PORT_MST_POD_FK"].ToString()))) {
					POD = 0;
				} else {
					POD = Convert.ToInt32(dsMain.Tables[0].Rows[0]["PORT_MST_POD_FK"]);
				}

				PortDis = CheckPOD(POD);
				Status = "Vessel Arrived At '" + PortDis + "' at '" + ArrDate + "' ";

				Status = "Vessel Arrived At '" + PortDis + "' at '" + ArrDate + "' ";

				objTrackNTrace.DeleteOnSaveTraceExportOnATDLDUpd(jobPk, 2, 2, "Vessel Voyage", "CNT-DT-DATA-DEL-SEA-IMP", nlocationfk, TRAN, "INS", CREATED_BY, "O",
				"Null");
				for (Cnt = 0; Cnt <= dsContainerData.Tables[0].Rows.Count - 1; Cnt++) {
					if (!string.IsNullOrEmpty((dsContainerData.Tables[0].Rows[Cnt]["gen_land_date"].ToString()))) {
						UpdCntrOnQuay = true;
						var _with57 = dsContainerData.Tables[0].Rows[Cnt];
						// Updated by Amit on 05-Jan-07 to solve Issue of Track & Trace
						// The Status "Discharge Container XXXXXXX at XXXX(POD ID)" should be displayed as "Container XXXXXXXX discharged at XXXXX (POD Name)"
						if (string.IsNullOrEmpty((dsMain.Tables[0].Rows[0]["POD"].ToString()))) {
							//strContData = "Discharge Container " & .Item("container_number") & "~" & .Item("gen_land_date")
							strContData = "Container " + _with57["container_number"] + " discharged" + "~" + _with57["gen_land_date"];
						} else {
							//strContData = "Discharge Container " & .Item("container_number") & " at  " & dsMain.Tables(0).Rows(0).Item("POD") & "~" & .Item("gen_land_date")
							strContData = "Container " + _with57["container_number"] + " discharged at  " + dsMain.Tables[0].Rows[0]["POD"] + "~" + _with57["gen_land_date"];
						}
						// End

						// strContData = "Discharge Container " & .Item("container_number") & " at  " & dsMain.Tables(0).Rows(0).Item("del_address") & "~" & .Item("gen_land_date")
						arrMessage = objTrackNTrace.SaveTrackAndTraceImportJob(jobPk, 2, 2, strContData, "CNT-ON-QUAY-DT-UPD-SEA-IMP", nlocationfk, TRAN, "INS", CREATED_BY, "O");
					}
				}
				//modified by thiyagarajan on 18/12/08 for import track n trace
				if (IsEditing == false) {
					arrMessage = objTrackNTrace.SaveTrackAndTraceImportJob(jobPk, 2, 2, "Job Card", "JOB-INS-SEA-IMP", nlocationfk, TRAN, "INS", CREATED_BY, "O");
					arrMessage = objTrackNTrace.SaveTrackAndTraceImportJobATA(jobPk, 2, 2, Status, "JOB-INS-SEA-IMP-ATA", nlocationfk, TRAN, "INS", CREATED_BY, "O",
					ArrDate);
				} else if (Convert.ToInt32(CheckATA(jobPk)) == 0) {
					arrMessage = objTrackNTrace.SaveTrackAndTraceImportJobATA(jobPk, 2, 2, Status, "JOB-INS-SEA-IMP-ATA", nlocationfk, TRAN, "INS", CREATED_BY, "O",
					ArrDate);
				}
				arrMessage.Clear();
				arrMessage.Add("All Data Saved Successfully");
				return arrMessage;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				arrMessage.Add("Error");
				return arrMessage;
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
			try {
				WorkFlow objWF = new WorkFlow();
				string sql = null;
				string PortDis = "";

				sql = "select pmt.port_name from port_mst_tbl pmt where pmt.port_mst_pk='" + POD + "'";
				PortDis = objWF.ExecuteScaler(sql);

				return PortDis;
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
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
			try {
				sql = "select count(*) from TRACK_N_TRACE_TBL TR WHERE TR.STATUS LIKE '%Vessel Arrived At%' and TR.BIZ_TYPE=2 AND ";
				sql += " TR.PROCESS = 2 And TR.JOB_CARD_FK = " + jobpk;
				return objWF.ExecuteScaler(sql);
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

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

			try {
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

			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
        #endregion

        /// <summary>
        /// Fills the container type data set.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet FillContainerTypeDataSet(string pk = "0", int flag = 0)
		{
			System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();

			if (flag > 0) {
				strSQL.Append("    select 0 as container_type_mst_pk  ,' ' container_type_mst_id from dual union select distinct");
			} else {
				strSQL.Append("  select distinct");
			}
			strSQL.Append(" cont.container_type_mst_pk,");
			strSQL.Append(" cont.container_type_mst_id");
			strSQL.Append(" FROM");
			strSQL.Append("  JOB_TRN_CONT job_trn,");
			strSQL.Append(" container_type_mst_tbl cont");
			strSQL.Append(" WHERE");
			strSQL.Append(" job_trn.container_type_mst_fk = cont.container_type_mst_pk");
			strSQL.Append(" and job_trn.JOB_CARD_TRN_FK =" + pk);

			if (flag > 0) {
				strSQL.Append(" ORDER BY container_type_mst_id");
			} else {
				strSQL.Append(" ORDER BY cont.container_type_mst_id");
			}


			try {
				return objWF.GetDataSet(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}

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
			strSQL.Append("  group by cargo_move_pk,cargo_move_code ");

			try {
				return objWF.GetDataSet(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
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

			strSQL.Append("SELECT");
			strSQL.Append("      s.shipping_terms_mst_pk,");
			strSQL.Append("      s.inco_code ");
			strSQL.Append(" FROM");
			strSQL.Append("      shipping_terms_mst_tbl s ");
			strSQL.Append(" WHERE");
			strSQL.Append("      s.active_flag = 1 ");
			strSQL.Append("  group by shipping_terms_mst_pk,inco_code ");

			try {
				return objWF.GetDataSet(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
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
			try {
				return objWF.GetDataSet(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}


        #endregion

        #region " Fetch Revenue data export"
        /// <summary>
        /// Fetches the revenue data.
        /// </summary>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
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

			try {
				objWF.MyCommand.Parameters.Clear();
				var _with58 = objWF.MyCommand.Parameters;
				_with58.Add("JOBCARD_PK", jobCardPK).Direction = ParameterDirection.Input;
				_with58.Add("JOB_SEA_IMP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				return objWF.GetDataSet("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_SEA_IMP");
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "Calculate_TAX" ''Added by subhransu for tax calculation
        /// <summary>
        /// Calculate_s the tax.
        /// </summary>
        /// <param name="jobCardID">The job card identifier.</param>
        /// <returns></returns>
        public object Calculate_TAX(string jobCardID = "0")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();

			try {
				sb.Append(" SELECT  NVL(SUM(JP.TAX_AMT), 0) AS COST_TAX");
				sb.Append("   FROM JOB_CARD_SEA_IMP_TBL   JC,");
				sb.Append("        JOB_TRN_SEA_IMP_PIA    JP");
				sb.Append("  WHERE ");
				sb.Append("     JC.JOB_CARD_SEA_IMP_PK = " + jobCardID + "");
				sb.Append("    AND JC.JOB_CARD_SEA_IMP_PK = JP.JOB_CARD_SEA_IMP_FK(+)");

				return (objWF.GetDataSet(sb.ToString()));

			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
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

			try {
				sb.Append(" SELECT NVL(SUM(CI.TAX_AMT), 0) AS REVENUE_TAX");
				sb.Append("   FROM JOB_CARD_SEA_IMP_TBL   JC,");
				sb.Append("        CONSOL_INVOICE_TRN_TBL CI");

				sb.Append("  WHERE JC.JOB_CARD_SEA_IMP_PK = CI.JOB_CARD_FK(+)");
				sb.Append("    AND JC.JOB_CARD_SEA_IMP_PK = " + jobCardID + "");

				return (objWF.GetDataSet(sb.ToString()));

			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion
        #region "GetRevenueDetails"
        /// <summary>
        /// Gets the revenue details.
        /// </summary>
        /// <param name="LocationPk">The location pk.</param>
        /// <param name="actualCost">The actual cost.</param>
        /// <param name="actualRevenue">The actual revenue.</param>
        /// <param name="estimatedCost">The estimated cost.</param>
        /// <param name="estimatedRevenue">The estimated revenue.</param>
        /// <param name="jobCardPK">The job card pk.</param>
        /// <returns></returns>
        public DataSet GetRevenueDetails(int LocationPk, decimal actualCost, decimal actualRevenue, decimal estimatedCost, decimal estimatedRevenue, string jobCardPK)
		{

			//Dim SQL As New System.Text.StringBuilder
			WorkFlow objWF = new WorkFlow();
			//Snigdharani - 10/11/2008 - making the values same as consolidation screen.
			try {
				DataSet DS = new DataSet();
				//adding "CurrPk" by thiyagarajan on 24/11/08 for location based currency task
				var _with59 = objWF.MyCommand.Parameters;
				_with59.Add("JCPK", jobCardPK).Direction = ParameterDirection.Input;
				_with59.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
				_with59.Add("JOB_IMP_SEA", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				DS = objWF.GetDataSet("FETCH_COST_REVENUE_PROFIT", "FETCH_JOBCARD_IMP_SEA");
				return DS;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
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
			//SQL.Append(vbCrLf & "           job_trn_sea_imp_pia  job_trn_pia,")
			//SQL.Append(vbCrLf & "           currency_type_mst_tbl curr,")
			//SQL.Append(vbCrLf & "           cost_element_mst_tbl cost_ele,")
			//SQL.Append(vbCrLf & "           job_card_sea_imp_tbl job_imp")
			//SQL.Append(vbCrLf & "     WHERE")
			//SQL.Append(vbCrLf & "           job_trn_pia.job_card_sea_imp_fk = job_imp.job_card_sea_imp_pk")
			//SQL.Append(vbCrLf & "           AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk")
			//SQL.Append(vbCrLf & "           AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk")
			//SQL.Append(vbCrLf & "           AND job_imp.job_card_sea_imp_pk =" + jobCardPK)

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
			//SQL.Append(vbCrLf & "       job_trn_sea_imp_fd  job_trn_fd,")
			//SQL.Append(vbCrLf & "       currency_type_mst_tbl curr,")
			//SQL.Append(vbCrLf & "       job_card_sea_imp_tbl job_imp")
			//SQL.Append(vbCrLf & "    WHERE")
			//SQL.Append(vbCrLf & "       job_trn_fd.job_card_sea_imp_fk = job_imp.job_card_sea_imp_pk")
			//SQL.Append(vbCrLf & "       AND job_trn_fd.currency_mst_fk =curr.currency_mst_pk")
			//SQL.Append(vbCrLf & "       AND job_imp.job_card_sea_imp_pk =" + jobCardPK)

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
			//SQL.Append(vbCrLf & "               AND q.currency_mst_pk = exch.currency_mst_fk")
			//SQL.Append(vbCrLf & "           )end ")
			//SQL.Append(vbCrLf & "   ),4)) ""Estimated Revenue""")
			//SQL.Append(vbCrLf & "FROM")
			//SQL.Append(vbCrLf & "   (SELECT")
			//SQL.Append(vbCrLf & "       job_imp.jobcard_date,")
			//SQL.Append(vbCrLf & "       curr.currency_mst_pk,")
			//SQL.Append(vbCrLf & "       sum(job_trn_othr.amount) freight_amt")
			//SQL.Append(vbCrLf & "    FROM")
			//SQL.Append(vbCrLf & "       job_trn_sea_imp_oth_chrg  job_trn_othr,")
			//SQL.Append(vbCrLf & "       currency_type_mst_tbl curr,")
			//SQL.Append(vbCrLf & "       job_card_sea_imp_tbl job_imp")
			//SQL.Append(vbCrLf & "    WHERE")
			//SQL.Append(vbCrLf & "       job_trn_othr.job_card_sea_imp_fk = job_imp.job_card_sea_imp_pk")
			//SQL.Append(vbCrLf & "       AND job_trn_othr.currency_mst_fk =curr.currency_mst_pk")
			//SQL.Append(vbCrLf & "       AND job_imp.job_card_sea_imp_pk =" + jobCardPK)
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
			//        '.Add("LocationsPk", LocationPk).Direction = ParameterDirection.Input 'adding by Thiyagarajan for location based curr.
			//        'Commented BY ANAND AS LOCATION Based Currency is not moved to eqa
			//        .Add("JOB_SEA_IMP_CUR", OracleClient.OracleDbType.RefCursor).Direction = ParameterDirection.Output
			//    End With
			//    'Return objWF.GetDataSet("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_SEA_EXP")
			//Catch sqlExp As Exception
			//    Throw sqlExp
			//End Try

			//oraReader = objWF.GetDataReader("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_SEA_IMP_ACTREV")
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

        /// <summary>
        /// Fills the job card other charges data set.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <param name="baseCurrency">The base currency.</param>
        /// <returns></returns>
        public DataSet FillJobCardOtherChargesDataSet(string pk = "0", Int64 baseCurrency = 0)
		{
			System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();

			strSQL.Append("         SELECT");
			strSQL.Append("         oth_chrg.job_trn_sea_imp_oth_pk,");
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
			if (baseCurrency > 0) {
				strSQL.Append("    ROUND(GET_EX_RATE(oth_chrg.currency_mst_fk," + baseCurrency + ",round(sysdate - .5)),4) AS ROE ,");
			} else {
				strSQL.Append("         oth_chrg.EXCHANGE_RATE \"ROE\",");
			}
			strSQL.Append("         oth_chrg.amount amount,");
			strSQL.Append("         'false' \"Delete\"");
			strSQL.Append("FROM");
			strSQL.Append("         job_trn_sea_imp_oth_chrg oth_chrg,");
			strSQL.Append("         job_card_sea_imp_tbl jobcard_mst,");
			strSQL.Append("         freight_element_mst_tbl frt,");
			strSQL.Append("         currency_type_mst_tbl curr,");
			//latha
			strSQL.Append("   location_mst_tbl loc,");
			strSQL.Append("   customer_mst_tbl cus");

			strSQL.Append("WHERE");
			strSQL.Append("         oth_chrg.job_card_sea_imp_fk = jobcard_mst.job_card_sea_imp_pk");
			strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
			strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
			strSQL.Append("         AND oth_chrg.job_card_sea_imp_fk    = " + pk);
			//latha
			strSQL.Append("   AND oth_chrg.frtpayer_cust_mst_fk = cus.customer_mst_pk(+) ");
			strSQL.Append("   AND oth_chrg.location_mst_fk = loc.location_mst_pk(+) ");

			strSQL.Append("ORDER BY freight_element_id ");

			try {
				return objWF.GetDataSet(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}

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
			///strsql = "select * from  CONSOL_INVOICE_TRN_TBl where JOB_CARD_FK = " & jobcardpk & " AND FRT_OTH_ELEMENT = 1"
			strsql = "select con.FRT_OTH_ELEMENT_FK,cont.container_type_mst_pk from  CONSOL_INVOICE_TRN_TBl con , ";
			strsql = strsql + " job_trn_sea_imp_fd fd_trn, container_type_mst_tbl cont ";
			strsql = strsql + " where con.FRT_OTH_ELEMENT = 1 AND fd_trn.container_type_mst_fk = cont.container_type_mst_pk";
			strsql = strsql + " and con.frt_oth_element_fk=fd_trn.freight_element_mst_fk ";
			strsql = strsql + "  and con.JOB_CARD_FK = " + jobcardpk;
			strsql = strsql + " and con.consol_invoice_trn_pk=fd_trn.consol_invoice_trn_fk ";
			return objWF.GetDataSet(strsql);
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
			try {
				sb.Append("SELECT INVTRN.COST_FRT_ELEMENT_FK, CONT.CONTAINER_TYPE_MST_PK");
				sb.Append("  FROM INV_AGENT_TBL     INV,");
				sb.Append("       INV_AGENT_TRN_TBL INVTRN,");
				sb.Append("       JOB_TRN_SEA_IMP_FD        JOB_TRN_FD,");
				sb.Append("       CONTAINER_TYPE_MST_TBL    CONT");
				sb.Append(" WHERE INVTRN.COST_FRT_ELEMENT = 2");
				sb.Append("   AND JOB_TRN_FD.CONTAINER_TYPE_MST_FK = CONT.CONTAINER_TYPE_MST_PK(+)");
				sb.Append("   AND INV.JOB_CARD_FK = JOB_TRN_FD.JOB_CARD_SEA_IMP_FK");
				sb.Append("   AND INV.INV_AGENT_PK = INVTRN.INV_AGENT_FK");
				sb.Append("   AND INVTRN.COST_FRT_ELEMENT_FK = JOB_TRN_FD.FREIGHT_ELEMENT_MST_FK");
				sb.Append("   AND INV.JOB_CARD_FK = " + jobcardpk);
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        /// <summary>
        /// Gets the oth character.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public DataSet getOthChr(string pk = "0")
		{
			System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();

			strSQL.Append("         SELECT");
			strSQL.Append("         oth_chrg.inv_cust_trn_sea_imp_fk,");
			strSQL.Append("         oth_chrg.inv_agent_trn_sea_imp_fk,");
			strSQL.Append("         oth_chrg.consol_invoice_trn_fk");
			strSQL.Append("FROM");
			strSQL.Append("         job_trn_sea_imp_oth_chrg oth_chrg,");
			strSQL.Append("         job_card_sea_imp_tbl jobcard_mst,");
			strSQL.Append("         freight_element_mst_tbl frt,");
			strSQL.Append("         currency_type_mst_tbl curr,");
			//latha
			strSQL.Append("   location_mst_tbl loc,");
			strSQL.Append("   customer_mst_tbl cus");
			strSQL.Append("WHERE");
			strSQL.Append("         oth_chrg.job_card_sea_imp_fk = jobcard_mst.job_card_sea_imp_pk");
			strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
			strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
			strSQL.Append("         AND oth_chrg.job_card_sea_imp_fk    = " + pk);
			//latha
			strSQL.Append("   AND oth_chrg.frtpayer_cust_mst_fk = cus.customer_mst_pk(+) ");
			strSQL.Append("   AND oth_chrg.location_mst_fk = loc.location_mst_pk(+) ");

			strSQL.Append("ORDER BY freight_element_id ");

			try {
				return objWF.GetDataSet(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
        /// <summary>
        /// Gets the base currency.
        /// </summary>
        /// <returns></returns>
        public Int64 GetBaseCurrency()
		{
			WorkFlow objWF = new WorkFlow();
			StringBuilder SQL = new StringBuilder();

			SQL.Append(" SELECT c.currency_mst_fk FROM corporate_mst_tbl c ");

			try {
				return Convert.ToInt64(objWF.ExecuteScaler(SQL.ToString()));
			} catch (OracleException sqlExp) {
				return 0;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region " Fetch base currency Exchange rate on vessel voyage"
        /// <summary>
        /// Fetches the vessel voyage roe.
        /// </summary>
        /// <param name="voyage">The voyage.</param>
        /// <returns></returns>
        public DataSet FetchVesselVoyageROE(Int64 voyage)
		{
			string strSQL = null;
			WorkFlow objWF = new WorkFlow();

			try {
				strSQL = " SELECT C.CURRENCY_MST_PK," + " 1  ROE" + " FROM CURRENCY_TYPE_MST_TBL C" + " WHERE C.CURRENCY_MST_PK =" + "(SELECT CMT.CURRENCY_MST_FK FROM CORPORATE_MST_TBL CMT)" + " AND C.ACTIVE_FLAG = 1" + " UNION" + " SELECT CURR.CURRENCY_MST_PK," + " EXCHANGE_RATE ROE" + " FROM CURRENCY_TYPE_MST_TBL CURR, Exchange_Rate_Trn EXC" + " WHERE EXC.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK" + " AND CURR.ACTIVE_FLAG = 1" + " AND EXC.Voyage_Trn_Fk is not null" + " AND exc.voyage_trn_fk = " + voyage + " AND EXC.CURRENCY_MST_BASE_FK =" + " (SELECT CMT.CURRENCY_MST_FK FROM CORPORATE_MST_TBL CMT)" + " AND EXC.CURRENCY_MST_BASE_FK <> EXC.CURRENCY_MST_FK" + " UNION" + " SELECT T.CURRENCY_MST_PK," + " TO_NUMBER(NULL) ROE" + " FROM CURRENCY_TYPE_MST_TBL T,CORPORATE_MST_TBL CC" + " WHERE T.CURRENCY_MST_PK NOT IN" + " ( SELECT CURRENCY_MST_PK" + " FROM CURRENCY_TYPE_MST_TBL , Exchange_Rate_Trn " + " WHERE CURRENCY_MST_FK = CURRENCY_MST_PK" + " AND ACTIVE_FLAG = 1" + " AND voyage_trn_fk = " + voyage + " AND CURRENCY_MST_BASE_FK =" + " (SELECT CURRENCY_MST_FK FROM CORPORATE_MST_TBL CMT)" + " AND CURRENCY_MST_BASE_FK <> CURRENCY_MST_FK )" + " AND T.CURRENCY_MST_PK<>CC.CURRENCY_MST_FK";


				return objWF.GetDataSet(strSQL);
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
        #endregion

        #region " Fetch base currency Exchange rate export"
        /// <summary>
        /// Fetches the roe.
        /// </summary>
        /// <param name="baseCurrency">The base currency.</param>
        /// <param name="todate">The todate.</param>
        /// <returns></returns>
        public DataSet FetchROE(Int64 baseCurrency, string todate)
		{
			StringBuilder strSQL = new StringBuilder();
			WorkFlow objWF = new WorkFlow();

			try {
				strSQL.Append("SELECT");
				strSQL.Append("    CURR.CURRENCY_MST_PK,");
				strSQL.Append("    CURR.CURRENCY_ID,");
				//strSQL.Append(vbCrLf & "    ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK,(select c.currency_mst_fk from corporate_mst_tbl c),round(sysdate - .5)),6) AS ROE")
				//removing "Corporate mst tbl" by thiyagarajan on 21/11/08 for location based currency task
				strSQL.Append("    ROUND(GET_EX_RATE(CURR.CURRENCY_MST_PK," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",to_date('" + todate + "',dateformat)),6) as ROE ");
				//)),6) AS ROE")
				strSQL.Append("FROM");
				strSQL.Append("    CURRENCY_TYPE_MST_TBL CURR");
				strSQL.Append("WHERE");
				strSQL.Append("    CURR.ACTIVE_FLAG = 1");

				return objWF.GetDataSet(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
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

			if (invoiceType == 1) {
				//SQL.Append(vbCrLf & "select i.inv_cust_sea_imp_pk from inv_cust_sea_imp_tbl i where i.job_card_sea_imp_fk = " & jobCardPK)
				SQL.Append("SELECT CON.CONSOL_INVOICE_PK");
				SQL.Append("  FROM CONSOL_INVOICE_TBL CON, CONSOL_INVOICE_TRN_TBL CONTRN");
				SQL.Append(" WHERE CON.CONSOL_INVOICE_PK = CONTRN.CONSOL_INVOICE_FK");
				SQL.Append("  AND CON.BUSINESS_TYPE = 2");
				SQL.Append("  AND CON.PROCESS_TYPE = 2");
				SQL.Append("   AND CONTRN.JOB_CARD_FK = " + jobCardPK);
			} else if (invoiceType == 2) {
				SQL.Append("select i.inv_agent_pk from INV_AGENT_TBL i where i.CB_DP_LOAD_AGENT=1 AND  i.job_card_fk = " + jobCardPK);
			} else if (invoiceType == 3) {
				SQL.Append("select i.inv_agent_pk from INV_AGENT_TBL i where  i.CB_DP_LOAD_AGENT=2 AND i.job_card_fk = " + jobCardPK);
			}

			oraReader = objWF.GetDataReader(SQL.ToString());

			while (oraReader.Read()) {
				if ((!object.ReferenceEquals(oraReader[0], ""))) {
					invoicePK = Convert.ToInt64(oraReader[0]);
					invoiceCount += 1;
				}
			}

			if (invoiceCount == 0) {
				return 0;
			} else if (invoiceCount > 1) {
				return -1;
			} else {
				return invoicePK;
			}

			oraReader.Close();

			try {
				return invoicePK;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
        #endregion

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

			try {
				strSQL.Append("select sum(nvl(cont.gross_weight,0)),");
				strSQL.Append("           sum(nvl(cont.volume_in_cbm,0))");
				strSQL.Append("      from job_card_sea_imp_tbl job, job_trn_sea_imp_cont cont");
				strSQL.Append("     where job.job_card_sea_imp_pk = " + jobcardNumber);
				strSQL.Append("   and job.job_card_sea_imp_pk = cont.job_card_sea_imp_fk");

				oraReader = objWF.GetDataReader(strSQL.ToString());

				while (oraReader.Read()) {
					if ((!object.ReferenceEquals(oraReader[0], ""))) {
						strWeight = Convert.ToString(oraReader[0]);
					}

					if ((!object.ReferenceEquals(oraReader[1], ""))) {
						strVolume = Convert.ToString(oraReader[1]);
					}
				}

				strReturnValue = strWeight + "|" + strVolume;

				return strReturnValue;

			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
        #endregion

        #region " Update Job Card data to Export Side"
        /// <summary>
        /// Saves the data to export.
        /// </summary>
        /// <param name="JobCardRefNo">The job card reference no.</param>
        /// <param name="CargoMovePk">The cargo move pk.</param>
        /// <param name="Shipment_Terms">The shipment_ terms.</param>
        /// <param name="PymtType">Type of the pymt.</param>
        /// <param name="ConsigneePK">The consignee pk.</param>
        /// <param name="ETADate">The eta date.</param>
        /// <param name="ArrivalDate">The arrival date.</param>
        /// <returns></returns>
        public object SaveDataToExport(string JobCardRefNo, long CargoMovePk, long Shipment_Terms, long PymtType, string ConsigneePK, string ETADate, string ArrivalDate)
		{
			string strSQL = null;

			WorkFlow objWF = new WorkFlow();
			try {
				strSQL = " update job_card_sea_exp_tbl job_exp ";
				strSQL = strSQL + " set job_exp.pymt_type = " + PymtType;
				if (CargoMovePk > 0) {
					strSQL = strSQL + " , job_exp.cargo_move_fk = " + CargoMovePk;
				}
				if (Shipment_Terms > 0) {
					strSQL = strSQL + " , job_exp.shipping_terms_mst_fk =   " + Shipment_Terms;
				}
				strSQL = strSQL + " , job_exp.CONSIGNEE_CUST_MST_FK = " + ConsigneePK;
				strSQL = strSQL + " , job_exp.ETA_DATE =  to_date('" + ETADate + "', datetimeformat24)";
				//" & ETADate & "'"
				strSQL = strSQL + " , job_exp.ARRIVAL_DATE = to_date('" + ArrivalDate + "', datetimeformat24)";
				//" & ArrivalDate & "'"
				strSQL = strSQL + " where job_exp.jobcard_ref_no in ( ";
				strSQL = strSQL + "  select j.jobcard_ref_no from job_card_sea_imp_tbl jj,job_card_sea_exp_tbl j ";
				strSQL = strSQL + " where jj.jobcard_ref_no = j.jobcard_ref_no  ";
				strSQL = strSQL + " and jj.jobcard_ref_no = '" + JobCardRefNo + "' )";
				return objWF.ExecuteScaler(strSQL.ToString());
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}

		}
        #endregion

        #region "Get Invoice PK"
        /// <summary>
        /// Gets the pk value.
        /// </summary>
        /// <param name="strID">The string identifier.</param>
        /// <param name="FieldType">Type of the field.</param>
        /// <returns></returns>
        public long GetPKValue(string strID, string FieldType)
		{

			System.Text.StringBuilder SQL = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			long lngPK = 0;

			if (FieldType == "POL" | FieldType == "POD" | FieldType == "PFD" | FieldType == "POO") {
				SQL.Append("select p.port_mst_pk from port_mst_tbl p where p.port_id = '" + strID + "' ");
			} else if (FieldType == "Customer" | FieldType == "Shipper" | FieldType == "Consignee" | FieldType == "Notify1" | FieldType == "Freight Payer") {
				SQL.Append("select c.customer_mst_pk from customer_mst_tbl c where c.customer_id = '" + strID + "' ");
			} else if (FieldType == "Line Operator") {
				SQL.Append("select op.operator_mst_pk from operator_mst_tbl op where op.operator_id = '" + strID + "' ");
			} else if (FieldType == "AirLine Operator") {
				SQL.Append("select AMT.AIRLINE_MST_PK from AIRLINE_MST_TBL AMT where AMT.AIRLINE_ID = '" + strID + "' ");
			} else if (FieldType == "POL Agent" | FieldType == "X-Trade") {
				SQL.Append("select agnt.agent_mst_pk  from agent_mst_tbl agnt where agnt.agent_id = '" + strID + "' ");
			} else if (FieldType == "Shippment Term") {
				SQL.Append("select SHPTERM.SHIPPING_TERMS_MST_PK FROM SHIPPING_TERMS_MST_TBL SHPTERM WHERE SHPTERM.INCO_CODE  = '" + strID + "' ");
			} else if (FieldType == "Cargo MoveCode") {
				SQL.Append(" select  c.cargo_move_pk FROM cargo_move_mst_tbl c where c.cargo_move_code= '" + strID + "' ");
			} else if (FieldType == "First Vsl") {
				SQL.Append(" select VOYTRN.VOYAGE_TRN_PK  from VESSEL_VOYAGE_TRN VOYTRN, VESSEL_VOYAGE_TBL VSLVOY ");
				SQL.Append("  WHERE(VSLVOY.VESSEL_VOYAGE_TBL_PK = VOYTRN.VESSEL_VOYAGE_TBL_FK) ");
				SQL.Append("  AND VSLVOY.VESSEL_NAME = '" + strID + "' ");
			} else if (FieldType == "Commodity Grp") {
				if (strID == "GENERAL") {
					SQL.Append("SELECT COMMGRP.COMMODITY_GROUP_PK FROM COMMODITY_GROUP_MST_TBL COMMGRP, PARAMETERS_TBL PT WHERE COMMGRP.COMMODITY_GROUP_PK=PT.GENERAL_CARGO_FK ");
				} else if (strID == "REEFER") {
					SQL.Append("SELECT COMMGRP.COMMODITY_GROUP_PK FROM COMMODITY_GROUP_MST_TBL COMMGRP, PARAMETERS_TBL PT WHERE COMMGRP.COMMODITY_GROUP_PK=PT.REEFER_CARGO_FK ");
				} else if (strID == "HAZARDOUS") {
					SQL.Append("SELECT COMMGRP.COMMODITY_GROUP_PK FROM COMMODITY_GROUP_MST_TBL COMMGRP, PARAMETERS_TBL PT WHERE COMMGRP.COMMODITY_GROUP_PK=PT.HAZ_CARGO_FK ");
				} else if (strID == "BREAK BULK") {
					SQL.Append("SELECT COMMGRP.COMMODITY_GROUP_PK FROM COMMODITY_GROUP_MST_TBL COMMGRP, PARAMETERS_TBL PT WHERE COMMGRP.COMMODITY_GROUP_PK=PT.ODC_CARGO_FK ");
				} else {
					SQL.Append("SELECT COMMGRP.COMMODITY_GROUP_PK FROM COMMODITY_GROUP_MST_TBL COMMGRP, PARAMETERS_TBL PT WHERE COMMGRP.COMMODITY_GROUP_PK=PT.GENERAL_CARGO_FK ");
				}
			} else if (FieldType == "Container Type") {
				SQL.Append("select cnt.container_type_mst_pk  from container_type_mst_tbl cnt where cnt.container_type_mst_id  = '" + strID + "' ");
			} else if (FieldType == "Pack Type") {
				SQL.Append("select pck.pack_type_mst_pk  from pack_type_mst_tbl pck where pck.pack_type_id  = '" + strID + "' ");
			} else if (FieldType == "Commodity") {
				SQL.Append("SELECT comm.commodity_mst_pk FROM commodity_mst_tbl comm where comm.commodity_id ='" + strID + "' ");
			} else if (FieldType == "Freight Element") {
				SQL.Append("select frt.freight_element_mst_pk from  freight_element_mst_tbl frt where frt.freight_element_id ='" + strID + "' ");
			} else if (FieldType == "Freight Curency") {
				SQL.Append("select curr.currency_mst_pk from currency_type_mst_tbl curr where curr.currency_id ='" + strID + "' ");
			} else if (FieldType == "Liner Terms") {
				SQL.Append("SELECT QDT.DD_VALUE FROM QFOR_DROP_DOWN_TBL QDT WHERE QDT.CONFIG_ID = 'QFOR3052' AND QDT.DD_FLAG='LINER_TERMS' AND QDT.DD_ID='" + strID + "' ");
			} else if (FieldType == "Move Code") {
				SQL.Append("SELECT CMV.CARGO_MOVE_PK FROM CARGO_MOVE_MST_TBL CMV, QFOR_DROP_DOWN_TBL QD WHERE CMV.WIN_CARGO_MOVE_FK = QD.DD_VALUE AND QD.DD_FLAG='CARGO_MOVEMENT' AND QD.CONFIG_ID='QFLX2008' AND QD.DD_ID LIKE '" + strID + "%' ");
			} else if (FieldType == "Delv Mode") {
				SQL.Append("SELECT QDT.DD_VALUE FROM QFOR_DROP_DOWN_TBL QDT WHERE QDT.CONFIG_ID = 'QFOR3030' AND QDT.DD_FLAG='Carriage_Mode_IMP_POD' AND QDT.DD_ID LIKE '" + strID + "%' ");
			} else if (FieldType == "Pick Mode") {
				SQL.Append(" SELECT QDT.DD_VALUE FROM QFOR_DROP_DOWN_TBL QDT WHERE QDT.CONFIG_ID='QFOR3030' AND QDT.DD_FLAG='Carriage_Mode_EXP_POD' AND QDT.DD_ID LIKE '" + strID + "%' ");
			} else if (FieldType == "Customer" | FieldType == "WinShipper" | FieldType == "WinConsignee" | FieldType == "WinNotify") {
				SQL.Append("select c.customer_mst_pk from customer_mst_tbl c where c.customer_name = '" + strID + "' ");
			} else if (FieldType == "User") {
				SQL.Append(" SELECT UMT.USER_MST_PK FROM USER_MST_TBL UMT WHERE UMT.USER_NAME='" + strID + "' ");
			} else if (FieldType == "CB_DOC_TYPE") {
				SQL.Append(" SELECT CBMT.CB_DOC_MST_PK FROM CB_DOC_MST_TBL CBMT WHERE CBMT.DOC_ID = '" + strID + "'");
			} else if (FieldType == "Currency") {
				SQL.Append(" SELECT CTMT.CURRENCY_MST_PK FROM CURRENCY_TYPE_MST_TBL CTMT WHERE CTMT.CURRENCY_ID = '" + strID + "'");
			} else if (FieldType == "JMF_ACTIVITY") {
				SQL.Append(" SELECT QDT.DD_VALUE FROM QFOR_DROP_DOWN_TBL QDT WHERE QDT.CONFIG_ID='QFOR4369' AND QDT.DD_FLAG='JMF_ACTIVITY' AND SUBSTR(QDT.DD_ID, 0, 3) = '" + strID + "'");
			} else if (FieldType == "JMF_STATUS") {
				SQL.Append(" SELECT QDT.DD_VALUE FROM QFOR_DROP_DOWN_TBL QDT WHERE QDT.CONFIG_ID='QFOR4369' AND QDT.DD_FLAG='JMF_WIN_STATUS' AND SUBSTR(QDT.DD_ID, 0, 2) = '" + strID + "'");
			} else if (FieldType == "CARRIER_TYPE") {
				SQL.Append(" SELECT QDT.DD_VALUE FROM QFOR_DROP_DOWN_TBL QDT WHERE QDT.DD_FLAG = 'SCarrier_Type_IMP_POD' AND QDT.CONFIG_ID='QFOR3030' AND QDT.DD_ID = '" + strID + "'");
			} else if (FieldType == "CARRIER_MODE") {
				SQL.Append(" SELECT QDT.DD_VALUE FROM QFOR_DROP_DOWN_TBL QDT WHERE QDT.DD_FLAG = 'Carriage_Mode_IMP_POD' AND QDT.CONFIG_ID='QFOR3030' AND QDT.DD_ID = '" + strID + "'");
			} else if (FieldType == "CARRIER_MODE_AIR") {
				SQL.Append(" SELECT QDT.DD_VALUE FROM QFOR_DROP_DOWN_TBL QDT WHERE QDT.DD_FLAG = 'ACarriage_Mode_IMP_POD' AND QDT.CONFIG_ID='QFOR3030' AND QDT.DD_ID = '" + strID + "'");
			} else if (FieldType == "Commodity Name") {
				SQL.Append("SELECT comm.commodity_mst_pk FROM commodity_mst_tbl comm where Upper(comm.COMMODITY_NAME) =UPPER('" + strID + "')");
			} else if (FieldType == "Country") {
				SQL.Append(" SELECT CTR.COUNTRY_MST_PK FROM COUNTRY_MST_TBL CTR WHERE CTR.COUNTRY_ID ='" + strID + "'");
			} else if (FieldType == "PackType") {
				SQL.Append(" SELECT PTMT.PACK_TYPE_MST_PK FROM PACK_TYPE_MST_TBL PTMT WHERE PTMT.ACTIVE_FLAG = 1 AND PTMT.PACK_TYPE_ID ='" + strID + "'");
			} else if (FieldType == "Dimension") {
				SQL.Append(" SELECT DMT.DIMENTION_UNIT_MST_PK FROM DIMENTION_UNIT_MST_TBL DMT WHERE DMT.DIMENTION_ID ='" + strID + "'");
			} else if (FieldType == "Dimension_UOM") {
				SQL.Append(" SELECT QDT.DD_VALUE FROM QFOR_DROP_DOWN_TBL QDT WHERE QDT.DD_FLAG = 'WIN_DIM_UOM' AND QDT.DD_ID='" + strID + "'");
			}

			try {
				lngPK = Convert.ToInt64(objWF.GetDataTable(SQL.ToString()).Rows[0][0]);
				return lngPK;

			} catch (OracleException sqlExp) {
				return 0;
			} catch (Exception exp) {
				return 0;
			}

		}
        #endregion

        #region "check Customer,Shipper,Notify Paryty"
        /// <summary>
        /// Checks the exist.
        /// </summary>
        /// <param name="strID">The string identifier.</param>
        /// <param name="FieldType">Type of the field.</param>
        /// <returns></returns>
        public object CheckExist(string strID, string FieldType)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			int CustCount = 0;
			int CustCatCount = 0;
			int TempCustCount = 0;
			try {
				sb.Append(" SELECT COUNT(*) FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_NAME ='" + strID + "'");
				CustCount = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
				if (CustCount > 0) {
					sb.Remove(0, sb.Length);
					sb.Append(" SELECT COUNT(*) FROM CUSTOMER_MST_TBL  CMT,CUSTOMER_CATEGORY_TRN  CCT,CUSTOMER_CATEGORY_MST_TBL CCMT ");
					sb.Append(" WHERE CMT.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK ");
					sb.Append(" AND CCT.CUSTOMER_CATEGORY_MST_FK = CCMT.CUSTOMER_CATEGORY_MST_PK ");
					sb.Append(" AND CCMT.CUSTOMER_CATEGORY_ID = '" + FieldType + "'");
					sb.Append(" AND CMT.CUSTOMER_NAME ='" + strID + "'");
					CustCatCount = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
					if (CustCatCount > 0) {
						sb.Remove(0, sb.Length);
						sb.Append(" SELECT CMT.CUSTOMER_MST_PK FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_NAME ='" + strID + "'");
						return objWF.ExecuteScaler(sb.ToString());
					} else {
						// objWF.OpenConnection() ''Check for Temp
						sb.Remove(0, sb.Length);
						sb.Append(" SELECT COUNT(*) FROM TEMP_CUSTOMER_TBL TMP ");
						sb.Append(" WHERE TMP.CUSTOMER_TYPE_FK IN (SELECT CCAT.CUSTOMER_CATEGORY_MST_PK ");
						sb.Append(" FROM CUSTOMER_CATEGORY_MST_TBL CCAT ");
						sb.Append(" WHERE CCAT.CUSTOMER_CATEGORY_ID = '" + FieldType + "')");
						sb.Append(" AND TMP.CUSTOMER_NAME ='" + strID + "'");
						TempCustCount = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
						if (TempCustCount > 0) {
							sb.Remove(0, sb.Length);
							sb.Append(" SELECT TMP.CUSTOMER_MST_PK FROM TEMP_CUSTOMER_TBL TMP ");
							sb.Append(" WHERE TMP.CUSTOMER_TYPE_FK IN (SELECT CCAT.CUSTOMER_CATEGORY_MST_PK ");
							sb.Append(" FROM CUSTOMER_CATEGORY_MST_TBL CCAT ");
							sb.Append(" WHERE CCAT.CUSTOMER_CATEGORY_ID = '" + FieldType + "')");
							sb.Append(" AND TMP.CUSTOMER_NAME ='" + strID + "'");
							return objWF.ExecuteScaler(sb.ToString());
						} else {
							return 0;
						}
					}

				//'Check Temp Customer is there
				} else {
					sb.Remove(0, sb.Length);
					sb.Append(" SELECT COUNT(*) FROM TEMP_CUSTOMER_TBL TMP ");
					sb.Append(" WHERE TMP.CUSTOMER_TYPE_FK IN (SELECT CCAT.CUSTOMER_CATEGORY_MST_PK ");
					sb.Append(" FROM CUSTOMER_CATEGORY_MST_TBL CCAT ");
					sb.Append(" WHERE CCAT.CUSTOMER_CATEGORY_ID = '" + FieldType + "')");
					sb.Append(" AND TMP.CUSTOMER_NAME ='" + strID + "'");
					TempCustCount = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
					if (TempCustCount > 0) {
						sb.Remove(0, sb.Length);
						sb.Append(" SELECT TMP.CUSTOMER_MST_PK FROM TEMP_CUSTOMER_TBL TMP ");
						sb.Append(" WHERE TMP.CUSTOMER_TYPE_FK IN (SELECT CCAT.CUSTOMER_CATEGORY_MST_PK ");
						sb.Append(" FROM CUSTOMER_CATEGORY_MST_TBL CCAT ");
						sb.Append(" WHERE CCAT.CUSTOMER_CATEGORY_ID = '" + FieldType + "')");
						sb.Append(" AND TMP.CUSTOMER_NAME ='" + strID + "'");
						return objWF.ExecuteScaler(sb.ToString());
					} else {
						return 0;
					}
				}
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion
        //Added By Koteshwari on 11/5/2011
        #region "Get Pol PK"
        /// <summary>
        /// Gets the pol pk.
        /// </summary>
        /// <param name="PolId">The pol identifier.</param>
        /// <returns></returns>
        public object GetPolPK(string PolId = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT PMT.PORT_MST_PK FROM PORT_MST_TBL PMT WHERE PMT.PORT_ID = '" + PolId + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Pod PK"
        /// <summary>
        /// Gets the pod pk.
        /// </summary>
        /// <param name="PodId">The pod identifier.</param>
        /// <returns></returns>
        public object GetPodPK(string PodId = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT PMT.PORT_MST_PK FROM PORT_MST_TBL PMT WHERE PMT.PORT_ID = '" + PodId + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Country PK"
        /// <summary>
        /// Gets the country pk.
        /// </summary>
        /// <param name="CountryId">The country identifier.</param>
        /// <returns></returns>
        public object GetCountryPK(string CountryId = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT C.COUNTRY_MST_PK FROM COUNTRY_MST_TBL C WHERE C.COUNTRY_ID =  '" + CountryId + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Operator PK"
        /// <summary>
        /// Gets the operator pk.
        /// </summary>
        /// <param name="OperId">The oper identifier.</param>
        /// <returns></returns>
        public object GetOperatorPK(string OperId = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT OP.OPERATOR_MST_PK FROM OPERATOR_MST_TBL OP WHERE OP.OPERATOR_ID= '" + OperId + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Customer PK"
        /// <summary>
        /// Gets the customer pk.
        /// </summary>
        /// <param name="CustId">The customer identifier.</param>
        /// <returns></returns>
        public object GetCustomerPK(string CustId)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT CMT.CUSTOMER_MST_PK FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_NAME= '" + CustId + "' OR CMT.CUSTOMER_ID= '" + CustId + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Consignee PK"
        /// <summary>
        /// Gets the consignee pk.
        /// </summary>
        /// <param name="ConId">The con identifier.</param>
        /// <returns></returns>
        public DataSet GetConsigneePK(string ConId)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			DataSet ds = null;
			try {
				sb.Append("SELECT CMT.CUSTOMER_MST_PK FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_NAME= '" + ConId + "'");
				ds = objWF.GetDataSet(sb.ToString());
				return ds;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Notify1 PK"
        /// <summary>
        /// Gets the notify1 pk.
        /// </summary>
        /// <param name="Notify1Id">The notify1 identifier.</param>
        /// <returns></returns>
        public object GetNotify1PK(string Notify1Id = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT CMT.CUSTOMER_MST_PK FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_ID= '" + Notify1Id + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Notify1 PK"
        /// <summary>
        /// Gets the agent pk.
        /// </summary>
        /// <param name="AgentId">The agent identifier.</param>
        /// <returns></returns>
        public object GetAgentPK(string AgentId = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT A.AGENT_MST_PK FROM AGENT_MST_TBL A WHERE A.AGENT_REF_NR =  '" + AgentId + "'");
				return (objWF.ExecuteScaler(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        /// <summary>
        /// Gets the recp agent pk.
        /// </summary>
        /// <param name="AgentRefNr">The agent reference nr.</param>
        /// <returns></returns>
        public object GetRecpAgentPK(string AgentRefNr = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append(" SELECT AGT.AGENT_MST_PK  FROM AGENT_MST_TBL AGT");
				sb.Append(" WHERE AGT.AGENT_REF_NR = '" + AgentRefNr + "'");
				sb.Append(" AND AGT.LOCATION_MST_FK IN (SELECT LMT.LOCATION_MST_PK FROM LOCATION_MST_TBL LMT");
				sb.Append(" START WITH LMT.LOCATION_MST_PK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
				sb.Append(" CONNECT BY PRIOR LMT.LOCATION_MST_PK = LMT.REPORTING_TO_FK)");
				return (objWF.ExecuteScaler(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Deleivery Place PK"
        /// <summary>
        /// Gets the delete place.
        /// </summary>
        /// <param name="PlaceId">The place identifier.</param>
        /// <returns></returns>
        public object GetDelPlace(string PlaceId = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT PMT.PLACE_PK,PMT.PLACE_NAME FROM PLACE_MST_TBL PMT WHERE PMT.PLACE_NAME= '" + PlaceId + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Commodity PK"
        /// <summary>
        /// Gets the commodity pk.
        /// </summary>
        /// <param name="CommName">Name of the comm.</param>
        /// <returns></returns>
        public object GetCommodityPK(string CommName = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT COMM.COMMODITY_MST_PK FROM COMMODITY_MST_TBL COMM WHERE UPPER(COMM.COMMODITY_NAME) = UPPER('" + CommName + "')");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get PackType PK"
        /// <summary>
        /// Gets the pack type pk.
        /// </summary>
        /// <param name="PackType">Type of the pack.</param>
        /// <returns></returns>
        public object GetPackTypePK(string PackType = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT PMT.PACK_TYPE_MST_PK  FROM PACK_TYPE_MST_TBL PMT WHERE PMT.PACK_TYPE_ID= '" + PackType + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Freight PK"
        /// <summary>
        /// Gets the freight pk.
        /// </summary>
        /// <param name="FreightId">The freight identifier.</param>
        /// <returns></returns>
        public object GetFreightPK(string FreightId = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT FMT.FREIGHT_ELEMENT_MST_PK FROM FREIGHT_ELEMENT_MST_TBL FMT WHERE FMT.FREIGHT_ELEMENT_ID= '" + FreightId + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Currency PK"
        /// <summary>
        /// Gets the currency pk.
        /// </summary>
        /// <param name="CurrId">The curr identifier.</param>
        /// <returns></returns>
        public object GetCurrencyPK(string CurrId = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT CMT.CURRENCY_MST_PK  FROM CURRENCY_TYPE_MST_TBL CMT  WHERE CMT.CURRENCY_ID= '" + CurrId + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Basis PK"
        /// <summary>
        /// Gets the basis pk.
        /// </summary>
        /// <param name="BasisId">The basis identifier.</param>
        /// <returns></returns>
        public object GetBasisPK(string BasisId = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT DMT.DIMENTION_UNIT_MST_PK  FROM DIMENTION_UNIT_MST_TBL DMT   WHERE DMT.DIMENTION_ID= '" + BasisId + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Voyage PK"
        /// <summary>
        /// Gets the voyage pk.
        /// </summary>
        /// <param name="VesselId">The vessel identifier.</param>
        /// <param name="VoyageId">The voyage identifier.</param>
        /// <returns></returns>
        public object GetVoyagePK(string VesselId = "", string VoyageId = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append(" select v.voyage_trn_pk from vessel_voyage_trn v , vessel_voyage_tbl vm ");
				sb.Append(" where v.vessel_voyage_tbl_fk=vm.vessel_voyage_tbl_pk");
				sb.Append("  and vm.VESSEL_ID='" + VesselId + "'");
				sb.Append("  AND V.VOYAGE='" + VoyageId + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        /// <summary>
        /// Gets the voyage pkwin.
        /// </summary>
        /// <param name="VesselId">The vessel identifier.</param>
        /// <param name="VoyageId">The voyage identifier.</param>
        /// <returns></returns>
        public object GetVoyagePKWIN(string VesselId = "", string VoyageId = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append(" select v.voyage_trn_pk from vessel_voyage_trn v , vessel_voyage_tbl vm ");
				sb.Append(" where v.vessel_voyage_tbl_fk=vm.vessel_voyage_tbl_pk");
				sb.Append("  and vm.VESSEL_NAME='" + VesselId + "'");
				sb.Append("  AND V.VOYAGE='" + VoyageId + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Container PK"
        /// <summary>
        /// Gets the container pk.
        /// </summary>
        /// <param name="ContainerId">The container identifier.</param>
        /// <returns></returns>
        public object GetContainerPK(string ContainerId = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT CMT.CONTAINER_TYPE_MST_PK FROM CONTAINER_TYPE_MST_TBL CMT WHERE CMT.CONTAINER_TYPE_MST_ID= '" + ContainerId + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Get Container Type"
        /// <summary>
        /// Gets the type of the container.
        /// </summary>
        /// <param name="Container_Size">Size of the container_.</param>
        /// <param name="Container_Type">Type of the container_.</param>
        /// <returns></returns>
        public DataSet GetContainerType(string Container_Size = "", string Container_Type = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT C.CONTAINER_TYPE_MST_PK, C.CONTAINER_TYPE_MST_ID FROM CONTAINER_TYPE_MST_TBL C WHERE C.ISO_NUMBER = '" + Container_Size + "" + Container_Type + "'");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch Consol Inv Pk"
        /// <summary>
        /// Checks the inv.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <returns></returns>
        public DataSet CheckInv(Int64 JobPK)
		{
			try {
				System.Text.StringBuilder sb = new System.Text.StringBuilder();
				WorkFlow objWF = new WorkFlow();
				sb.Append(" SELECT COUNT(*) ");
				sb.Append(" FROM (SELECT JF.FREIGHT_ELEMENT_MST_FK");
				sb.Append("  FROM JOB_CARD_SEA_IMP_TBL J, JOB_TRN_SEA_IMP_FD JF");
				sb.Append(" WHERE J.JOB_CARD_SEA_IMP_PK =" + JobPK);
				sb.Append(" AND JF.JOB_CARD_SEA_IMP_FK = J.JOB_CARD_SEA_IMP_PK");
				sb.Append(" AND JF.CONSOL_INVOICE_TRN_FK IS NULL");
				sb.Append(" UNION");
				sb.Append(" SELECT JO.FREIGHT_ELEMENT_MST_FK");
				sb.Append(" FROM JOB_CARD_SEA_IMP_TBL J, JOB_TRN_SEA_IMP_OTH_CHRG JO");
				sb.Append(" WHERE J.JOB_CARD_SEA_IMP_PK = " + JobPK);
				sb.Append(" AND J.JOB_CARD_SEA_IMP_PK = JO.JOB_CARD_SEA_IMP_FK");
				sb.Append(" AND JO.CONSOL_INVOICE_TRN_FK IS NULL) ");
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "Fetch Income and Expense Details"
        /// <summary>
        /// Fetches the sec ser income details.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchSecSerIncomeDetails(string Jobpk)
		{
			WorkFlow objWF = new WorkFlow();
			DataTable dtTotalAmt = new DataTable();
			DataTable dtChargeDet = new DataTable();
			DataSet dsIncomeDet = new DataSet();

			//Parent Details
			try {
				var _with60 = objWF.MyCommand.Parameters;
				_with60.Clear();
				_with60.Add("JOB_CARD_SEA_IMP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
				_with60.Add("BASE_CURRENCY_FK_IN", Convert.ToInt64(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
				_with60.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				dtTotalAmt = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "INCOME_MAIN_SEA_IMP");
			} catch (Exception sqlExp) {
				throw sqlExp;
			}

			//Child Details
			try {
				var _with61 = objWF.MyCommand.Parameters;
				_with61.Clear();
				_with61.Add("JOB_CARD_SEA_IMP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
				_with61.Add("BASE_CURRENCY_FK_IN", Convert.ToInt64(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
				_with61.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				dtChargeDet = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "INCOME_CHILD_SEA_IMP");
			} catch (Exception sqlExp) {
				throw sqlExp;
			}

			try {
				dsIncomeDet.Tables.Add(dtTotalAmt);
				dsIncomeDet.Tables.Add(dtChargeDet);
				dsIncomeDet.Tables[0].TableName = "TOTAL_AMOUNT";
				dsIncomeDet.Tables[1].TableName = "CHARGE_DETAILS";
				var rel_TotAmtAndCharge = new DataRelation("rel1", dsIncomeDet.Tables["TOTAL_AMOUNT"].Columns["FLAG"], dsIncomeDet.Tables["CHARGE_DETAILS"].Columns["FLAG"]);
				dsIncomeDet.Relations.Add(rel_TotAmtAndCharge);
			} catch (Exception ex) {
				throw ex;
			} finally {
				objWF.MyConnection.Close();
			}
			return dsIncomeDet;
		}
        /// <summary>
        /// Fetches the sec ser expense details.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchSecSerExpenseDetails(string Jobpk)
		{
			WorkFlow objWF = new WorkFlow();
			DataTable dtTotalAmt = new DataTable();
			DataTable dtChargeDet = new DataTable();
			DataSet dsExpenseDet = new DataSet();

			//Parent Details
			try {
				var _with62 = objWF.MyCommand.Parameters;
				_with62.Add("JOB_CARD_SEA_IMP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
				_with62.Add("BASE_CURRENCY_FK_IN", Convert.ToInt64(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
				_with62.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				dtTotalAmt = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "EXPENSE_MAIN_SEA_IMP");
			} catch (Exception sqlExp) {
				throw sqlExp;
			} finally {
				objWF.MyConnection.Close();
			}

			//Child Details
			try {
				var _with63 = objWF.MyCommand.Parameters;
				_with63.Clear();
				_with63.Add("JOB_CARD_SEA_IMP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
				_with63.Add("BASE_CURRENCY_FK_IN", Convert.ToInt64(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
				_with63.Add("SEC_SRVC_CURSOR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				dtChargeDet = objWF.GetDataTable("JOBCARD_SEC_SERVICE_PKG", "EXPENSE_CHILD_SEA_IMP");
			} catch (Exception sqlExp) {
				throw sqlExp;
			} finally {
				objWF.MyConnection.Close();
			}

			try {
				dsExpenseDet.Tables.Add(dtTotalAmt);
				dsExpenseDet.Tables.Add(dtChargeDet);
				dsExpenseDet.Tables[0].TableName = "TOTAL_AMOUNT";
				dsExpenseDet.Tables[1].TableName = "CHARGE_DETAILS";
				var rel_TotAmtAndCharge = new DataRelation("rel1", dsExpenseDet.Tables["TOTAL_AMOUNT"].Columns["FLAG"], dsExpenseDet.Tables["CHARGE_DETAILS"].Columns["FLAG"]);
				dsExpenseDet.Relations.Add(rel_TotAmtAndCharge);
			} catch (Exception ex) {
				throw ex;
			}
			return dsExpenseDet;
		}
        #endregion

        #region "JC Close Details"
        /// <summary>
        /// Fetches the jc close detail.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public object FetchJCCloseDetail(string strCond)
		{
			WorkFlow objWF = new WorkFlow();
			OracleCommand cmd = new OracleCommand();
			string strReturn = null;
			string[] arr = null;
			long JobPk = 0;
			arr = strCond.Split(Convert.ToChar("~"));
			JobPk = Convert.ToInt64(arr.GetValue(0));

			try {
				objWF.OpenConnection();
				cmd.Connection = objWF.MyConnection;
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandText = objWF.MyUserName + ".JOB_CLOSE_PKG.JOB_CLOSE_FETCH";
				var _with64 = cmd.Parameters;
				_with64.Add("JOB_PK_IN", JobPk).Direction = ParameterDirection.Input;
				_with64.Add("BIZ_TYPE_IN", arr[1]).Direction = ParameterDirection.Input;
				_with64.Add("PROCESS_TYPE_IN", arr[2]).Direction = ParameterDirection.Input;
				_with64.Add("CURRENCY_PK_IN", Convert.ToInt64(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
				_with64.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
				cmd.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				cmd.ExecuteNonQuery();
				strReturn = Convert.ToString(cmd.Parameters["RETURN_VALUE"].Value).Trim();
				return strReturn;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}

        /// <summary>
        /// Fetches the jc pending activities.
        /// </summary>
        /// <param name="JcPK">The jc pk.</param>
        /// <param name="Biz">The biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchJCPendingActivities(int JcPK, int Biz, int Process)
		{
			WorkFlow objWK = new WorkFlow();
			OracleCommand objCommand = new OracleCommand();
			DataSet dsData = new DataSet();

			try {
				objWK.OpenConnection();
				objWK.MyCommand.Connection = objWK.MyConnection;

				var _with65 = objWK.MyCommand;
				_with65.CommandType = CommandType.StoredProcedure;
				_with65.CommandText = objWK.MyUserName + ".JOB_CLOSE_PKG.JOB_CLOSE_PENDING_ACTIVITIES";

				objWK.MyCommand.Parameters.Clear();
				var _with66 = objWK.MyCommand.Parameters;
				_with66.Add("JOB_PK_IN", JcPK).Direction = ParameterDirection.Input;
				_with66.Add("BIZ_TYPE_IN", Biz).Direction = ParameterDirection.Input;
				_with66.Add("PROCESS_TYPE_IN", Process).Direction = ParameterDirection.Input;
				_with66.Add("CURRENCY_PK_IN", Convert.ToInt64(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
				_with66.Add("JOB_CLOSE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
				objWK.MyDataAdapter.Fill(dsData);
				return dsData;
			} catch (OracleException Oraexp) {
				throw Oraexp;
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        #region "Fetch ErrorLog Format"
        /// <summary>
        /// Fetches the error log format.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchErrorLogFormat()
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append(" SELECT '' JOBNR, '' ERROR_MSG FROM DUAL");
				return (objWF.GetDataSet(sb.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
        #endregion

        #region "Fetch Jobcard Cont PKs for Commodity details Popup"
        /// <summary>
        /// Fetches the job cont p ks.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="Commpk">The commpk.</param>
        /// <returns></returns>
        public DataSet FetchJobContPKs(Int64 JobPK, Int64 Commpk)
		{
			try {
				System.Text.StringBuilder sb = new System.Text.StringBuilder();
				WorkFlow objWF = new WorkFlow();
				sb.Append(" SELECT IMP_CONT.JOB_CARD_SEA_IMP_CONT_PK ");
				sb.Append(" FROM JOB_TRN_SEA_IMP_CONT IMP_CONT ");
				sb.Append(" WHERE IMP_CONT.JOB_CARD_SEA_IMP_FK =" + JobPK);
				//sb.Append(" AND IMP_CONT.COMMODITY_MST_FKS =" & Commpk)
				sb.Append("   AND INSTR(',' || IMP_CONT.COMMODITY_MST_FKS || ',','," + Commpk + ",') > 0 ");

				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        /// <summary>
        /// Fetches the job air cont p ks.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="Commpk">The commpk.</param>
        /// <returns></returns>
        public DataSet FetchJobAirContPKs(Int64 JobPK, Int64 Commpk)
		{
			try {
				System.Text.StringBuilder sb = new System.Text.StringBuilder();
				WorkFlow objWF = new WorkFlow();
				sb.Append(" SELECT IMP_CONT.JOB_TRN_AIR_IMP_CONT_PK ");
				sb.Append(" FROM JOB_TRN_AIR_IMP_CONT IMP_CONT ");
				sb.Append(" WHERE IMP_CONT.JOB_CARD_AIR_IMP_FK =" + JobPK);
				//sb.Append(" AND IMP_CONT.COMMODITY_MST_FKS =" & Commpk)
				sb.Append("   AND INSTR(',' || IMP_CONT.COMMODITY_MST_FKS || ',','," + Commpk + ",') > 0 ");
				return objWF.GetDataSet(sb.ToString());
			} catch (OracleException Oraexp) {
				throw Oraexp;
				//'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
			} catch (Exception ex) {
				throw ex;
			}
		}
        #endregion

        //'For WIN Customer Save 
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
			if (CustCat == 1) {
				Category = "Shipper";
			} else if (CustCat == 2) {
				Category = "Consignee";
			} else {
				Category = "NotifyParty";
			}

			var _with67 = objWK.MyCommand;
			_with67.Parameters.Clear();
			_with67.Transaction = TRAN;
			_with67.CommandType = CommandType.StoredProcedure;
			_with67.CommandText = objWK.MyUserName + ".TEMP_CUSTOMER_TBL_PKG.TEMP_CUSTOMER_TBL_INS";

			//.Parameters.Add("CUSTOMER_ID_IN", DSCust.Tables(0).Rows(0).Item("" & Category & "_ReferenceNumber")).Direction = ParameterDirection.Input
			_with67.Parameters.Add("CUSTOMER_ID_IN", "").Direction = ParameterDirection.Input;
			_with67.Parameters.Add("CUSTOMER_NAME_IN", DSCust.Tables[0].Rows[0]["" + Category + "_Name1"]).Direction = ParameterDirection.Input;
			_with67.Parameters.Add("ACTIVE_FLAG_IN", 1).Direction = ParameterDirection.Input;
			_with67.Parameters.Add("CREDIT_LIMIT_IN", "").Direction = ParameterDirection.Input;
			_with67.Parameters.Add("CREDIT_DAYS_IN", "").Direction = ParameterDirection.Input;
			_with67.Parameters.Add("SECURITY_CHK_REQD_IN", "").Direction = ParameterDirection.Input;
			_with67.Parameters.Add("ACCOUNT_NO_IN", "").Direction = ParameterDirection.Input;
			_with67.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
			_with67.Parameters.Add("BUSINESS_TYPE_IN", Biztype).Direction = ParameterDirection.Input;
			_with67.Parameters.Add("CUSTOMER_TYPE_FK_IN", CustCat).Direction = ParameterDirection.Input;
			//'Newd to save Shipper,Customer
			_with67.Parameters.Add("VAT_NO_IN", "").Direction = ParameterDirection.Input;
			_with67.Parameters.Add("TRANSACTION_TYPE_IN", Transaction_type).Direction = ParameterDirection.Input;
			_with67.Parameters.Add("RECONCILE_STATUS_IN", IsReconciled).Direction = ParameterDirection.Input;
			_with67.Parameters.Add("RECONCILED_BY_IN", "").Direction = ParameterDirection.Input;
			_with67.Parameters.Add("PERMANENT_CUST_MST_FK_IN", "").Direction = ParameterDirection.Input;
			//'Contact Details
			_with67.Parameters.Add("ADM_ADDRESS_1_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine1"]).Direction = ParameterDirection.Input;
			if (DSCust.Tables[0].Columns.Contains("" + Category + "_AddressLine2")) {
				_with67.Parameters.Add("ADM_ADDRESS_2_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine2"]).Direction = ParameterDirection.Input;
			} else {
				_with67.Parameters.Add("ADM_ADDRESS_2_IN", "").Direction = ParameterDirection.Input;
			}
			if (DSCust.Tables[0].Columns.Contains("" + Category + "_AddressLine3")) {
				_with67.Parameters.Add("ADM_ADDRESS_3_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine3"]).Direction = ParameterDirection.Input;
			} else {
				_with67.Parameters.Add("ADM_ADDRESS_3_IN", "").Direction = ParameterDirection.Input;
			}
			if (DSCust.Tables[0].Columns.Contains("" + Category + "_PostalCode")) {
				_with67.Parameters.Add("ADM_ZIP_CODE_IN", DSCust.Tables[0].Rows[0]["" + Category + "_PostalCode"]).Direction = ParameterDirection.Input;
			} else {
				_with67.Parameters.Add("ADM_ZIP_CODE_IN", "").Direction = ParameterDirection.Input;
			}

			_with67.Parameters.Add("ADM_CITY_IN", DSCust.Tables[0].Rows[0]["" + Category + "_City"]).Direction = ParameterDirection.Input;

			if (DSCust.Tables[0].Columns.Contains("" + Category + "_ContactPerson")) {
				_with67.Parameters.Add("ADM_CONTACT_PERSON_IN", DSCust.Tables[0].Rows[0]["" + Category + "_ContactPerson"]).Direction = ParameterDirection.Input;
			} else {
				_with67.Parameters.Add("ADM_CONTACT_PERSON_IN", "").Direction = ParameterDirection.Input;
			}

			if (DSCust.Tables[0].Columns.Contains("" + Category + "_PhoneNumber")) {
				_with67.Parameters.Add("ADM_PHONE_NO_1_IN", DSCust.Tables[0].Rows[0]["" + Category + "_PhoneNumber"]).Direction = ParameterDirection.Input;
			} else {
				_with67.Parameters.Add("ADM_PHONE_NO_1_IN", "").Direction = ParameterDirection.Input;
			}
			if (DSCust.Tables[0].Columns.Contains("" + Category + "_FaxNumber")) {
				_with67.Parameters.Add("ADM_FAX_NO_IN", DSCust.Tables[0].Rows[0]["" + Category + "_FaxNumber"]).Direction = ParameterDirection.Input;
			} else {
				_with67.Parameters.Add("ADM_FAX_NO_IN", "").Direction = ParameterDirection.Input;
			}
			if (DSCust.Tables[0].Columns.Contains("" + Category + "_CellNumber")) {
				_with67.Parameters.Add("ADM_PHONE_NO_2_IN", DSCust.Tables[0].Rows[0]["" + Category + "_CellNumber"]).Direction = ParameterDirection.Input;
			} else {
				_with67.Parameters.Add("ADM_PHONE_NO_2_IN", "").Direction = ParameterDirection.Input;
			}
			if (DSCust.Tables[0].Columns.Contains("" + Category + "_Email")) {
				_with67.Parameters.Add("ADM_EMAIL_ID_IN", DSCust.Tables[0].Rows[0]["" + Category + "_Email"]).Direction = ParameterDirection.Input;
			} else {
				_with67.Parameters.Add("ADM_EMAIL_ID_IN", "").Direction = ParameterDirection.Input;
			}
			//'End
			_with67.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
			_with67.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
			try {
				_with67.ExecuteNonQuery();
				Custpk = Convert.ToInt32(_with67.Parameters["RETURN_VALUE"].Value);
			} catch (Exception ex) {
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
			if (CustCat == 1) {
				Category = "Shipper";
			} else if (CustCat == 2) {
				Category = "Consignee";
			} else {
				Category = "NotifyParty";
			}

			var _with68 = objWK.MyCommand;
			_with68.Parameters.Clear();
			_with68.Transaction = TRAN;
			_with68.CommandType = CommandType.StoredProcedure;
			_with68.CommandText = objWK.MyUserName + ".TEMP_CUSTOMER_TBL_PKG.TEMP_CUSTOMER_TBL_IMP_INS";

			_with68.Parameters.Add("CUSTOMER_ID_IN", DSCust.Tables[0].Rows[0]["" + Category + "_ReferenceNumber"]).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("CUSTOMER_NAME_IN", DSCust.Tables[0].Rows[0]["" + Category + "_Name1"]).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("BUSINESS_TYPE_IN", Biztype).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("TEMP_PATRY_IN", Temp_patry).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("ACCOUNT_NO_IN", "").Direction = ParameterDirection.Input;
			_with68.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("CURRENCY_MST_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("CUSTOMER_TYPE_FK_IN", CustCat).Direction = ParameterDirection.Input;
			//'Newd to save Shipper,Customer
			//'Contact Details
			_with68.Parameters.Add("ADM_ADDRESS_1_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine1"]).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("ADM_ADDRESS_2_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine2"]).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("ADM_ADDRESS_3_IN", DSCust.Tables[0].Rows[0]["" + Category + "_AddressLine3"]).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("ADM_ZIP_CODE_IN", DSCust.Tables[0].Rows[0]["" + Category + "_PostalCode"]).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("ADM_CITY_IN", DSCust.Tables[0].Rows[0]["" + Category + "_City"]).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("ADM_CONTACT_PERSON_IN", DSCust.Tables[0].Rows[0]["" + Category + "_ContactPerson"]).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("ADM_PHONE_NO_1_IN", DSCust.Tables[0].Rows[0]["" + Category + "_PhoneNumber"]).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("ADM_FAX_NO_IN", DSCust.Tables[0].Rows[0]["" + Category + "_FaxNumber"]).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("ADM_PHONE_NO_2_IN", DSCust.Tables[0].Rows[0]["" + Category + "_CellNumber"]).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("ADM_EMAIL_ID_IN", DSCust.Tables[0].Rows[0]["" + Category + "_Email"]).Direction = ParameterDirection.Input;
			//'End
			_with68.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
			_with68.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
			try {
				_with68.ExecuteNonQuery();
				Custpk = Convert.ToInt32(_with68.Parameters["RETURN_VALUE"].Value);
			} catch (Exception ex) {
				return false;
			}
			return true;
		}
        #endregion

        #region "Fetch WIN Details"
        /// <summary>
        /// Fetches the win details.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <param name="BizTYpe">The biz t ype.</param>
        /// <returns></returns>
        public DataSet FetchWinDetails(int Jobpk, int BizTYpe)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			if (BizTYpe == 2) {
				sb.Append("SELECT JSI.JOBCARD_REF_NO,");
				sb.Append("       JSI.WIN_MEM_JOBREF_NR,");
				sb.Append("       (SELECT CMT.CUSTOMER_NAME");
				sb.Append("          FROM CUSTOMER_MST_TBL CMT");
				sb.Append("         WHERE CMT.CUSTOMER_MST_PK = JSI.SHIPPER_CUST_MST_FK) CUST_SHIPPER_NAME,");
				sb.Append("       (SELECT TMT.CUSTOMER_NAME");
				sb.Append("          FROM TEMP_CUSTOMER_TBL TMT");
				sb.Append("         WHERE TMT.CUSTOMER_MST_PK = JSI.SHIPPER_CUST_MST_FK) TEMP_SHIPPER_NAME,");
				sb.Append("       (SELECT CMT.CUSTOMER_NAME");
				sb.Append("          FROM CUSTOMER_MST_TBL CMT");
				sb.Append("         WHERE CMT.CUSTOMER_MST_PK = JSI.CUST_CUSTOMER_MST_FK) CUST_CUSTOMER_NAME,");
				sb.Append("       (SELECT TMT.CUSTOMER_NAME");
				sb.Append("          FROM TEMP_CUSTOMER_TBL TMT");
				sb.Append("         WHERE TMT.CUSTOMER_MST_PK = JSI.CUST_CUSTOMER_MST_FK) TEMP_CUSTOMER_NAME,");
				sb.Append("       JSI.WIN_QUOT_REF_NR,");
				sb.Append("       JSI.WIN_CONSOL_REF_NR,");
				sb.Append("       JSI.WIN_INCO_PLACE,");
				sb.Append("       JSI.RFS_DATE,");
				sb.Append("       JSI.WIN_CUTTOFF_DT,");
				sb.Append("       JSI.WIN_CUTTOFF_TIME,");
				sb.Append("  (SELECT PMT.PORT_NAME FROM PORT_MST_TBL PMT ");
				sb.Append(" WHERE PMT.PORT_MST_PK =JSI.POO_FK) ORIGIN ");
				sb.Append("  FROM JOB_CARD_SEA_IMP_TBL JSI");
				sb.Append(" WHERE JSI.JOB_CARD_SEA_IMP_PK = " + Jobpk);
			} else {
				sb.Append("SELECT JAI.JOBCARD_REF_NO,");
				sb.Append("       JAI.WIN_MEM_JOBREF_NR,");
				sb.Append("       (SELECT CMT.CUSTOMER_NAME");
				sb.Append("          FROM CUSTOMER_MST_TBL CMT");
				sb.Append("         WHERE CMT.CUSTOMER_MST_PK = JAI.SHIPPER_CUST_MST_FK) CUST_SHIPPER_NAME,");
				sb.Append("       (SELECT TMT.CUSTOMER_NAME");
				sb.Append("          FROM TEMP_CUSTOMER_TBL TMT");
				sb.Append("         WHERE TMT.CUSTOMER_MST_PK = JAI.SHIPPER_CUST_MST_FK) TEMP_SHIPPER_NAME,");
				sb.Append("       (SELECT CMT.CUSTOMER_NAME");
				sb.Append("          FROM CUSTOMER_MST_TBL CMT");
				sb.Append("         WHERE CMT.CUSTOMER_MST_PK = JAI.CUST_CUSTOMER_MST_FK) CUST_CUSTOMER_NAME,");
				sb.Append("       (SELECT TMT.CUSTOMER_NAME");
				sb.Append("          FROM TEMP_CUSTOMER_TBL TMT");
				sb.Append("         WHERE TMT.CUSTOMER_MST_PK = JAI.CUST_CUSTOMER_MST_FK) TEMP_CUSTOMER_NAME,");
				sb.Append("       JAI.WIN_QUOT_REF_NR,");
				sb.Append("       JAI.WIN_CONSOL_REF_NR,");
				sb.Append("       JAI.WIN_INCO_PLACE,");
				sb.Append("       JAI.RFS_DATE,");
				sb.Append("       NULL WIN_CUTTOFF_DT,");
				sb.Append("       NULL WIN_CUTTOFF_TIME,");
				sb.Append("  (SELECT PMT.PORT_NAME FROM PORT_MST_TBL PMT ");
				sb.Append("  WHERE PMT.PORT_MST_PK =JAI.WIN_AOO_PK) ORIGIN ");
				sb.Append("  FROM JOB_CARD_AIR_IMP_TBL JAI");
				sb.Append("  WHERE JAI.JOB_CARD_AIR_IMP_PK = " + Jobpk);
			}

			return objWF.GetDataSet(sb.ToString());
		}
        #endregion

        #region "Check WIN For WIN Related Activity"
        /// <summary>
        /// Checks the win jc.
        /// </summary>
        /// <param name="ContPK">The cont pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public int CheckWinJC(int ContPK, int BizType)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			if (BizType == 2) {
				sb.Append(" SELECT COUNT(*) FROM JOB_CARD_SEA_IMP_TBL JSI, JOB_TRN_SEA_IMP_CONT JSICONT ");
				sb.Append(" WHERE JSI.JOB_CARD_SEA_IMP_PK = JSICONT.JOB_CARD_SEA_IMP_FK ");
				sb.Append(" AND JSICONT.JOB_CARD_SEA_IMP_CONT_PK =" + ContPK);
				sb.Append(" AND JSI.WIN_UNIQ_REF_ID IS NOT NULL ");
			} else {
				sb.Append(" SELECT COUNT(*) FROM JOB_CARD_AIR_IMP_TBL JAI, JOB_TRN_AIR_IMP_CONT JAICONT  ");
				sb.Append(" WHERE JAI.JOB_CARD_AIR_IMP_PK = JAICONT.JOB_CARD_AIR_IMP_FK  ");
				sb.Append(" AND JAICONT.JOB_TRN_AIR_IMP_CONT_PK =" + ContPK);
				sb.Append(" AND JAI.WIN_UNIQ_REF_ID IS NOT NULL ");
			}

			return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
		}
        #endregion

        #region "Special Requirement DS"
        /// <summary>
        /// Fetches the SPCL req.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchSpclReq(int Jobpk)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			sb.Append(" SELECT 0 AS JOB_CARD_TRN_CONT_PK,0 CONTAINER_TYPE_MST_FK,");
			sb.Append(" 0 AS COMMODITY_PK, '' AS SPCL_REQ ");
			sb.Append(" FROM DUAL WHERE 1=2 ");
			return objWF.GetDataSet(sb.ToString());
		}

        #endregion

        //'For Win Special Req Save
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
			if (dsSepcialReq.Tables[0].Rows.Count > 0) {
				try {
					for (rowCnt = 0; rowCnt <= dsSepcialReq.Tables[0].Rows.Count - 1; rowCnt++) {
						int CntType = 0;
						CntType = Convert.ToInt32(dsSepcialReq.Tables[0].Rows[rowCnt]["CONTAINER_TYPE_MST_FK"]);
						string strSql = null;
						string drCntKind = null;
						strSql = "SELECT C.CONTAINER_KIND FROM CONTAINER_TYPE_MST_TBL C WHERE C.CONTAINER_TYPE_MST_PK= " + CntType + "";

						var _with69 = objWK.MyCommand;
						_with69.Parameters.Clear();
						_with69.CommandType = CommandType.Text;
						_with69.CommandText = strSql;
						drCntKind = Convert.ToString(_with69.ExecuteScalar());
						objWK.MyCommand.Parameters.Clear();
						if (CommodityGroup == "HAZARDOUS") {
							arrMessage = SaveTransactionHZSpcl(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsSepcialReq.Tables[0].Rows[rowCnt]["SPCL_REQ"], "")), Convert.ToInt64(dsSepcialReq.Tables[0].Rows[rowCnt]["JOB_CARD_TRN_CONT_PK"]));
						} else if (CommodityGroup == "REEFER") {
                            arrMessage = SaveTransactionReefer(objWK.MyCommand, objWK.MyUserName, Convert.ToString(getDefault(dsSepcialReq.Tables[0].Rows[rowCnt]["SPCL_REQ"], "")), Convert.ToInt64(dsSepcialReq.Tables[0].Rows[rowCnt]["JOB_CARD_TRN_CONT_PK"]));
						}
					}
					if (string.Compare(arrMessage[0].ToString(), "saved") > 0) {
						arrMessage.Clear();
						TRAN.Commit();
						arrMessage.Add("All data Saved successfully");
						return arrMessage;
					} else {
						TRAN.Rollback();
						return arrMessage;
					}
				} catch (OracleException oraexp) {
					arrMessage.Add(oraexp.Message);
					return arrMessage;
				} catch (Exception ex) {
					arrMessage.Add(ex.Message);
					return arrMessage;
				}
			}
            return new ArrayList();
			// End
		}
        #endregion

        #region "Check New Record or Exisitng"
        /// <summary>
        /// Checks the new or exist.
        /// </summary>
        /// <param name="UniqRefID">The uniq reference identifier.</param>
        /// <param name="BizTYpe">The biz t ype.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet CheckNewOrExist(string UniqRefID, int BizTYpe, int ProcessType)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			if (BizTYpe == 2 & ProcessType == 1) {
				sb.Append(" SELECT JSE.JOB_CARD_SEA_EXP_PK,JSE.JOBCARD_REF_NO FROM JOB_CARD_SEA_EXP_TBL JSE WHERE UPPER(JSE.WIN_UNIQUE_REF_ID) = UPPER('" + UniqRefID + "')");
			} else if (BizTYpe == 2 & ProcessType == 2) {
				sb.Append("  SELECT JSI.JOB_CARD_SEA_IMP_PK,JSI.JOBCARD_REF_NO,");
				sb.Append(" (SELECT COUNT(*) FROM CAN_MST_TBL CAN WHERE CAN.JOB_CARD_FK = JSI.JOB_CARD_SEA_IMP_PK) CAN_PK ");
				sb.Append(" FROM JOB_CARD_SEA_IMP_TBL JSI WHERE UPPER(JSI.WIN_UNIQ_REF_ID) = UPPER('" + UniqRefID + "')");
			} else if (BizTYpe == 1 & ProcessType == 1) {
				sb.Append(" SELECT JAE.JOB_CARD_AIR_EXP_PK,JAE.JOBCARD_REF_NO FROM JOB_CARD_AIR_EXP_TBL JAE WHERE UPPER(JAE.WIN_UNIQUE_REF_ID) = UPPER('" + UniqRefID + "')");
			} else if (BizTYpe == 1 & ProcessType == 2) {
				sb.Append(" SELECT JAI.JOB_CARD_AIR_IMP_PK,JAI.JOBCARD_REF_NO,");
				sb.Append(" (SELECT COUNT(*) FROM CAN_MST_TBL CAN WHERE CAN.JOB_CARD_FK = JAI.JOB_CARD_AIR_IMP_PK) CAN_PK ");
				sb.Append(" FROM JOB_CARD_AIR_IMP_TBL JAI WHERE UPPER(JAI.WIN_UNIQ_REF_ID) = UPPER('" + UniqRefID + "')");
			}
			return objWF.GetDataSet(sb.ToString());
		}
        #endregion

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
			sb.Append(" SELECT COUNT (*) FROM JOB_CARD_SEA_EXP_TBL JSE,JOB_TRN_SEA_EXP_CONT JSCONT ");
			sb.Append(" WHERE JSE.JOB_CARD_SEA_EXP_PK = JSCONT.JOB_CARD_SEA_EXP_FK");
			sb.Append(" AND JSE.JOB_CARD_SEA_EXP_PK =" + JobPK);
			sb.Append(" AND UPPER(JSCONT.CONTAINER_NUMBER) = UPPER('" + ContNr + "')");
			return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
		}
        #endregion

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
			try {
				//UPDATE JC Fields
				UpdQuery = string.Empty ;
				UpdQuery += " UPDATE JOB_CARD_SEA_EXP_TBL JSE SET JSE.ARRIVAL_DATE = '" + ATADt + "',";
				UpdQuery += " JSE.WIN_REC_QTY =" + RecQty + ",JSE.WIN_REC_WT =" + RecWt;
				UpdQuery += " WHERE JSE.JOB_CARD_SEA_EXP_PK = " + JobPK;
				objWK.ExecuteCommands(UpdQuery);
				arrMessage.Add("All Data Saved Successfully");
			} catch (Exception ex) {
				arrMessage.Add(ex.Message);
			}
			return arrMessage;
		}
        #endregion

        #region "Delete Old Jobacrd and Update Downstream with New JCPK"
        /// <summary>
        /// Deletes the existing job.
        /// </summary>
        /// <param name="OldJCPK">The old JCPK.</param>
        /// <param name="NewJCPK">The new JCPK.</param>
        /// <returns></returns>
        public ArrayList DeleteExistingJob(int OldJCPK, int NewJCPK)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleCommand insCommand = new OracleCommand();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			int NewPK = 0;
			try {
				var _with70 = insCommand;
				_with70.Connection = objWK.MyConnection;
				_with70.CommandType = CommandType.StoredProcedure;
				_with70.CommandText = objWK.MyUserName + ".UDTAE_WIN_JC.UPDATE_JC";
				_with70.Parameters.Clear();
				var _with71 = _with70.Parameters;
				_with71.Add("CSH_PK_IN", OldJCPK).Direction = ParameterDirection.Input;
				_with71.Add("CST_PK_IN", NewJCPK).Direction = ParameterDirection.Input;
				_with71.Add("RETURN_VALUE", OracleDbType.Int32).Direction = ParameterDirection.Output;
				var _with72 = objWK.MyDataAdapter;
				_with72.InsertCommand = insCommand;
				_with72.InsertCommand.Transaction = TRAN;
				_with72.InsertCommand.ExecuteNonQuery();
				NewPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
				if (arrMessage.Count == 0) {
					TRAN.Commit();
					arrMessage.Add("Saved");
					return arrMessage;
				} else {
					TRAN.Rollback();
					return arrMessage;
				}
			} catch (OracleException oraEx) {
				ErrorMessage = oraEx.Message;
				throw oraEx;
			} catch (Exception ex) {
				ErrorMessage = ex.Message;
				throw ex;
			}
		}
        #endregion

        #region "Fetch PickDrop Count"
        /// <summary>
        /// Fetches the pick drop count.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Pick_dropFlg">The pick_drop FLG.</param>
        /// <returns></returns>
        public int FetchPickDropCount(int JobPK, int BizType, int Process, int Pick_dropFlg)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			sb.Append(" SELECT COUNT (*) FROM JOB_PICKUPDROP_TRN PCD WHERE PCD.JOB_TRN_FK = " + JobPK);
			sb.Append(" AND PCD.BIZ_TYPE =" + BizType + " AND PCD.PROCESS_TYPE=" + Process + " AND PCD.PICKUP_DROP_TYPE = " + Pick_dropFlg);
			return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
		}
        /// <summary>
        /// Fetches the pick drop pk.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Pick_dropFlg">The pick_drop FLG.</param>
        /// <returns></returns>
        public int FetchPickDropPK(int JobPK, int BizType, int Process, int Pick_dropFlg)
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			WorkFlow objWF = new WorkFlow();
			sb.Append(" SELECT PICK_DROP_MST_PK FROM JOB_PICKUPDROP_TRN PCD WHERE PCD.JOB_TRN_FK = " + JobPK);
			sb.Append(" AND PCD.BIZ_TYPE =" + BizType + " AND PCD.PROCESS_TYPE=" + Process + " AND PCD.PICKUP_DROP_TYPE = " + Pick_dropFlg);
			return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
		}
        #endregion

        #region "Available Fields"
        /// <summary>
        /// Fetches the avaialble fields.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Cargotype">The cargotype.</param>
        /// <returns></returns>
        public DataSet FetchAvaialbleFields(int BizType = 0, int Cargotype = 0)
		{
			WorkFlow objWF = new WorkFlow();
			DataSet DS = new DataSet();
			try {
				var _with73 = objWF.MyCommand.Parameters;
				_with73.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with73.Add("CARGOTYPE_IN", Cargotype).Direction = ParameterDirection.Input;
				_with73.Add("SELETED_IN", 0).Direction = ParameterDirection.Input;
				_with73.Add("FL_HEADER", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				_with73.Add("FL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				DS = objWF.GetDataSet("JOB_CARD_SEA_IMP_TBL_PKG", "JOBCARD_AVAILABLE_FIELDS");
				DataRelation Rel_Field = new DataRelation("YEAR", new DataColumn[] { DS.Tables[0].Columns["FIELD_TYPE"] }, new DataColumn[] { DS.Tables[1].Columns["FIELD_TYPE"] });

				Rel_Field.Nested = true;
				DS.Relations.Add(Rel_Field);
				return DS;
			} catch (Exception sqlExp) {
				throw sqlExp;
			} finally {
				objWF.MyConnection.Close();
			}
		}

        /// <summary>
        /// Fetches the selected fields.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Cargotype">The cargotype.</param>
        /// <returns></returns>
        public DataSet FetchSelectedFields(int BizType = 0, int Cargotype = 0)
		{
			WorkFlow objWF = new WorkFlow();
			DataSet DS = new DataSet();
			try {
				var _with74 = objWF.MyCommand.Parameters;
				_with74.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
				_with74.Add("CARGOTYPE_IN", Cargotype).Direction = ParameterDirection.Input;
				_with74.Add("SELETED_IN", 1).Direction = ParameterDirection.Input;
				_with74.Add("FL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
				DS = objWF.GetDataSet("JOB_CARD_SEA_IMP_TBL_PKG", "JOBCARD_SEL_FIELDS");
				return DS;
			} catch (Exception sqlExp) {
				throw sqlExp;
			} finally {
				objWF.MyConnection.Close();
			}
		}

        #region "Save Commodity Details"

        /// <summary>
        /// Saves the commodity details.
        /// </summary>
        /// <param name="Jobpk">The jobpk.</param>
        /// <returns></returns>
        public object SaveCommodityDetails(int Jobpk)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleCommand insCommand = new OracleCommand();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			Int32 RowEft = default(Int32);
			try {
				var _with75 = insCommand;
				_with75.Connection = objWK.MyConnection;
				_with75.CommandType = CommandType.StoredProcedure;
				_with75.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JC_COMMODITY_DTL_INS";
				_with75.Parameters.Clear();
				var _with76 = _with75.Parameters;
				_with76.Add("JOB_CARD_SEA_IMP_PK_IN", Jobpk).Direction = ParameterDirection.Input;
				_with76.Add("RETURN_VALUE", OracleDbType.Int32).Direction = ParameterDirection.Output;
				var _with77 = objWK.MyDataAdapter;
				_with77.InsertCommand = insCommand;
				_with77.InsertCommand.Transaction = TRAN;
				RowEft = _with77.InsertCommand.ExecuteNonQuery();
				if (RowEft > 0) {
					TRAN.Commit();
				} else {
					TRAN.Rollback();
				}
			} catch (OracleException oraEx) {
				ErrorMessage = oraEx.Message;
				throw oraEx;
			} catch (Exception ex) {
				ErrorMessage = ex.Message;
				throw ex;
			}
            return new object();
		}

        /// <summary>
        /// Saves the SPL req details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="CntType">Type of the count.</param>
        /// <param name="SplReq">The SPL req.</param>
        /// <returns></returns>
        public object SaveSplReqDetails(int JOBPK, int CntType, string SplReq = "")
		{
			WorkFlow objWK = new WorkFlow();
			WorkFlow objWK1 = new WorkFlow();
			objWK.OpenConnection();
			try {
				Int32 i = default(Int32);
				string strSql = null;
				string strSql1 = null;
				string drCntKind = null;
				DataSet DSCont = new DataSet();
				strSql = "SELECT C.CONTAINER_KIND FROM CONTAINER_TYPE_MST_TBL C WHERE C.CONTAINER_TYPE_MST_PK= " + CntType + "";
				strSql1 = " SELECT JC.JOB_CARD_SEA_IMP_CONT_PK  FROM JOB_TRN_SEA_IMP_CONT JC  WHERE JC.JOB_CARD_SEA_IMP_FK = " + JOBPK;
				DSCont = objWK1.GetDataSet(strSql1.ToString());

				var _with78 = objWK.MyCommand;
				_with78.Parameters.Clear();
				_with78.CommandType = CommandType.Text;
				_with78.CommandText = strSql;
				drCntKind = Convert.ToString(_with78.ExecuteScalar());
				objWK.MyCommand.Parameters.Clear();
				if (DSCont.Tables[0].Rows.Count > 0) {
					for (i = 0; i <= DSCont.Tables[0].Rows.Count - 1; i++) {
						if (CommodityGroup == "HAZARDOUS") {
							if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5") {
								arrMessage = SaveTransactionODC(objWK.MyCommand, objWK.MyUserName, SplReq, Convert.ToInt64(DSCont.Tables[0].Rows[i][0]));
							} else {
								arrMessage = SaveTransactionHZSpcl(objWK.MyCommand, objWK.MyUserName, SplReq, Convert.ToInt64(DSCont.Tables[0].Rows[i][0]));
                            }

						} else if (CommodityGroup == "REEFER") {
							if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5") {
								arrMessage = SaveTransactionODC(objWK.MyCommand, objWK.MyUserName, SplReq, Convert.ToInt64(DSCont.Tables[0].Rows[i][0]));
                            } else {
								arrMessage = SaveTransactionReefer(objWK.MyCommand, objWK.MyUserName, SplReq, Convert.ToInt64(DSCont.Tables[0].Rows[i][0]));
                            }

						} else if (CommodityGroup == "ODC") {
							if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5") {
								arrMessage = SaveTransactionODC(objWK.MyCommand, objWK.MyUserName, SplReq, Convert.ToInt64(DSCont.Tables[0].Rows[i][0]));
                            }
						} else {
							if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5") {
								arrMessage = SaveTransactionODC(objWK.MyCommand, objWK.MyUserName, SplReq, Convert.ToInt64(DSCont.Tables[0].Rows[i][0]));
                            }
						}
					}
				}
			} catch (Exception ex) {
			}
            return new object();
		}
		#endregion
		#endregion
	}
}
