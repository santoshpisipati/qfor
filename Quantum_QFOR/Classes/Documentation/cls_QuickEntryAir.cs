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
//'*  Modified DateTime(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"


using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class ClsQuickEntryAir : CommonFeatures
	{

		Quantum_QFOR.cls_SeaBookingEntry objVesselVoyage = new Quantum_QFOR.cls_SeaBookingEntry();

		#region "Fetch Main Jobcard for export"
		public DataSet FetchMainJobCardData(string MJCPK = "0", Int32 CurrentPage = 0, Int32 TotalPage = 0)
		{
			StringBuilder strSQL = new StringBuilder();
			WorkFlow objWF = new WorkFlow();
			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);

			strSQL.Append("SELECT");
			strSQL.Append("    job_imp.JOB_CARD_TRN_PK JOB_CARD_TRN_PK,");
			strSQL.Append("    job_imp.jobcard_ref_no,");
			strSQL.Append("    job_imp.jobcard_date,");
			strSQL.Append("    job_imp.job_card_status,");
			strSQL.Append("    job_imp.job_card_closed_on,");
			strSQL.Append("    job_imp.remarks,");
			strSQL.Append("    job_imp.cust_customer_mst_fk,");
			strSQL.Append("    cust.customer_id \"customer_id\",");
			strSQL.Append("    cust.customer_name \"customer_name\",");
			strSQL.Append("    del_place_mst_fk,");
			strSQL.Append("    upper(del_place.PLACE_CODE)\"PLACECODE\",");
			//'
			strSQL.Append("    upper(del_place.place_name) \"DeliveryPlace\",");
			strSQL.Append("    port_mst_pol_fk,");
			strSQL.Append("    pol.port_id \"POL\",");
			strSQL.Append("    pol.PORT_NAME \"POLNAME\",");
			//'
			strSQL.Append("    port_mst_pod_fk,");
			strSQL.Append("    pod.port_id \"POD\",");
			strSQL.Append("    pod.PORT_NAME \"PODNAME\",");
			//'
			strSQL.Append("    job_imp.CARRIER_MST_FK airline_mst_fk,");
			strSQL.Append("    airline.airline_id \"airline_id\",");
			strSQL.Append("    airline.airline_name \"airline_name\",");
			strSQL.Append("    JOB_IMP.VOYAGE_FLIGHT_NO flight_no ,");
			strSQL.Append("    job_imp.AIRLINE_SCHEDULE_TRN_FK ,");
			strSQL.Append("    eta_date,");
			strSQL.Append("    etd_date,");
			strSQL.Append("    arrival_date,");
			strSQL.Append("    departure_date,");
			strSQL.Append("    shipper_cust_mst_fk,");
			strSQL.Append("    shipper.customer_id \"Shipper\",");
			strSQL.Append("    shipper.customer_name \"ShipperName\",");
			strSQL.Append("    consignee_cust_mst_fk,");
			strSQL.Append("    consignee.customer_id \"Consignee\",");
			strSQL.Append("    consignee.customer_name \"ConsigneeName\",");
			strSQL.Append("    notify1_cust_mst_fk,");
			strSQL.Append("    notify1.customer_id \"Notify1\",");
			strSQL.Append("    notify1.customer_name \"Notify1Name\",");
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
			strSQL.Append("    JOB_IMP.HBL_HAWB_REF_NO hawb_ref_no,");
			strSQL.Append("    JOB_IMP.HBL_HAWB_DATE hawb_date,");
			strSQL.Append("    JOB_IMP.MBL_MAWB_REF_NO mawb_ref_no,");
			strSQL.Append("    JOB_IMP.MBL_MAWB_DATE mawb_date,");
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
			//strSQL.Append(vbCrLf & "    job_imp.commodity_group_fk,")
			strSQL.Append("    comm.commodity_group_desc,");
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
			strSQL.Append("    job_imp.JC_AUTO_MANUAL , JOB_IMP.HBL_HAWB_SURRDT HAWBSURRDT,JOB_IMP.MBL_MAWB_SURRDT MAWBSURRDT, ");
			//
			strSQL.Append("    job_imp.sb_number,job_imp.sb_date, ");
			strSQL.Append("    curr.currency_id,job_imp.page_nr,");
			strSQL.Append("    curr.currency_mst_pk base_currency_fk,");
			strSQL.Append("    job_imp.LC_SHIPMENT,");
			strSQL.Append("    job_imp.Lc_Number,");
			strSQL.Append("    job_imp.Lc_Date,");
			strSQL.Append("    job_imp.Lc_Expires_On,");
			strSQL.Append("    job_imp.Lc_Cons_Bank,");
			strSQL.Append("    job_imp.Lc_Remarks,");
			strSQL.Append("    job_imp.Bro_Received,");
			strSQL.Append("    job_imp.Bro_Number,");
			strSQL.Append("    job_imp.Bro_Date,");
			strSQL.Append("    job_imp.Bro_Issuedby,");
			strSQL.Append("    job_imp.Bro_Remarks, ");
			strSQL.Append("    NVL(job_imp.CHK_CSR,1) CHK_CSR,");
			strSQL.Append("    NVL(EMP.EMPLOYEE_MST_PK,NVL(CONS_SE.EMPLOYEE_MST_PK,0)) SALES_EXEC_FK,");
			strSQL.Append("    NVL(EMP.EMPLOYEE_ID,CONS_SE.EMPLOYEE_ID) SALES_EXEC_ID,");
			strSQL.Append("    NVL(EMP.EMPLOYEE_NAME,CONS_SE.EMPLOYEE_NAME) SALES_EXEC_NAME ");
			strSQL.Append("  FROM");
			strSQL.Append("    JOB_CARD_TRN job_imp,");
			strSQL.Append("    port_mst_tbl POD,");
			strSQL.Append("    port_mst_tbl POL,");
			strSQL.Append("    customer_mst_tbl cust,");
			strSQL.Append("    customer_mst_tbl consignee,");
			strSQL.Append("    customer_mst_tbl shipper,");
			strSQL.Append("    customer_mst_tbl notify1,");
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
			strSQL.Append("    EMPLOYEE_MST_TBL        CONS_SE ");
			//CONSIGNEE SALES PERSON
			strSQL.Append("    WHERE");
			//strSQL.Append(vbCrLf & "    job_imp.JOB_CARD_TRN_PK          = " + jobCardPK)
			strSQL.Append("    job_imp.MASTER_JC_FK          = " + MJCPK);
			strSQL.Append("    AND job_imp.port_mst_pol_fk          =  pol.port_mst_pk");
			strSQL.Append("    AND job_imp.port_mst_pod_fk          =  pod.port_mst_pk");
			strSQL.Append("    AND job_imp.del_place_mst_fk         =  del_place.place_pk(+)");
			strSQL.Append("    AND job_imp.cust_customer_mst_fk     =  cust.customer_mst_pk(+) ");
			strSQL.Append("    AND job_imp.CARRIER_MST_FK           =  airline.airline_mst_pk");
			strSQL.Append("    AND job_imp.shipper_cust_mst_fk      =  shipper.customer_mst_pk(+)");
			strSQL.Append("    AND job_imp.consignee_cust_mst_fk    =  consignee.customer_mst_pk(+)");
			strSQL.Append("    AND job_imp.notify1_cust_mst_fk      =  notify1.customer_mst_pk(+)");
			strSQL.Append("    AND job_imp.Notify2_Cust_Mst_Fk      =  notify2.customer_mst_pk(+)");
			strSQL.Append("    AND job_imp.cl_agent_mst_fk          =  clagnt.agent_mst_pk(+)");
			strSQL.Append("    AND job_imp.Cb_Agent_Mst_Fk          =  cbagnt.agent_mst_pk(+)");
			strSQL.Append("    AND job_imp.pol_agent_mst_fk         =  polagnt.agent_mst_pk(+)");
			strSQL.Append("    AND job_imp.commodity_group_fk       =  comm.commodity_group_pk(+)");
			strSQL.Append("    AND job_imp.transporter_depot_fk     =  depot.vendor_mst_pk(+)");
			strSQL.Append("    AND job_imp.transporter_carrier_fk   =  carrier.vendor_mst_pk(+)");
			strSQL.Append("    AND job_imp.country_origin_fk        =  country.country_mst_pk(+)");
			strSQL.Append("    AND curr.currency_mst_pk(+) = job_imp.BASE_CURRENCY_MST_FK");
			strSQL.Append("    AND consignee.REP_EMP_MST_FK=CONS_SE.EMPLOYEE_MST_PK(+) ");
			strSQL.Append("    AND JOB_IMP.EXECUTIVE_MST_FK=EMP.EMPLOYEE_MST_PK(+) ");
			strSQL.Append("    AND JOB_IMP.BUSINESS_TYPE=1 ");
			strSQL.Append("    AND JOB_IMP.PROCESS_TYPE=2 ");

			Int16 RecPerPage = 1;
			TotalRecords = TotalPage;
			TotalPage = TotalRecords / RecPerPage;
			if (TotalRecords % RecPerPage != 0) {
				TotalPage += 1;
			}

			if (CurrentPage > TotalPage)
				CurrentPage = 1;
			if (TotalRecords == 0)
				CurrentPage = 0;
			last = CurrentPage * RecPerPage;
			start = (CurrentPage - 1) * RecPerPage + 1;

			System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
			sqlstr2.Append(" SELECT * FROM ");
			sqlstr2.Append("  ( SELECT ROWNUM SR_NO, Q.* FROM ");
			sqlstr2.Append("  (" + strSQL.ToString() + " ");
			//sqlstr2.Append("  ) Q )  WHERE ""SR_NO""  BETWEEN " & start & " AND " & last & "")
			sqlstr2.Append("  ) Q )  WHERE PAGE_NR =" + start + "");

			try {
				return objWF.GetDataSet(sqlstr2.ToString());
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
		public DataSet GetContainerData(string jobCardPK = "0")
		{
			WorkFlow objWF = new WorkFlow();
			StringBuilder SQL = new StringBuilder();
			SQL.Append("SELECT ");
			SQL.Append("      JOB_TRN_CONT_PK job_trn_air_imp_cont_pk,");
			SQL.Append("      palette_size,");
			//SQL.Append(vbCrLf & "      commodity_name,")
			SQL.Append("      CASE WHEN CONT_TRN.COMMODITY_MST_FKS IS NOT NULL THEN ");
			SQL.Append("      ROWTOCOL('SELECT COM.COMMODITY_NAME FROM COMMODITY_MST_TBL COM WHERE COM.COMMODITY_MST_PK IN (' || CONT_TRN.COMMODITY_MST_FKS || ')') ");
			SQL.Append("      ELSE COMM.COMMODITY_NAME END COMMODITY_NAME, ");
			SQL.Append("      pack_type_mst_fk,");
			SQL.Append("      pack_count,");
			SQL.Append("      gross_weight,");
			SQL.Append("      volume_in_cbm,");
			SQL.Append("      chargeable_weight,");
			SQL.Append("      to_char(load_date,dateformat) load_date,");
			//SQL.Append(vbCrLf & "      commodity_mst_fk,'false' as ""Delete"" ")
			SQL.Append("      CASE WHEN CONT_TRN.COMMODITY_MST_FKS IS NOT NULL THEN TO_NUMBER(CONT_TRN.COMMODITY_MST_FKS) ELSE COMMODITY_MST_FK END COMMODITY_MST_FK, 'false' as \"Delete\" ");
			SQL.Append("FROM");
			SQL.Append("      JOB_TRN_CONT cont_trn,");
			SQL.Append("      JOB_CARD_TRN job_card,");
			SQL.Append("      pack_type_mst_tbl pack,");
			SQL.Append("      commodity_mst_tbl comm");
			SQL.Append("WHERE");
			SQL.Append("      cont_trn.JOB_CARD_TRN_FK =" + jobCardPK);
			SQL.Append("      AND cont_trn.JOB_CARD_TRN_FK = job_card.JOB_CARD_TRN_PK");
			SQL.Append("      AND cont_trn.pack_type_mst_fk = pack.pack_type_mst_pk(+)");
			SQL.Append("      AND cont_trn.commodity_mst_fk = comm.commodity_mst_pk(+)");
			SQL.Append("      AND JOB_CARD.BUSINESS_TYPE=1 ");
			SQL.Append("      AND JOB_CARD.PROCESS_TYPE=2 ");
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

		#region "Get Freight Data"
		public DataSet GetFreightData(string jobCardPK = "0")
		{
			WorkFlow objWF = new WorkFlow();
			StringBuilder SQL = new StringBuilder();
			SQL.Append("SELECT");
			SQL.Append("   JOB_TRN_FD_PK job_trn_air_imp_fd_pk,");
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
			SQL.Append("   EXCHANGE_RATE \"ROE\",");
			SQL.Append("  (fd_trn.freight_amt*fd_trn.EXCHANGE_RATE) total_amt,");
			SQL.Append("   '0' \"Delete\"");
			SQL.Append("FROM");
			SQL.Append("   JOB_TRN_FD fd_trn,");
			SQL.Append("   JOB_CARD_TRN job_card,");
			SQL.Append("   currency_type_mst_tbl curr,");
			SQL.Append("    parameters_tbl prm,");
			SQL.Append("   freight_element_mst_tbl frt,");
			SQL.Append("   location_mst_tbl loc,");
			SQL.Append("   customer_mst_tbl cus");
			SQL.Append("WHERE");
			SQL.Append("   fd_trn.JOB_CARD_TRN_FK = " + jobCardPK);
			SQL.Append("   AND fd_trn.JOB_CARD_TRN_FK = job_card.JOB_CARD_TRN_PK");
			SQL.Append("   AND fd_trn.currency_mst_fk = curr.currency_mst_pk(+)");
			SQL.Append("   AND fd_trn.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
			SQL.Append("   AND fd_trn.freight_element_mst_fk = prm.frt_afc_fk");
			SQL.Append("   AND fd_trn.frtpayer_cust_mst_fk = cus.customer_mst_pk(+)");
			SQL.Append("   AND fd_trn.location_mst_fk = loc.location_mst_pk(+)");
			SQL.Append("   AND JOB_CARD.BUSINESS_TYPE=1 ");
			SQL.Append("   AND JOB_CARD.PROCESS_TYPE=2 ");
			SQL.Append(" union all ");
			SQL.Append("SELECT");
			SQL.Append("   JOB_TRN_FD_PK job_trn_air_imp_fd_pk,");
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
			SQL.Append("   EXCHANGE_RATE \"ROE\",");
			SQL.Append("  (fd_trn.freight_amt*fd_trn.EXCHANGE_RATE) total_amt,");
			SQL.Append("   '0' \"Delete\"");
			SQL.Append("FROM");
			SQL.Append("   JOB_TRN_FD fd_trn,");
			SQL.Append("   JOB_CARD_TRN job_card,");
			SQL.Append("   currency_type_mst_tbl curr,");
			SQL.Append("    parameters_tbl prm,");
			SQL.Append("   freight_element_mst_tbl frt,");
			SQL.Append("   location_mst_tbl loc,");
			SQL.Append("   customer_mst_tbl cus");
			SQL.Append("WHERE");
			SQL.Append("   fd_trn.JOB_CARD_TRN_FK = " + jobCardPK);
			SQL.Append("   AND fd_trn.JOB_CARD_TRN_FK = job_card.JOB_CARD_TRN_PK");
			SQL.Append("   AND fd_trn.currency_mst_fk = curr.currency_mst_pk(+)");
			SQL.Append("   AND fd_trn.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
			SQL.Append("   AND fd_trn.freight_element_mst_fk not in prm.frt_afc_fk");
			SQL.Append("   AND fd_trn.frtpayer_cust_mst_fk = cus.customer_mst_pk(+)");
			SQL.Append("   AND fd_trn.location_mst_fk = loc.location_mst_pk(+)");
			SQL.Append("   AND JOB_CARD.BUSINESS_TYPE=1 ");
			SQL.Append("   AND JOB_CARD.PROCESS_TYPE=2 ");
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

		#region "Get OtherCharges Data"
		public DataSet FillJobCardOtherChargesDataSet(string pk = "0")
		{
			System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			strSQL.Append("         SELECT");
			strSQL.Append("         oth_chrg.JOB_TRN_OTH_PK job_trn_air_imp_oth_pk,");
			strSQL.Append("         frt.freight_element_mst_pk,");
			strSQL.Append("         frt.freight_element_id,");
			strSQL.Append("         frt.freight_element_name,");
			strSQL.Append("         curr.currency_mst_pk,");
			strSQL.Append("         DECODE(oth_chrg.freight_type,1,'Prepaid',2,'Collect') PaymentType, ");
			strSQL.Append("   oth_chrg.location_mst_fk  \"location_fk\" ,");
			strSQL.Append("   loc.location_id \"location_id\" ,");
			strSQL.Append("   oth_chrg.frtpayer_cust_mst_fk \"frtpayer_mst_fk\" ,");
			strSQL.Append("   cus.customer_id \"frtpayer\",");
			strSQL.Append("         oth_chrg.EXCHANGE_RATE \"ROE\",");
			strSQL.Append("         oth_chrg.amount amount,");
			strSQL.Append("         'false' \"Delete\"");
			strSQL.Append("FROM");
			strSQL.Append("         JOB_TRN_OTH_CHRG oth_chrg,");
			strSQL.Append("         JOB_CARD_TRN jobcard_mst,");
			strSQL.Append("         freight_element_mst_tbl frt,");
			strSQL.Append("         currency_type_mst_tbl curr,");
			strSQL.Append("   location_mst_tbl loc,");
			strSQL.Append("   customer_mst_tbl cus");
			strSQL.Append("WHERE");
			strSQL.Append("         oth_chrg.JOB_CARD_TRN_FK = jobcard_mst.JOB_CARD_TRN_PK");
			strSQL.Append("         AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
			strSQL.Append("         AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
			strSQL.Append("         AND oth_chrg.JOB_CARD_TRN_FK    = " + pk);
			strSQL.Append("   AND oth_chrg.frtpayer_cust_mst_fk = cus.customer_mst_pk(+) ");
			strSQL.Append("   AND oth_chrg.location_mst_fk = loc.location_mst_pk(+) ");
			strSQL.Append("   AND JOBCARD_MST.BUSINESS_TYPE=1 ");
			strSQL.Append("   AND JOBCARD_MST.PROCESS_TYPE=2 ");
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
		#endregion

		#region "Save Function"
		public ArrayList Save(DataSet M_DataSet, DataSet dsContainerData, DataSet dsFreightDetails, bool isEdting, string jobCardRefNumber, string userLocation, string employeeID, long JobCardPK, DataSet dsOtherCharges, int PageNr,
		long MasterJCPk)
		{
			WorkFlow objWK = new WorkFlow();
			objWK.OpenConnection();
			OracleTransaction TRAN = null;
			TRAN = objWK.MyConnection.BeginTransaction();
			arrMessage.Clear();
			objVesselVoyage.ConfigurationPK = M_Configuration_PK;
			objVesselVoyage.CREATED_BY = M_CREATED_BY_FK;
			Int32 RecAfct = default(Int32);
			Int16 intIns = default(Int16);
			string str = null;

			OracleCommand insCommand = new OracleCommand();
			OracleCommand updCommand = new OracleCommand();

			OracleCommand insContainerDetails = new OracleCommand();
			OracleCommand updContainerDetails = new OracleCommand();
			OracleCommand delContainerDetails = new OracleCommand();

			OracleCommand insFreightDetails = new OracleCommand();
			OracleCommand updFreightDetails = new OracleCommand();
			OracleCommand delFreightDetails = new OracleCommand();

			OracleCommand insOtherChargesDetails = new OracleCommand();
			OracleCommand updOtherChargesDetails = new OracleCommand();
			OracleCommand delOtherChargesDetails = new OracleCommand();
			try {
				var _with1 = insCommand;
				_with1.Connection = objWK.MyConnection;
				_with1.CommandType = CommandType.StoredProcedure;
				_with1.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_CARD_AIR_IMP_TBL_INS";
				var _with2 = _with1.Parameters;
				insCommand.Parameters.Add("JOBCARD_REF_NO_IN", jobCardRefNumber).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("JOBCARD_DATE_IN", OracleDbType.Date, 20, "jobcard_date").Direction = ParameterDirection.Input;

				insCommand.Parameters["JOBCARD_DATE_IN"].SourceVersion = DataRowVersion.Current;
				insCommand.Parameters.Add("JOB_CARD_STATUS_IN", 1).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("JOB_CARD_CLOSED_ON_IN", OracleDbType.Date, 20, "job_card_closed_on").Direction = ParameterDirection.Input;
				insCommand.Parameters["JOB_CARD_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "remarks").Direction = ParameterDirection.Input;
				insCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "cust_customer_mst_fk").Direction = ParameterDirection.Input;
				insCommand.Parameters["CUST_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

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

				insCommand.Parameters.Add("DA_NUMBER_IN", OracleDbType.Int32, 10, "da_number").Direction = ParameterDirection.Input;
				insCommand.Parameters["DA_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "commodity_group_fk").Direction = ParameterDirection.Input;
				insCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CLEARANCE_ADDRESS_IN", OracleDbType.Varchar2, 200, "clearance_address").Direction = ParameterDirection.Input;
				insCommand.Parameters["CLEARANCE_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("HAWBSURRDT_IN", OracleDbType.Date, 20, "HAWBSURRDT").Direction = ParameterDirection.Input;
				insCommand.Parameters["HAWBSURRDT_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("MAWBSURRDT_IN", OracleDbType.Date, 20, "MAWBSURRDT").Direction = ParameterDirection.Input;
				insCommand.Parameters["MAWBSURRDT_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CONFIG_PK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("BASE_CURRENCY_FK_IN", OracleDbType.Int32, 10, "base_currency_fk").Direction = ParameterDirection.Input;
				insCommand.Parameters["BASE_CURRENCY_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("SB_DATE_IN", OracleDbType.Date, 20, "sb_date").Direction = ParameterDirection.Input;
				insCommand.Parameters["SB_DATE_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("SB_NO_IN", OracleDbType.Varchar2, 20, "sb_number").Direction = ParameterDirection.Input;
				insCommand.Parameters["SB_NO_IN"].SourceVersion = DataRowVersion.Current;
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

				insCommand.Parameters.Add("CRQ_DATE_IN", "").Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("CRQ_IN", "").Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("PO_NUMBER_IN", "").Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("PO_DATE_IN", "").Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("ROUTING_INST_IN", "").Direction = ParameterDirection.Input;
				insCommand.Parameters.Add("SENT_ON_IN", "").Direction = ParameterDirection.Input;

				//nomination parameters
				insCommand.Parameters.Add("CHK_CSR_IN", OracleDbType.Int32, 1, "CHK_CSR").Direction = ParameterDirection.Input;
				insCommand.Parameters["CHK_CSR_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("EXECUTIVE_MST_FK_IN", OracleDbType.Int32, 10, "SALES_EXEC_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["EXECUTIVE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
				//-----------------------------------------------------------------------------------
				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_CARD_TRN_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;



				var _with3 = updCommand;
				_with3.Connection = objWK.MyConnection;
				_with3.CommandType = CommandType.StoredProcedure;
				_with3.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_CARD_AIR_IMP_TBL_UPD";
				var _with4 = _with3.Parameters;

				updCommand.Parameters.Add("JOB_CARD_AIR_IMP_PK_IN", OracleDbType.Int32, 10, "JOB_CARD_TRN_PK").Direction = ParameterDirection.Input;
				updCommand.Parameters["JOB_CARD_AIR_IMP_PK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("JOBCARD_REF_NO_IN", jobCardRefNumber).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("JOBCARD_DATE_IN", OracleDbType.Date, 20, "jobcard_date").Direction = ParameterDirection.Input;
				updCommand.Parameters["JOBCARD_DATE_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("JOB_CARD_STATUS_IN", OracleDbType.Int32, 1, "job_card_status").Direction = ParameterDirection.Input;
				updCommand.Parameters["JOB_CARD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("JOB_CARD_CLOSED_ON_IN", OracleDbType.Date, 20, "job_card_closed_on").Direction = ParameterDirection.Input;
				updCommand.Parameters["JOB_CARD_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "remarks").Direction = ParameterDirection.Input;
				updCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "cust_customer_mst_fk").Direction = ParameterDirection.Input;
				updCommand.Parameters["CUST_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

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

				updCommand.Parameters.Add("DA_NUMBER_IN", OracleDbType.Int32, 10, "da_number").Direction = ParameterDirection.Input;
				updCommand.Parameters["DA_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "commodity_group_fk").Direction = ParameterDirection.Input;
				updCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("CLEARANCE_ADDRESS_IN", OracleDbType.Varchar2, 200, "clearance_address").Direction = ParameterDirection.Input;
				updCommand.Parameters["CLEARANCE_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("HAWBSURRDT_IN", OracleDbType.Date, 20, "HAWBSURRDT").Direction = ParameterDirection.Input;
				updCommand.Parameters["HAWBSURRDT_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("MAWBSURRDT_IN", OracleDbType.Date, 20, "MAWBSURRDT").Direction = ParameterDirection.Input;
				updCommand.Parameters["MAWBSURRDT_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("SB_DATE_IN", OracleDbType.Date, 20, "sb_date").Direction = ParameterDirection.Input;
				updCommand.Parameters["SB_DATE_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("SB_NO_IN", OracleDbType.Varchar2, 20, "sb_number").Direction = ParameterDirection.Input;
				updCommand.Parameters["SB_NO_IN"].SourceVersion = DataRowVersion.Current;

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

				updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

				updCommand.Parameters.Add("CRQ_DATE_IN", "").Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("CRQ_IN", "").Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("PO_NUMBER_IN", "").Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("PO_DATE_IN", "").Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("ROUTING_INST_IN", "").Direction = ParameterDirection.Input;
				updCommand.Parameters.Add("SENT_ON_IN", "").Direction = ParameterDirection.Input;

				//nomination parameters
				updCommand.Parameters.Add("CHK_CSR_IN", OracleDbType.Int32, 1, "CHK_CSR").Direction = ParameterDirection.Input;
				updCommand.Parameters["CHK_CSR_IN"].SourceVersion = DataRowVersion.Current;

				updCommand.Parameters.Add("EXECUTIVE_MST_FK_IN", OracleDbType.Int32, 10, "SALES_EXEC_FK").Direction = ParameterDirection.Input;
				updCommand.Parameters["EXECUTIVE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
				//------------------------------------------------------------------------------------
				updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
				var _with5 = objWK.MyDataAdapter;
				_with5.InsertCommand = insCommand;
				_with5.InsertCommand.Transaction = TRAN;

				_with5.UpdateCommand = updCommand;
				_with5.UpdateCommand.Transaction = TRAN;

				RecAfct = _with5.Update(M_DataSet.Tables[0]);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					if (isEdting == false) {
						RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
						//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
					}
					return arrMessage;
				} else {
					if (isEdting == false) {
						JobCardPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
					}
				}
				///
				if (JobCardPK != 0) {
					OracleCommand updCmdUser = new OracleCommand();
					updCmdUser.Transaction = TRAN;
					str = " update JOB_CARD_TRN job set job.page_nr=" + PageNr;
					str += " ,job.CONSOLE = 2";
					if (MasterJCPk != 0) {
						str += " ,job.MASTER_JC_FK=" + MasterJCPk;
					}
					str += " WHERE job.JOB_CARD_TRN_PK=" + JobCardPK;
					var _with6 = updCmdUser;
					_with6.Connection = objWK.MyConnection;
					_with6.Transaction = TRAN;
					_with6.CommandType = CommandType.Text;
					_with6.CommandText = str;
					intIns = Convert.ToInt16(_with6.ExecuteNonQuery());
				}
				///
				var _with7 = insContainerDetails;
				_with7.Connection = objWK.MyConnection;
				_with7.CommandType = CommandType.StoredProcedure;
				_with7.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_CONT_INS";
				var _with8 = _with7.Parameters;
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

				insContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "commodity_mst_fk").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "load_date").Direction = ParameterDirection.Input;
				insContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

				insContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_IMP_CONT_PK").Direction = ParameterDirection.Output;
				insContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with9 = updContainerDetails;
				_with9.Connection = objWK.MyConnection;
				_with9.CommandType = CommandType.StoredProcedure;
				_with9.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_CONT_UPD";
				var _with10 = _with9.Parameters;

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

				updContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", OracleDbType.Int32, 10, "commodity_mst_fk").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "load_date").Direction = ParameterDirection.Input;
				updContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

				updContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				updContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with11 = delContainerDetails;
				_with11.Connection = objWK.MyConnection;
				_with11.CommandType = CommandType.StoredProcedure;
				_with11.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_CONT_DEL";

				delContainerDetails.Parameters.Add("JOB_TRN_AIR_IMP_CONT_PK_IN", OracleDbType.Int32, 10, "job_trn_air_imp_cont_pk").Direction = ParameterDirection.Input;
				delContainerDetails.Parameters["JOB_TRN_AIR_IMP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

				delContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				delContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


				var _with12 = objWK.MyDataAdapter;

				_with12.InsertCommand = insContainerDetails;
				_with12.InsertCommand.Transaction = TRAN;

				_with12.UpdateCommand = updContainerDetails;
				_with12.UpdateCommand.Transaction = TRAN;

				_with12.DeleteCommand = delContainerDetails;
				_with12.DeleteCommand.Transaction = TRAN;

				RecAfct = _with12.Update(dsContainerData.Tables[0]);

				if (arrMessage.Count > 0) {
					if (isEdting == false) {
						RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
						//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
					}
					TRAN.Rollback();
					return arrMessage;
				}

				var _with13 = insFreightDetails;
				_with13.Connection = objWK.MyConnection;
				_with13.CommandType = CommandType.StoredProcedure;
				_with13.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_FD_INS";
				var _with14 = _with13.Parameters;

				insFreightDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

				insFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
				insFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
				insFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

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


				var _with15 = updFreightDetails;
				_with15.Connection = objWK.MyConnection;
				_with15.CommandType = CommandType.StoredProcedure;
				_with15.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_FD_UPD";
				var _with16 = _with15.Parameters;

				updFreightDetails.Parameters.Add("JOB_TRN_AIR_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_fd_pk").Direction = ParameterDirection.Input;
				updFreightDetails.Parameters["JOB_TRN_AIR_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

				updFreightDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

				updFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
				updFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
				updFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

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

				var _with17 = delFreightDetails;
				_with17.Connection = objWK.MyConnection;
				_with17.CommandType = CommandType.StoredProcedure;
				_with17.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_FD_DEL";

				delFreightDetails.Parameters.Add("JOB_TRN_AIR_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_fd_pk").Direction = ParameterDirection.Input;
				delFreightDetails.Parameters["JOB_TRN_AIR_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

				delFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				delFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with18 = objWK.MyDataAdapter;

				_with18.InsertCommand = insFreightDetails;
				_with18.InsertCommand.Transaction = TRAN;

				_with18.UpdateCommand = updFreightDetails;
				_with18.UpdateCommand.Transaction = TRAN;

				_with18.DeleteCommand = delFreightDetails;
				_with18.DeleteCommand.Transaction = TRAN;

				RecAfct = _with18.Update(dsFreightDetails);

				if (arrMessage.Count > 0) {
					if (isEdting == false) {
                        RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
						//Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
					}
					TRAN.Rollback();
					return arrMessage;
				}

				var _with19 = insOtherChargesDetails;
				_with19.Connection = objWK.MyConnection;
				_with19.CommandType = CommandType.StoredProcedure;
				_with19.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_OTH_CHRG_INS";
				var _with20 = _with19.Parameters;
				insOtherChargesDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

				insOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
				insOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
				insOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insOtherChargesDetails.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "PaymentType").Direction = ParameterDirection.Input;
				insOtherChargesDetails.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				insOtherChargesDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
				insOtherChargesDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insOtherChargesDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "Frtpayer_mst_fk").Direction = ParameterDirection.Input;
				insOtherChargesDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
				insOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

				insOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
				insOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

				insOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "job_trn_air_IMP_oth_pk").Direction = ParameterDirection.Output;
				insOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with21 = updOtherChargesDetails;
				_with21.Connection = objWK.MyConnection;
				_with21.CommandType = CommandType.StoredProcedure;
				_with21.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_OTH_CHRG_UPD";
				var _with22 = _with21.Parameters;

				updOtherChargesDetails.Parameters.Add("JOB_TRN_AIR_IMP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_oth_pk").Direction = ParameterDirection.Input;
				updOtherChargesDetails.Parameters["JOB_TRN_AIR_IMP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

				updOtherChargesDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

				updOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
				updOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
				updOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updOtherChargesDetails.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "PaymentType").Direction = ParameterDirection.Input;
				updOtherChargesDetails.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

				updOtherChargesDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
				updOtherChargesDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updOtherChargesDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_mst_fk").Direction = ParameterDirection.Input;
				updOtherChargesDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				updOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
				updOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

				updOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
				updOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

				updOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				updOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with23 = delOtherChargesDetails;
				_with23.Connection = objWK.MyConnection;
				_with23.CommandType = CommandType.StoredProcedure;
				_with23.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_OTH_CHRG_DEL";

				delOtherChargesDetails.Parameters.Add("JOB_TRN_AIR_IMP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_oth_pk").Direction = ParameterDirection.Input;
				delOtherChargesDetails.Parameters["JOB_TRN_AIR_IMP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

				delOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
				delOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

				var _with24 = objWK.MyDataAdapter;

				_with24.InsertCommand = insOtherChargesDetails;
				_with24.InsertCommand.Transaction = TRAN;

				_with24.UpdateCommand = updOtherChargesDetails;
				_with24.UpdateCommand.Transaction = TRAN;

				_with24.DeleteCommand = delOtherChargesDetails;
				_with24.DeleteCommand.Transaction = TRAN;

				RecAfct = _with24.Update(dsOtherCharges);
				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					if (isEdting == false) {
                        RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                    }
					return arrMessage;
				} else {
					arrMessage.Clear();
					arrMessage.Add("All Data Saved Successfully");
					TRAN.Commit();
					return arrMessage;
				}
			} catch (OracleException oraexp) {
				if (isEdting == false) {
                    RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
				throw oraexp;
			} catch (Exception ex) {
				if (isEdting == false) {
                    RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                    //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                }
				throw ex;
			} finally {
				objWK.CloseConnection();
				//Added by sivachandran - To close the connection after Transaction
			}
		}
		#endregion

		#region "Fetch MJCRefNr"
		public string FetchMJCRefNr(string MJobpk)
		{
			System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			strSQL.Append(" SELECT MJOB.MASTER_JC_REF_NO FROM ");
			strSQL.Append(" MASTER_JC_AIR_IMP_TBL MJOB");
			strSQL.Append(" WHERE MJOB.MASTER_JC_AIR_IMP_PK = " + MJobpk + " ");
			try {
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

		#region "Save MasterJobcard"
		public ArrayList SaveMjc(DataSet M_DataSet, string MSTJCRefNo, string Location, string EmpPk, long MSTJobCardPK, int NoOfHbls, string sid = "", string polid = "", string podid = "")
		{
			arrMessage.Clear();
			WorkFlow objWK = new WorkFlow();
			OracleTransaction TRAN = null;
			objWK.OpenConnection();
			TRAN = objWK.MyConnection.BeginTransaction();
			OracleCommand SelectCommand = new OracleCommand();
			objWK.MyCommand.Transaction = TRAN;
			OracleCommand insCommand = new OracleCommand();
			bool chkflag = false;
			Int32 RecAfct = default(Int32);
			Int16 intIns = default(Int16);
			string str = null;
			objVesselVoyage.ConfigurationPK = M_Configuration_PK;
			objVesselVoyage.CREATED_BY = M_CREATED_BY_FK;

			MSTJCRefNo = GenerateProtocolKey("MASTER JC AIR IMPORT", Convert.ToInt32(Location), Convert.ToInt32(EmpPk), DateTime.Now, "","" , polid, CREATED_BY,new WorkFlow() , sid,
			podid);
			if (MSTJCRefNo == "Protocol Not Defined.") {
				arrMessage.Add("Protocol Not Defined.");
				return arrMessage;
			} else if (MSTJCRefNo.Length > 20) {
				arrMessage.Add("Protocol should be less than 20 Characters");
				return arrMessage;
			}
			try {
				var _with25 = insCommand;
				_with25.Connection = objWK.MyConnection;
				_with25.CommandType = CommandType.StoredProcedure;
				_with25.CommandText = objWK.MyUserName + ".MASTER_JC_AIR_IMP_TBL_PKG.MASTER_JC_AIR_IMP_TBL_INS";
				var _with26 = _with25.Parameters;

				insCommand.Parameters.Add("MASTER_JC_REF_NO_IN", MSTJCRefNo).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("MASTER_JC_DATE_IN", OracleDbType.Date, 20, "MASTER_JC_DATE").Direction = ParameterDirection.Input;
				insCommand.Parameters["MASTER_JC_DATE_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("MASTER_JC_STATUS_IN", OracleDbType.Int32, 1, "MASTER_JC_STATUS").Direction = ParameterDirection.Input;
				insCommand.Parameters["MASTER_JC_STATUS_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("MASTER_JC_CLOSED_ON_IN", OracleDbType.Date, 20, "MASTER_JC_CLOSED_ON").Direction = ParameterDirection.Input;
				insCommand.Parameters["MASTER_JC_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("PORT_MST_POL_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POL_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("PORT_MST_POD_FK_IN", OracleDbType.Int32, 10, "PORT_MST_POD_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("LOAD_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "LOAD_AGENT_MST_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["LOAD_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 500, "REMARKS").Direction = ParameterDirection.Input;
				insCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("AOD_ATA_IN", OracleDbType.Date, 20, "AOD_ATA").Direction = ParameterDirection.Input;
				insCommand.Parameters["AOD_ATA_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2, 25, "FLIGHT_NO").Direction = ParameterDirection.Input;
				insCommand.Parameters["FLIGHT_NO_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("AIRLINE_MST_FK_IN", OracleDbType.Int32, 10, "AIRLINE_MST_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["AIRLINE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_FK").Direction = ParameterDirection.Input;
				insCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(CREATED_BY)).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;

				insCommand.Parameters.Add("AOD_ETA_IN", OracleDbType.Date, 20, "AOD_ETA").Direction = ParameterDirection.Input;
				insCommand.Parameters["AOD_ETA_IN"].SourceVersion = DataRowVersion.Current;

				insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MASTER_JC_AIR_IMP_PK").Direction = ParameterDirection.Output;
				insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                

				var _with27 = objWK.MyDataAdapter;
				_with27.InsertCommand = insCommand;
				_with27.InsertCommand.Transaction = TRAN;
				if ((M_DataSet.GetChanges(DataRowState.Added) != null)) {
					chkflag = true;
				} else {
					chkflag = false;
				}
				RecAfct = _with27.Update(M_DataSet);

				if (arrMessage.Count > 0) {
					TRAN.Rollback();
					if (chkflag) {
                        RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNo, System.DateTime.Now);
                        //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    }
					return arrMessage;
				} else {
					MSTJobCardPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
					///
					if (MSTJobCardPK != 0) {
						OracleCommand updCmdUser = new OracleCommand();
						updCmdUser.Transaction = TRAN;
						str = " update MASTER_JC_AIR_IMP_TBL MJOB set MJOB.NO_HBLS=" + NoOfHbls;
						str += " WHERE MJOB.MASTER_JC_AIR_IMP_PK=" + MSTJobCardPK;
						var _with28 = updCmdUser;
						_with28.Connection = objWK.MyConnection;
						_with28.Transaction = TRAN;
						_with28.CommandType = CommandType.Text;
						_with28.CommandText = str;
						intIns = Convert.ToInt16(_with28.ExecuteNonQuery());
					}
					///
					arrMessage.Add("All Data Saved Successfully");
					TRAN.Commit();
					return arrMessage;
				}
			} catch (OracleException oraexp) {
				if (chkflag) {
                    RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNo, System.DateTime.Now);
				}
				throw oraexp;
			} catch (Exception ex) {
				if (chkflag) {
                    RollbackProtocolKey("JOB CARD IMP (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNo, System.DateTime.Now);
                }
				throw ex;
			} finally {
				objWK.CloseConnection();
			}
		}
		#endregion

		#region "Fetch NoOfHAWBS"
		public int FetchNoOfHawbs(string MJobpk)
		{
			System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
			WorkFlow objWF = new WorkFlow();
			strSQL.Append(" SELECT MJOB.NO_HBLS FROM ");
			strSQL.Append(" MASTER_JC_AIR_IMP_TBL MJOB");
			strSQL.Append(" WHERE MJOB.MASTER_JC_AIR_IMP_PK = " + MJobpk + " ");
			try {
				return Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Fetch Grid Details"
		public DataSet FetchGridDetails(string POLPK = "", string PODPK = "", string FlightNr = "", string ShipperPK = "", string ConsigneePK = "", string HawbNr = "", string MawbNr = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0)
		{
			WorkFlow objWF = new WorkFlow();
			Int32 TotalRecords = default(Int32);
			Int32 last = default(Int32);
			Int32 start = default(Int32);
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			string strSQL = null;
			sb.Append("SELECT DISTINCT JOB.JOB_CARD_TRN_PK JOB_CARD_TRN_PK,");
			sb.Append("                MJC.MASTER_JC_AIR_IMP_PK,");
			sb.Append("                JOB.MBL_MAWB_REF_NO MAWB_REF_NO,");
			sb.Append("                JOB.HBL_HAWB_REF_NO HAWB_REF_NO,");
			sb.Append("                MJC.MASTER_JC_REF_NO,");
			sb.Append("                JOB.JOBCARD_REF_NO,");
			sb.Append("                AMT.AIRLINE_NAME,");
			sb.Append("                JOB.VOYAGE_FLIGHT_NO FLIGHT_NO,");
			sb.Append("                JOB.ETA_DATE,");
			sb.Append("                POL.PORT_ID              POLID,");
			sb.Append("                POD.PORT_ID              PODID,");
			sb.Append("                CMT.CUSTOMER_NAME, ");
			sb.Append("                JOB.PAGE_NR ");
			sb.Append("  FROM JOB_CARD_TRN  JOB,");
			sb.Append("       MASTER_JC_AIR_IMP_TBL MJC,");
			sb.Append("       AIRLINE_MST_TBL       AMT,");
			sb.Append("       PORT_MST_TBL          POL,");
			sb.Append("       PORT_MST_TBL          POD,");
			sb.Append("       USER_MST_TBL          UMT,");
			sb.Append("       CUSTOMER_MST_TBL      CMT");
			sb.Append(" WHERE JOB.MASTER_JC_FK = MJC.MASTER_JC_AIR_IMP_PK(+)");
			sb.Append("   AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
			sb.Append("   AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
			sb.Append("   AND JOB.CREATED_BY_FK = UMT.USER_MST_PK");
			sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
			sb.Append("   AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
			sb.Append("   AND AMT.AIRLINE_MST_PK = JOB.CARRIER_MST_FK");
			sb.Append("   AND JOB.BUSINESS_TYPE=1 ");
			sb.Append("   AND JOB.PROCESS_TYPE=2 ");
			sb.Append("   AND JOB.CONSOLE = 2");
			if (flag == 0) {
				sb.Append("AND 1=2");
			}
			if (!string.IsNullOrEmpty(POLPK)) {
				sb.Append("AND JOB.PORT_MST_POL_FK=" + POLPK);
			}
			if (!string.IsNullOrEmpty(PODPK)) {
				sb.Append("AND JOB.PORT_MST_POD_FK=" + PODPK);
			}
			if (!string.IsNullOrEmpty(ShipperPK)) {
				sb.Append("AND JOB.SHIPPER_CUST_MST_FK=" + ShipperPK);
			}
			if (!string.IsNullOrEmpty(ConsigneePK)) {
				sb.Append("AND JOB.CONSIGNEE_CUST_MST_FK=" + ConsigneePK);
			}
			if (HawbNr.Trim().Length > 0) {
				sb.Append(" AND UPPER(JOB.HBL_HAWB_REF_NO) LIKE '%" + HawbNr.ToUpper().Replace("'", "''") + "%'");
			}
			if (MawbNr.Trim().Length > 0) {
				sb.Append(" AND UPPER(JOB.MBL_MAWB_REF_NO) LIKE '%" + MawbNr.ToUpper().Replace("'", "''") + "%'");
			}
			if (FlightNr.Trim().Length > 0) {
				sb.Append(" AND UPPER(JOB.VOYAGE_FLIGHT_NO) LIKE '%" + FlightNr.ToUpper().Replace("'", "''") + "%'");
			}
			sb.Append("   ORDER BY JOB_CARD_TRN_PK DESC");
			System.Text.StringBuilder strCount = new System.Text.StringBuilder();
			strSQL = sb.ToString();
			strCount.Append(" SELECT COUNT(*)  from  ");
			strCount.Append((" (" + sb.ToString() + ")"));
			TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));
			TotalPage = TotalRecords / RecordsPerPage;
			if (TotalRecords % RecordsPerPage != 0) {
				TotalPage += 1;
			}
			if (CurrentPage > TotalPage)
				CurrentPage = 1;
			if (TotalRecords == 0)
				CurrentPage = 0;
			last = CurrentPage * RecordsPerPage;
			start = (CurrentPage - 1) * RecordsPerPage + 1;
			strCount.Remove(0, strCount.Length);
			System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
			sqlstr2.Append(" Select * from ");
			sqlstr2.Append("  ( Select ROWNUM SR_NO, q.* from ");
			sqlstr2.Append("  (" + sb.ToString() + " ");
			sqlstr2.Append("  ) q )  WHERE \"SR_NO\"  BETWEEN " + start + " AND " + last + "");
			strSQL = sqlstr2.ToString();
			try {
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

		#region "Fetch Frunction"
		public DataSet FetchFunction(string bizType = "", string POLPK = "", string PODPK = "", string FlightNr = "", string HawbNr = "", string MawbNr = "")
		{
			System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
			OracleDataAdapter DA = new OracleDataAdapter();
			DataSet MainDS = new DataSet();
			WorkFlow objWF = new WorkFlow();
			try {
				sb.Append("SELECT MJOB.MASTER_JC_AIR_IMP_PK JOBPK,");
				sb.Append("       MJOB.MASTER_JC_AIR_IMP_PK MJOBPK,");
				sb.Append("       JOB.MBL_MAWB_REF_NO REF_NO,");
				sb.Append("       JOB.MBL_MAWB_DATE MBL_DATE,");
				sb.Append("       POL.PORT_ID POLID,");
				sb.Append("       POD.PORT_ID PODID,");
				sb.Append("       MAX(JOB.ETA_DATE) ETA_DATE,");
				sb.Append("       MAX(JOB.ARRIVAL_DATE) ARRIVAL_DATE,");
				sb.Append("       '' COMMODITY_ID,");
				sb.Append("       '' PACK_TYPE_ID,");
				sb.Append("       NVL(SUM(JOB_TRN.PACK_COUNT), 0) QTY,");
				sb.Append("       NVL(SUM(JOB_TRN.VOLUME_IN_CBM), 0) VOLUME,");
				sb.Append("       NVL(SUM(JOB_TRN.GROSS_WEIGHT), 0) GROSS_WEIGHT,");
				sb.Append("       NVL(SUM(JOB_TRN.CHARGEABLE_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("       '' SEL,");
				sb.Append("       '' SAVEMJC");
				sb.Append("  FROM JOB_CARD_TRN  JOB,");
				sb.Append("       JOB_TRN_CONT  JOB_TRN,");
				sb.Append("       MASTER_JC_AIR_IMP_TBL MJOB,");
				sb.Append("       PORT_MST_TBL          POL,");
				sb.Append("       PORT_MST_TBL          POD,");
				sb.Append("       COMMODITY_MST_TBL     CMT,");
				sb.Append("       PACK_TYPE_MST_TBL     PTMT");
				sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = JOB_TRN.JOB_CARD_TRN_FK");
				sb.Append("   AND JOB.MASTER_JC_FK = MJOB.MASTER_JC_AIR_IMP_PK");
				sb.Append("   AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
				sb.Append("   AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
				sb.Append("   AND JOB_TRN.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
				sb.Append("   AND JOB_TRN.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK(+)");
				sb.Append("   AND JOB.CONSOLE = 2");
				sb.Append("   AND MJOB.NO_HBLS =");
				sb.Append("       (SELECT COUNT(*)");
				sb.Append("          FROM JOB_CARD_TRN J");
				sb.Append("         WHERE J.MASTER_JC_FK = MJOB.MASTER_JC_AIR_IMP_PK AND J.BUSINESS_TYPE=1 AND J.PROCESS_TYPE=2)");

				if (!string.IsNullOrEmpty(POLPK)) {
					sb.Append("  AND JOB.PORT_MST_POL_FK=" + POLPK);
				}
				if (!string.IsNullOrEmpty(PODPK)) {
					sb.Append("   AND JOB.PORT_MST_POD_FK=" + PODPK);
				}
				if (FlightNr.Trim().Length > 0) {
					sb.Append(" AND UPPER(JOB.VOYAGE_FLIGHT_NO) LIKE '%" + FlightNr.ToUpper().Replace("'", "''") + "%'");
				}
				if (HawbNr.Trim().Length > 0) {
					sb.Append(" AND UPPER(JOB.HBL_HAWB_REF_NO) LIKE '%" + HawbNr.ToUpper().Replace("'", "''") + "%'");
				}
				if (MawbNr.Trim().Length > 0) {
					sb.Append(" AND UPPER(JOB.MBL_MAWB_REF_NO) LIKE '%" + MawbNr.ToUpper().Replace("'", "''") + "%'");
				}
				sb.Append(" AND JOB.BUSINESS_TYPE = " + bizType);
				sb.Append(" AND JOB.PROCESS_TYPE = 2");
				sb.Append(" GROUP BY MJOB.MASTER_JC_AIR_IMP_PK,");
				sb.Append("          JOB.MBL_MAWB_REF_NO,");
				sb.Append("          JOB.MBL_MAWB_DATE,");
				sb.Append("          POL.PORT_ID,");
				sb.Append("          POD.PORT_ID,");
				sb.Append("          JOB.ETA_DATE,");
				sb.Append("          JOB.ARRIVAL_DATE ");

				//'Modified by Mayur for Getting Master JobCards List apart from Quick Entry for Deconsolidation 
				//sb.Append("  ORDER BY JOB.MAWB_DATE DESC ")
				sb.Append(" UNION");
				sb.Append(" SELECT M.MASTER_JC_AIR_IMP_PK JOBPK,");
				sb.Append("       M.MASTER_JC_AIR_IMP_PK MJOBPK,");
				sb.Append("       JOB.MBL_MAWB_REF_NO REF_NO,");
				sb.Append("       JOB.MBL_MAWB_DATE MBL_DATE,");
				sb.Append("       POL.PORT_ID POLID,");
				sb.Append("       POD.PORT_ID PODID,");
				sb.Append("       MAX(JOB.ETA_DATE) ETA_DATE,");
				sb.Append("       MAX(JOB.ARRIVAL_DATE) ARRIVAL_DATE,");
				sb.Append("       '' COMMODITY_ID,");
				sb.Append("       '' PACK_TYPE_ID,");
				sb.Append("       NVL(SUM(JOB_TRN.PACK_COUNT), 0) QTY,");
				sb.Append("       NVL(SUM(JOB_TRN.VOLUME_IN_CBM), 0) VOLUME,");
				sb.Append("       NVL(SUM(JOB_TRN.GROSS_WEIGHT), 0) GROSS_WEIGHT,");
				sb.Append("       NVL(SUM(JOB_TRN.CHARGEABLE_WEIGHT), 0) NET_WEIGHT,");
				sb.Append("       '' SEL,");
				sb.Append("       '' SAVEMJC");
				sb.Append("  FROM JOB_CARD_TRN  JOB,");
				sb.Append("       JOB_TRN_CONT  JOB_TRN,");
				sb.Append("       MASTER_JC_AIR_IMP_TBL M,");
				sb.Append("       PORT_MST_TBL          POL,");
				sb.Append("       PORT_MST_TBL          POD,");
				sb.Append("       AIRLINE_MST_TBL       ART,");
				sb.Append("       AGENT_MST_TBL         AMT");
				sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = JOB_TRN.JOB_CARD_TRN_FK");
				sb.Append("   AND JOB.MASTER_JC_FK = M.MASTER_JC_AIR_IMP_PK");
				sb.Append("   AND POL.PORT_MST_PK = M.PORT_MST_POL_FK");
				sb.Append("   AND POD.PORT_MST_PK = M.PORT_MST_POD_FK");
				sb.Append("   AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK");
				sb.Append("   AND ART.AIRLINE_MST_PK(+) = M.AIRLINE_MST_FK");
				sb.Append("   AND M.MASTER_JC_STATUS = 1");

				if (!string.IsNullOrEmpty(POLPK)) {
					sb.Append("  AND JOB.PORT_MST_POL_FK=" + POLPK);
				}
				if (!string.IsNullOrEmpty(PODPK)) {
					sb.Append("   AND JOB.PORT_MST_POD_FK=" + PODPK);
				}
				if (FlightNr.Trim().Length > 0) {
					sb.Append(" AND UPPER(JOB.VOYAGE_FLIGHT_NO) LIKE '%" + FlightNr.ToUpper().Replace("'", "''") + "%'");
				}
				if (HawbNr.Trim().Length > 0) {
					sb.Append(" AND UPPER(JOB.HBL_HAWB_REF_NO) LIKE '%" + HawbNr.ToUpper().Replace("'", "''") + "%'");
				}
				if (MawbNr.Trim().Length > 0) {
					sb.Append(" AND UPPER(JOB.MBL_MAWB_REF_NO) LIKE '%" + MawbNr.ToUpper().Replace("'", "''") + "%'");
				}
				sb.Append(" AND JOB.BUSINESS_TYPE = " + bizType);
				sb.Append(" AND JOB.PROCESS_TYPE = 2");
				sb.Append("  GROUP BY M.MASTER_JC_AIR_IMP_PK,");
				sb.Append("          JOB.MBL_MAWB_REF_NO,");
				sb.Append("          JOB.MBL_MAWB_DATE,");
				sb.Append("          POL.PORT_ID,");
				sb.Append("          POD.PORT_ID");
				//sb.Append("          JOB.ETA_DATE,")
				//sb.Append("          JOB.ARRIVAL_DATE")
				sb.Append(" ORDER BY MBL_DATE DESC");


				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "PARENT");

				sb.Remove(0, sb.Length);
				sb.Append("SELECT JOB.JOB_CARD_TRN_PK JOBPK,");
				sb.Append("       0 JOB_TRN_PK,");
				sb.Append("       MJOB.MASTER_JC_AIR_IMP_PK MJOBPK,");
				sb.Append("       JOB.HBL_HAWB_REF_NO REF_NO,");
				sb.Append("       JOB.HBL_HAWB_DATE HBL_DATE,");
				sb.Append("       POL.PORT_ID POLID,");
				sb.Append("       POD.PORT_ID PODID,");
				sb.Append("       JOB.ETA_DATE,");
				sb.Append("       JOB.ARRIVAL_DATE,");
				sb.Append("       CUST.CUSTOMER_NAME CONSIGNEE,");
				sb.Append("       SHIP.CUSTOMER_NAME SHIPPER,");
				sb.Append("       '' COMMODITY_ID,");
				sb.Append("       '' PACK_TYPE_ID,");
				sb.Append("       SUM(NVL(JOB_TRN.PACK_COUNT,0)) PACK_COUNT,");
				sb.Append("       SUM(NVL(JOB_TRN.VOLUME_IN_CBM,0)) VOLUME_IN_CBM,");
				sb.Append("       SUM(NVL(JOB_TRN.GROSS_WEIGHT,0)) GROSS_WEIGHT,");
				sb.Append("       SUM(NVL(JOB_TRN.CHARGEABLE_WEIGHT, 0)) NET_WEIGHT,");
				sb.Append("       '' SEL");
				sb.Append("  FROM JOB_CARD_TRN  JOB,");
				sb.Append("       MASTER_JC_AIR_IMP_TBL MJOB,");
				sb.Append("       JOB_TRN_CONT  JOB_TRN,");
				sb.Append("       PORT_MST_TBL          POL,");
				sb.Append("       PORT_MST_TBL          POD,");
				sb.Append("       COMMODITY_MST_TBL     CMT,");
				sb.Append("       PACK_TYPE_MST_TBL     PTMT,");
				sb.Append("       CUSTOMER_MST_TBL      CUST,");
				sb.Append("       CUSTOMER_MST_TBL      SHIP");
				sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = JOB_TRN.JOB_CARD_TRN_FK");
				sb.Append("   AND JOB.MASTER_JC_FK = MJOB.MASTER_JC_AIR_IMP_PK");
				sb.Append("   AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
				sb.Append("   AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
				sb.Append("   AND JOB_TRN.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
				sb.Append("   AND JOB_TRN.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK(+)");
				sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
				sb.Append("   AND JOB.SHIPPER_CUST_MST_FK = SHIP.CUSTOMER_MST_PK");
				sb.Append("   AND JOB.CONSOLE = 2");
				sb.Append("   AND MJOB.NO_HBLS =");
				sb.Append("       (SELECT COUNT(*)");
				sb.Append("          FROM JOB_CARD_TRN J");
				sb.Append("         WHERE J.MASTER_JC_FK = MJOB.MASTER_JC_AIR_IMP_PK AND J.BUSINESS_TYPE=1 AND J.PROCESS_TYPE=2)");

				if (!string.IsNullOrEmpty(POLPK)) {
					sb.Append("  AND JOB.PORT_MST_POL_FK=" + POLPK);
				}
				if (!string.IsNullOrEmpty(PODPK)) {
					sb.Append("   AND JOB.PORT_MST_POD_FK=" + PODPK);
				}
				if (FlightNr.Trim().Length > 0) {
					sb.Append(" AND UPPER(JOB.VOYAGE_FLIGHT_NO) LIKE '%" + FlightNr.ToUpper().Replace("'", "''") + "%'");
				}
				if (HawbNr.Trim().Length > 0) {
					sb.Append(" AND UPPER(JOB.HBL_HAWB_REF_NO) LIKE '%" + HawbNr.ToUpper().Replace("'", "''") + "%'");
				}
				if (MawbNr.Trim().Length > 0) {
					sb.Append(" AND UPPER(JOB.MBL_MAWB_REF_NO) LIKE '%" + MawbNr.ToUpper().Replace("'", "''") + "%'");
				}
				sb.Append(" AND JOB.BUSINESS_TYPE = " + bizType);
				sb.Append(" AND JOB.PROCESS_TYPE = 2");
				sb.Append(" GROUP BY JOB.JOB_CARD_TRN_PK ,");
				sb.Append("       MJOB.MASTER_JC_AIR_IMP_PK ,");
				sb.Append("       JOB.HBL_HAWB_REF_NO ,");
				sb.Append("       JOB.HBL_HAWB_DATE ,");
				sb.Append("       POL.PORT_ID ,");
				sb.Append("       POD.PORT_ID ,");
				sb.Append("       JOB.ETA_DATE,");
				sb.Append("       JOB.ARRIVAL_DATE,");
				sb.Append("       CUST.CUSTOMER_NAME ,");
				sb.Append("       SHIP.CUSTOMER_NAME ");

				sb.Append(" UNION");
				sb.Append("  SELECT");
				sb.Append(" JOB.JOB_CARD_TRN_PK JOBPK,");
				sb.Append("       0 JOB_TRN_PK,");
				sb.Append("       MJ.MASTER_JC_AIR_IMP_PK MJOBPK,");
				sb.Append("       JOB.HBL_HAWB_REF_NO REF_NO,");
				sb.Append("       JOB.HBL_HAWB_DATE HBL_DATE,");
				sb.Append("       POL.PORT_ID POLID,");
				sb.Append("       POD.PORT_ID PODID,");
				sb.Append("       JOB.ETA_DATE,");
				sb.Append("       JOB.ARRIVAL_DATE,");
				sb.Append("       CO.CUSTOMER_NAME CONSIGNEE,");
				sb.Append("       SH.CUSTOMER_NAME SHIPPER,");
				sb.Append("       '' COMMODITY_ID,");
				sb.Append("       '' PACK_TYPE_ID,");
				sb.Append("       SUM(NVL(JOB_TRN.PACK_COUNT,0)) PACK_COUNT,");
				sb.Append("       SUM(NVL(JOB_TRN.VOLUME_IN_CBM,0)) VOLUME_IN_CBM,");
				sb.Append("       SUM(NVL(JOB_TRN.GROSS_WEIGHT,0)) GROSS_WEIGHT,");
				sb.Append("       SUM(NVL(JOB_TRN.CHARGEABLE_WEIGHT, 0)) NET_WEIGHT,");
				sb.Append("       '' SEL");
				sb.Append("  FROM JOB_CARD_TRN  JOB,");
				sb.Append("       AGENT_MST_TBL         POLA,");
				sb.Append("       CUSTOMER_MST_TBL      SH,");
				sb.Append("       CUSTOMER_MST_TBL      CO,");
				sb.Append("       PORT_MST_TBL          POL,");
				sb.Append("       PORT_MST_TBL          POD,");
				sb.Append("       AGENT_MST_TBL         CBA,");
				sb.Append("       AGENT_MST_TBL         CLA,");
				sb.Append("       MASTER_JC_AIR_IMP_TBL MJ,");
				sb.Append("       PACK_TYPE_MST_TBL     PTMT,");
				sb.Append("       JOB_TRN_CONT  JOB_TRN");
				sb.Append(" WHERE JOB.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
				sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
				sb.Append("   AND JOB.MASTER_JC_FK = MJ.MASTER_JC_AIR_IMP_PK");
				sb.Append("   AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK");
				sb.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK");
				sb.Append("   AND JOB.CL_AGENT_MST_FK = CLA.AGENT_MST_PK(+)");
				sb.Append("   AND JOB.CB_AGENT_MST_FK = CBA.AGENT_MST_PK(+)");
				sb.Append("   AND JOB.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)");
				sb.Append("   AND JOB_TRN.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK(+)");
				sb.Append("   AND JOB_TRN.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK");
				sb.Append("   AND MJ.MASTER_JC_STATUS=1");
				if (!string.IsNullOrEmpty(POLPK)) {
					sb.Append("  AND JOB.PORT_MST_POL_FK=" + POLPK);
				}
				if (!string.IsNullOrEmpty(PODPK)) {
					sb.Append("   AND JOB.PORT_MST_POD_FK=" + PODPK);
				}
				if (FlightNr.Trim().Length > 0) {
					sb.Append(" AND UPPER(JOB.VOYAGE_FLIGHT_NO) LIKE '%" + FlightNr.ToUpper().Replace("'", "''") + "%'");
				}
				if (HawbNr.Trim().Length > 0) {
					sb.Append(" AND UPPER(JOB.HBL_HAWB_REF_NO) LIKE '%" + HawbNr.ToUpper().Replace("'", "''") + "%'");
				}
				if (MawbNr.Trim().Length > 0) {
					sb.Append(" AND UPPER(JOB.MBL_MAWB_REF_NO) LIKE '%" + MawbNr.ToUpper().Replace("'", "''") + "%'");
				}
				sb.Append(" AND JOB.BUSINESS_TYPE = " + bizType);
				sb.Append(" AND JOB.PROCESS_TYPE = 2");
				sb.Append(" GROUP BY JOB.JOB_CARD_TRN_PK ,");
				sb.Append("       MJ.MASTER_JC_AIR_IMP_PK ,");
				sb.Append("       JOB.HBL_HAWB_REF_NO ,");
				sb.Append("       JOB.HBL_HAWB_DATE ,");
				sb.Append("       POL.PORT_ID ,");
				sb.Append("       POD.PORT_ID ,");
				sb.Append("       JOB.ETA_DATE,");
				sb.Append("       JOB.ARRIVAL_DATE,");
				sb.Append("       CO.CUSTOMER_NAME ,");
				sb.Append("       SH.CUSTOMER_NAME");
				sb.Append(" ORDER BY HBL_DATE DESC");


				DA = objWF.GetDataAdapter(sb.ToString());
				DA.Fill(MainDS, "CHILDDETAILS");

				DataRelation rel_Details = new DataRelation("MJCDETAILS", new DataColumn[] { MainDS.Tables[0].Columns["MJOBPK"] }, new DataColumn[] { MainDS.Tables[1].Columns["MJOBPK"] });
				rel_Details.Nested = true;
				MainDS.Relations.Add(rel_Details);
				return MainDS;
			} catch (OracleException sqlExp) {
				ErrorMessage = sqlExp.Message;
				throw sqlExp;
			} catch (Exception exp) {
				ErrorMessage = exp.Message;
				throw exp;
			}
		}
		#endregion

		#region "Get containerdata"
		public DataSet GetContainer_Data(string jobCardPK = "0")
		{
			WorkFlow objWF = new WorkFlow();
			StringBuilder SQL = new StringBuilder();
			SQL.Append("SELECT ");
			SQL.Append("      job_trn_cont_pk,");
			SQL.Append("      job_card.job_card_trn_pk,");
			SQL.Append("      palette_size,");
			SQL.Append("      commodity_name,");
			SQL.Append("      pack_type_mst_fk,");
			SQL.Append("      pack_count,");
			SQL.Append("      gross_weight,");
			SQL.Append("      volume_in_cbm,");
			SQL.Append("      chargeable_weight,");
			SQL.Append("      to_char(load_date,dateformat) load_date,");
			SQL.Append("      commodity_mst_fks,'false' as \"Delete\" ");
			SQL.Append(" FROM");
			SQL.Append("      job_trn_cont cont_trn,");
			SQL.Append("      job_card_trn job_card,");
			SQL.Append("      pack_type_mst_tbl pack,");
			SQL.Append("      commodity_mst_tbl comm");
			SQL.Append(" WHERE");
			SQL.Append("      cont_trn.job_card_trn_fk in (" + jobCardPK + ")");
			SQL.Append("      AND cont_trn.job_card_trn_fk = job_card.job_card_trn_pk");
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

		#region "Save Container Details"

		public ArrayList SaveContainerDtls(DataSet dsContainerData, string JobcardPK, string FlightNr = "")
		{
			arrMessage.Clear();
			Int32 RecAfct = default(Int32);
			WorkFlow objWK = new WorkFlow();
			OracleTransaction TRAN = null;
			string Str = null;
			objWK.OpenConnection();
			TRAN = objWK.MyConnection.BeginTransaction();
			OracleCommand updContainerDetails = new OracleCommand();
			int nRowCnt = 0;
			int i = 0;
			int intIns = 0;
			Array JOBPK = null;
			JOBPK = JobcardPK.Split(',');
			try {
				if (!string.IsNullOrEmpty(FlightNr) & (FlightNr != null)) {
					for (i = 0; i <= JOBPK.Length - 1; i++) {
						OracleCommand updCmdUser = new OracleCommand();
						updCmdUser.Transaction = TRAN;
						Str = " update job_card_trn job set job.sec_flight_no='" + FlightNr + "'";
						Str += " WHERE job.job_card_trn_pk=" + JOBPK.GetValue(i);
						var _with29 = updCmdUser;
						_with29.Connection = objWK.MyConnection;
						_with29.Transaction = TRAN;
						_with29.CommandType = CommandType.Text;
						_with29.CommandText = Str;
						intIns = Convert.ToInt32(_with29.ExecuteNonQuery());
					}
				}
				for (nRowCnt = 0; nRowCnt <= dsContainerData.Tables[0].Rows.Count - 1; nRowCnt++) {
					var _with30 = updContainerDetails;
					_with30.Transaction = TRAN;
					_with30.Connection = objWK.MyConnection;
					_with30.CommandType = CommandType.StoredProcedure;
					_with30.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_CONT_UPD";
					_with30.Parameters.Clear();
					var _with31 = _with30.Parameters;
					updContainerDetails.Parameters.Add("JOB_TRN_AIR_IMP_CONT_PK_IN", dsContainerData.Tables[0].Rows[nRowCnt]["job_trn_cont_pk"]).Direction = ParameterDirection.Input;
					updContainerDetails.Parameters["JOB_TRN_AIR_IMP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

					updContainerDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", dsContainerData.Tables[0].Rows[nRowCnt]["JOB_CARD_TRN_PK"]).Direction = ParameterDirection.Input;

					updContainerDetails.Parameters.Add("PALETTE_SIZE_IN", dsContainerData.Tables[0].Rows[nRowCnt]["palette_size"]).Direction = ParameterDirection.Input;
					updContainerDetails.Parameters["PALETTE_SIZE_IN"].SourceVersion = DataRowVersion.Current;

					updContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", dsContainerData.Tables[0].Rows[nRowCnt]["volume_in_cbm"]).Direction = ParameterDirection.Input;
					updContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

					updContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", dsContainerData.Tables[0].Rows[nRowCnt]["gross_weight"]).Direction = ParameterDirection.Input;
					updContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

					updContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", dsContainerData.Tables[0].Rows[nRowCnt]["chargeable_weight"]).Direction = ParameterDirection.Input;
					updContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

					updContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", dsContainerData.Tables[0].Rows[nRowCnt]["pack_type_mst_fk"]).Direction = ParameterDirection.Input;
					updContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

					updContainerDetails.Parameters.Add("PACK_COUNT_IN", dsContainerData.Tables[0].Rows[nRowCnt]["pack_count"]).Direction = ParameterDirection.Input;
					updContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

					updContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", dsContainerData.Tables[0].Rows[nRowCnt]["commodity_mst_fks"]).Direction = ParameterDirection.Input;
					updContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

					updContainerDetails.Parameters.Add("LOAD_DATE_IN", dsContainerData.Tables[0].Rows[nRowCnt]["load_date"]).Direction = ParameterDirection.Input;
					updContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

					updContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
					updContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
					RecAfct = _with30.ExecuteNonQuery();
				}
				if (RecAfct > 0) {
					TRAN.Commit();
					arrMessage.Add("All data saved successfully");
					return arrMessage;
				} else {
					TRAN.Rollback();
					return arrMessage;
				}
			} catch (OracleException oraexp) {
				TRAN.Rollback();
				throw oraexp;
				arrMessage.Add(oraexp.Message);
			} catch (Exception ex) {
				TRAN.Rollback();
				arrMessage.Add(ex.Message);
				throw ex;
			} finally {
				objWK.MyCommand.Connection.Close();
			}
		}
		#endregion

		#region "De-Console MasterJobCard"

		public ArrayList DeConsoleMJCImp(string MJCIMPPK)
		{
			OracleCommand Cmd = new OracleCommand();
			WorkFlow objWK = new WorkFlow();
			OracleTransaction TRAN = null;
			int RAF = 0;
			int i = 0;
			Array MJCPKArray = null;
			string JCPKs = "";
			objWK.OpenConnection();
			string SRet_Value = null;
			arrMessage.Clear();
			TRAN = objWK.MyConnection.BeginTransaction();
			MJCPKArray = MJCIMPPK.Split(',');
			try {
				for (i = 0; i <= MJCPKArray.Length - 1; i++) {
					var _with32 = Cmd;
					_with32.Parameters.Clear();
					_with32.Transaction = TRAN;
					_with32.Connection = objWK.MyConnection;
					_with32.CommandType = CommandType.StoredProcedure;
					_with32.CommandText = objWK.MyUserName + ".MASTER_JC_AIR_EXP_TBL_PKG.MJC_IMP_DECONSOLE";
					var _with33 = _with32.Parameters;
					_with33.Add("MJC_AIR_IMP_PK_IN", MJCPKArray.GetValue(i)).Direction = ParameterDirection.Input;
					_with33.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, SRet_Value).Direction = ParameterDirection.Output;
					RAF = Cmd.ExecuteNonQuery();
					if (string.IsNullOrEmpty(JCPKs)) {
						JCPKs = Convert.ToString(MJCPKArray.GetValue(i));
					} else {
						JCPKs = JCPKs + "," + MJCPKArray.GetValue(i);
					}
				}
			} catch (OracleException oraexp) {
				if (oraexp.ErrorCode == 20999) {
					arrMessage.Add("20999");
					TRAN.Rollback();
				} else {
					arrMessage.Add(oraexp.Message);
					TRAN.Rollback();
				}
			} catch (Exception ex) {
				throw ex;
				TRAN.Rollback();
			}
			if (arrMessage.Count > 0) {
				TRAN.Rollback();
				return arrMessage;

			} else {
				TRAN.Commit();
				arrMessage.Add("Saved");
				//Push to financial system if realtime is selected
				if (!string.IsNullOrEmpty(JCPKs)) {
					Quantum_QFOR.cls_Scheduler objSch = new Quantum_QFOR.cls_Scheduler();
					ArrayList schDtls = null;
					bool errGen = false;
					if (objSch.GetSchedulerPushType() == true) {
						//QFSIService.serFinApp objPush = new QFSIService.serFinApp();
						//try {
						//	schDtls = objSch.FetchSchDtls();
						//	//'Used to Fetch the Sch Dtls
						//	objPush.UpdateCostCentre(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, schDtls[5].ToString().ToUpper(), schDtls[0].ToString().ToUpper(), , , JCPKs);
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
			}
		}
		#endregion
	}
}
