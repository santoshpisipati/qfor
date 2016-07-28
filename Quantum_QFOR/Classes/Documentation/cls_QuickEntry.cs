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
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class clsQuickEntry : CommonFeatures
    {
        Cls_BookingEntry objVesselVoyage = new Cls_BookingEntry();

        #region "Get container data"
        public DataSet GetContainerData(string jobCardPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT CONT_TRN.JOB_TRN_CONT_PK,");
            sb.Append("       CONT_TRN.CONTAINER_TYPE_MST_FK,");
            sb.Append("       CONT_TRN.CONTAINER_NUMBER,");
            sb.Append("       CONT_TRN.SEAL_NUMBER,");
            //Added by Arun to Fetch the Commodity Details if it is BBC
            sb.Append(" CASE WHEN JOB.CARGO_TYPE=4 THEN ");
            sb.Append(" COMM.COMMODITY_NAME ELSE '' END");
            sb.Append("       FETCH_COMM,");
            sb.Append("       CONT_TRN.COMMODITY_MST_FK,");
            sb.Append("       CONT_TRN.COMMODITY_MST_FKS,");
            sb.Append("       CONT_TRN.PACK_TYPE_MST_FK,");
            sb.Append("       CONT_TRN.PACK_COUNT,");
            sb.Append("       CONT_TRN.VOLUME_IN_CBM,");
            sb.Append("       CONT_TRN.GROSS_WEIGHT,");
            sb.Append("       CONT_TRN.NET_WEIGHT,");
            sb.Append("       CONT_TRN.CHARGEABLE_WEIGHT,");
            sb.Append("       TO_CHAR(CONT_TRN.LOAD_DATE,DATETIMEFORMAT24)gen_land_date,");
            sb.Append("       CTMT.CONTAINER_TYPE_MST_ID,");
            sb.Append("       'true' sel");
            sb.Append("  FROM JOB_CARD_TRN   JOB,");
            sb.Append("       JOB_TRN_CONT   CONT_TRN,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CTMT,");
            sb.Append("       PACK_TYPE_MST_TBL      PTMT,");
            sb.Append("       COMMODITY_MST_TBL      COMM");
            sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = CONT_TRN.JOB_CARD_TRN_FK(+)");
            sb.Append("   AND CONT_TRN.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND CONT_TRN.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK(+)");
            sb.Append("   AND CONT_TRN.COMMODITY_MST_FK = COMM.COMMODITY_MST_PK(+)");
            sb.Append("   AND JOB.JOB_CARD_TRN_PK=" + jobCardPK);
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
        #endregion

        #region "Get Freight data"
        public DataSet GetFreightData(string jobCardPK = "0", string CargoType = "1")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder();

            sb.Append("   SELECT");
            sb.Append("   DISTINCT JOB_TRN_FD_PK,");
            //sb.Append("   container_type_mst_fk,")
            if (CargoType == "4")
            {
                sb.Append("    TO_CHAR(COMM.COMMODITY_NAME) CONTAINER_TYPE_MST_FK,");
            }
            else
            {
                if (jobCardPK != "0")
                {
                    sb.Append("    FD_TRN.CONTAINER_TYPE_MST_FK,");
                    //sb.Append("    (SELECT C.CONTAINER_TYPE_MST_ID FROM CONTAINER_TYPE_MST_TBL C WHERE ")
                    //sb.Append("    C.CONTAINER_TYPE_MST_PK=FD_TRN.CONTAINER_TYPE_MST_FK) CONTAINER_TYPE_MST_FK,")
                }
                else
                {
                    sb.Append("    FD_TRN.CONTAINER_TYPE_MST_FK,");
                }
            }
            //sb.Append("  (CASE  WHEN JOB_CARD.CARGO_TYPE = 1 OR JOB_CARD.CARGO_TYPE = 2 THEN ")
            //sb.Append("    FD_TRN.CONTAINER_TYPE_MST_FK")
            //sb.Append("   ELSE TO_NUMBER(COMM.COMMODITY_MST_PK)")
            //sb.Append("    END) CONTAINER_TYPE_MST_FK,")
            sb.Append("   frt.freight_element_id,");
            sb.Append("   frt.freight_element_name,");
            sb.Append("   frt.freight_element_mst_pk,");
            sb.Append("   fd_trn.basis,");
            sb.Append("   fd_trn.quantity,");
            //sb.Append("   DECODE(fd_trn.freight_type,1,'Prepaid',2,'Collect') freight_type,")
            sb.Append("   fd_trn.freight_type,");
            sb.Append("   fd_trn.location_mst_fk,");
            sb.Append("   loc.location_id,");
            sb.Append("   fd_trn.frtpayer_cust_mst_fk,");
            sb.Append("   cus.customer_id,");
            sb.Append("   fd_trn.freight_amt,");
            sb.Append("   fd_trn.currency_mst_fk,");
            sb.Append("   fd_trn.exchange_rate roe,");
            sb.Append("   (fd_trn.freight_amt*fd_trn.exchange_rate) total_amt,");
            sb.Append("   '0' \"Delete\", ");
            if (CargoType == "4")
            {
                sb.Append("  COMM.COMMODITY_MST_PK,");
                sb.Append("  JCT.JOB_TRN_CONT_PK, ");
            }
            else
            {
                sb.Append("  0 COMMODITY_MST_PK,");
                sb.Append("  0 JOB_TRN_CONT_PK,");
            }
            sb.Append("   CONT.PREFERENCES, ");
            sb.Append("   FRT.PREFERENCE ");
            sb.Append(" FROM");
            sb.Append("   JOB_TRN_FD fd_trn,");
            sb.Append("   JOB_CARD_TRN job_card,");
            sb.Append("   currency_type_mst_tbl curr,");
            sb.Append("   freight_element_mst_tbl frt,");
            sb.Append("    parameters_tbl prm,");
            sb.Append("   container_type_mst_tbl cont,");
            sb.Append("   location_mst_tbl loc,");
            sb.Append("   customer_mst_tbl cus");
            if (CargoType == "4")
            {
                sb.Append("  , JOB_TRN_CONT    JCT,");
                sb.Append("   COMMODITY_MST_TBL       COMM");
            }

            sb.Append("   WHERE");
            sb.Append("   fd_trn.JOB_CARD_TRN_FK = " + jobCardPK);
            sb.Append("   AND fd_trn.JOB_CARD_TRN_FK = job_card.JOB_CARD_TRN_PK");
            sb.Append("   AND fd_trn.container_type_mst_fk = cont.container_type_mst_pk(+)");
            sb.Append("   AND fd_trn.currency_mst_fk = curr.currency_mst_pk(+)");
            sb.Append("   AND fd_trn.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            sb.Append("   AND fd_trn.frtpayer_cust_mst_fk = cus.customer_mst_pk(+)");
            sb.Append("   AND fd_trn.location_mst_fk = loc.location_mst_pk(+)");
            if (CargoType == "4")
            {
                sb.Append("   AND JCT.JOB_TRN_CONT_PK = FD_TRN.JOB_TRN_CONT_FK ");
                sb.Append("   AND JOB_CARD.JOB_CARD_TRN_PK=JCT.JOB_CARD_TRN_FK");
                sb.Append("   AND JCT.COMMODITY_MST_FK=COMM.COMMODITY_MST_PK(+)");
            }
            if (CargoType == "4")
            {
                sb.Append("  ORDER BY JCT.JOB_TRN_CONT_PK,FRT.PREFERENCE ");
            }
            else
            {
                sb.Append("  ORDER BY CONT.PREFERENCES, FRT.PREFERENCE");
            }

            //'New For Freight Element Preference
            System.Text.StringBuilder sb1 = new System.Text.StringBuilder();
            sb1.Append("SELECT Q.JOB_TRN_FD_PK,");
            sb1.Append("       Q.CONTAINER_TYPE_MST_FK,");
            sb1.Append("       Q.FREIGHT_ELEMENT_ID,");
            sb1.Append("       Q.FREIGHT_ELEMENT_NAME,");
            sb1.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
            sb1.Append("       Q.BASIS,");
            sb1.Append("       Q.QUANTITY,");
            sb1.Append("       DECODE(Q.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') FREIGHT_TYPE,");
            sb1.Append("       Q.LOCATION_MST_FK \"location_fk\",");
            sb1.Append("       Q.LOCATION_ID \"location_id\",");
            sb1.Append("       Q.FRTPAYER_CUST_MST_FK \"frtpayer_mst_fk\",");
            sb1.Append("       Q.CUSTOMER_ID \"frtpayer\",");
            sb1.Append("       Q.FREIGHT_AMT,");
            sb1.Append("       Q.CURRENCY_MST_FK,");
            sb1.Append("       Q.ROE ROE,");
            sb1.Append("       (Q.FREIGHT_AMT * Q.ROE) TOTAL_AMT,");
            sb1.Append("       '0' \"Delete\",");
            sb1.Append("       0 COMMODITY_MST_PK,");
            sb1.Append("       0 JOB_TRN_CONT_PK");
            sb1.Append("  FROM ");
            sb1.Append("  (" + sb.ToString() + " ");
            sb1.Append("  ) Q  ");
            //'End
            try
            {
                return objWF.GetDataSet(sb1.ToString());
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

        #region "Get Other Charge Details"
        public DataSet FillJobCardOtherChargesDataSet(string pk = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            sb.Append("         SELECT");
            sb.Append("   oth_chrg.JOB_TRN_OTH_PK,");
            sb.Append("   frt.freight_element_mst_pk,");
            sb.Append("   frt.freight_element_id,");
            sb.Append("   frt.freight_element_name,");
            sb.Append("   curr.currency_mst_pk,");
            sb.Append("   DECODE(oth_chrg.freight_type,1,'Prepaid',2,'Collect') PaymentType, ");
            sb.Append("   oth_chrg.location_mst_fk  \"location_fk\" ,");
            sb.Append("   loc.location_id \"location_id\" ,");
            sb.Append("   oth_chrg.frtpayer_cust_mst_fk \"frtpayer_mst_fk\" ,");
            sb.Append("   cus.customer_id \"frtpayer\",");
            sb.Append("   oth_chrg.EXCHANGE_RATE \"ROE\",");
            sb.Append("   oth_chrg.amount amount,");
            sb.Append("   'false' \"Delete\"");
            sb.Append("   FROM");
            sb.Append("     JOB_TRN_OTH_CHRG oth_chrg,");
            sb.Append("     JOB_CARD_TRN jobcard_mst,");
            sb.Append("     freight_element_mst_tbl frt,");
            sb.Append("     currency_type_mst_tbl curr,");
            sb.Append("     location_mst_tbl loc,");
            sb.Append("     customer_mst_tbl cus");
            sb.Append("   WHERE");
            sb.Append("   oth_chrg.JOB_CARD_TRN_FK = jobcard_mst.JOB_CARD_TRN_PK");
            sb.Append("   AND oth_chrg.freight_element_mst_fk = frt.freight_element_mst_pk(+)");
            sb.Append("   AND oth_chrg.currency_mst_fk        = curr.currency_mst_pk(+)");
            sb.Append("   AND oth_chrg.JOB_CARD_TRN_FK    = " + pk);
            sb.Append("   AND oth_chrg.frtpayer_cust_mst_fk = cus.customer_mst_pk(+) ");
            sb.Append("   AND oth_chrg.location_mst_fk = loc.location_mst_pk(+) ");
            sb.Append(" ORDER BY freight_element_id ");
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
        #endregion

        #region "Fetch Main Jobcard for export"
        public DataSet FetchMainJobCardData(Int32 MasterJCPk = 0,Int32 CurrentPage = 0,Int32 TotalPage = 0)
        {

            System.Text.StringBuilder strSQL = new System.Text.StringBuilder(5000);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            strSQL.Append(" SELECT");
            strSQL.Append("    JOB_IMP.JOB_CARD_TRN_PK JOB_CARD_TRN_PK,");
            strSQL.Append("    JOB_IMP.JOBCARD_REF_NO,");
            strSQL.Append("    JOB_IMP.JOBCARD_DATE,");
            strSQL.Append("    JOB_IMP.JOB_CARD_STATUS,");
            strSQL.Append("    JOB_IMP.JOB_CARD_CLOSED_ON,");
            strSQL.Append("    JOB_IMP.REMARKS,");
            strSQL.Append("    JOB_IMP.CARGO_TYPE,");
            strSQL.Append("    JOB_IMP.CUST_CUSTOMER_MST_FK,");
            strSQL.Append("    CUST.CUSTOMER_ID \"CUSTOMER_ID\",");
            strSQL.Append("    CUST.CUSTOMER_NAME \"CUSTOMER_NAME\",");
            strSQL.Append("    JOB_IMP.POO_FK,");
            //'
            //strSQL.Append("    POO.PORT_ID POOID,") ''
            //strSQL.Append("    POO.PORT_NAME POONAME,") ''
            strSQL.Append("    POO.PLACE_CODE POOID,");
            //'
            strSQL.Append("    POO.PLACE_NAME POONAME,");
            //'
            strSQL.Append("    CASE WHEN JOB_IMP.CARGO_TYPE = 1 THEN JOB_IMP.PFD_FK");
            strSQL.Append("    ELSE JOB_IMP.DEL_PLACE_MST_FK END DEL_PLACE_MST_FK,");
            strSQL.Append("    CASE WHEN JOB_IMP.CARGO_TYPE = 1 THEN UPPER(PFD.PLACE_NAME)");
            strSQL.Append("    ELSE UPPER(DEL_PLACE.PLACE_NAME) END \"DELIVERYPLACE\",");
            strSQL.Append("    CASE WHEN JOB_IMP.CARGO_TYPE = 1 THEN UPPER(PFD.PLACE_CODE)");
            //'
            strSQL.Append("    ELSE UPPER(DEL_PLACE.PLACE_CODE) END \"PLACECODE\",");
            //'
            strSQL.Append("    JOB_IMP.PORT_MST_POL_FK,");
            strSQL.Append("    POL.PORT_ID \"POL\",");
            strSQL.Append("    POL.PORT_NAME \"POLNAME\",");
            //'
            strSQL.Append("    JOB_IMP.PORT_MST_POD_FK,");
            strSQL.Append("    POD.PORT_ID \"POD\",");
            strSQL.Append("    POD.PORT_NAME \"PODNAME\",");
            //'
            strSQL.Append("    JOB_IMP.CARRIER_MST_FK OPERATOR_MST_FK,");
            strSQL.Append("    OPRATOR.OPERATOR_ID \"OPERATOR_ID\",");
            strSQL.Append("    OPRATOR.OPERATOR_NAME \"OPERATOR_NAME\",");
            strSQL.Append("    VVT.VOYAGE_TRN_PK \"VOYAGEPK\",");
            strSQL.Append("    V.VESSEL_ID \"VESSEL_ID\",   ");
            strSQL.Append("    V.VESSEL_NAME \"VESSEL_NAME\",");
            strSQL.Append("    VVT.VOYAGE \"VOYAGE\",");
            strSQL.Append("    TO_CHAR(JOB_IMP.ETA_DATE,DATETIMEFORMAT24)ETA_DATE , ");
            strSQL.Append("    TO_CHAR(JOB_IMP.ETD_DATE,DATETIMEFORMAT24) ETD_DATE, ");
            strSQL.Append("    TO_CHAR(JOB_IMP.ARRIVAL_DATE,DATETIMEFORMAT24) ARRIVAL_DATE, ");
            strSQL.Append("    TO_CHAR(JOB_IMP.DEPARTURE_DATE,DATETIMEFORMAT24) DEPARTURE_DATE, ");
            strSQL.Append("    JOB_IMP.SHIPPER_CUST_MST_FK,");
            strSQL.Append("    SHIPPER.CUSTOMER_ID \"SHIPPER\",");
            strSQL.Append("    SHIPPER.CUSTOMER_NAME \"SHIPPERNAME\",");
            strSQL.Append("    JOB_IMP.CONSIGNEE_CUST_MST_FK,");
            strSQL.Append("    CONSIGNEE.CUSTOMER_ID \"CONSIGNEE\",");
            strSQL.Append("    CONSIGNEE.CUSTOMER_NAME \"CONSIGNEENAME\",");
            strSQL.Append("    NOTIFY1_CUST_MST_FK,");
            strSQL.Append("    NOTIFY1.CUSTOMER_ID \"NOTIFY1\",");
            strSQL.Append("    NOTIFY1.CUSTOMER_NAME \"NOTIFY1NAME\",");
            strSQL.Append("    NOTIFY2_CUST_MST_FK,");
            strSQL.Append("    NOTIFY2.CUSTOMER_ID \"NOTIFY2\",");
            strSQL.Append("    NOTIFY2.CUSTOMER_NAME \"NOTIFY2NAME\",");
            strSQL.Append("    JOB_IMP.CB_AGENT_MST_FK,");
            strSQL.Append("    CBAGNT.AGENT_ID \"CBAGENT\",");
            strSQL.Append("    CBAGNT.AGENT_NAME \"CBAGENTNAME\",");
            strSQL.Append("    JOB_IMP.CL_AGENT_MST_FK,");
            strSQL.Append("    CLAGNT.AGENT_ID \"CLAGENT\",");
            strSQL.Append("    CLAGNT.AGENT_NAME \"CLAGENTNAME\",");
            strSQL.Append("    JOB_IMP.VERSION_NO,");
            strSQL.Append("    JOB_IMP.UCR_NO,");
            strSQL.Append("    JOB_IMP.GOODS_DESCRIPTION,");
            strSQL.Append("    JOB_IMP.DEL_ADDRESS,");
            strSQL.Append("    JOB_IMP.HBL_HAWB_REF_NO HBL_REF_NO,");
            strSQL.Append("    JOB_IMP.HBL_HAWB_DATE HBL_DATE,");
            strSQL.Append("    JOB_IMP.MBL_MAWB_REF_NO MBL_REF_NO,");
            strSQL.Append("    JOB_IMP.MBL_MAWB_DATE MBL_DATE,");
            strSQL.Append("    JOB_IMP.MARKS_NUMBERS,");
            strSQL.Append("    JOB_IMP.WEIGHT_MASS,");
            strSQL.Append("    JOB_IMP.CARGO_MOVE_FK,");
            strSQL.Append("    JOB_IMP.PYMT_TYPE,");
            strSQL.Append("    JOB_IMP.SHIPPING_TERMS_MST_FK,");
            strSQL.Append("    JOB_IMP.INSURANCE_AMT,");
            strSQL.Append("    JOB_IMP.INSURANCE_CURRENCY,");
            strSQL.Append("    JOB_IMP.POL_AGENT_MST_FK,");
            strSQL.Append("    POLAGNT.AGENT_ID \"POLAGENT\",");
            strSQL.Append("    POLAGNT.AGENT_NAME \"POLAGENTNAME\", ");
            strSQL.Append("    JOB_IMP.COMMODITY_GROUP_FK,");
            strSQL.Append("    COMM.COMMODITY_GROUP_DESC,");
            strSQL.Append("    DEPOT.VENDOR_ID \"DEPOT_ID\",");
            strSQL.Append("    DEPOT.VENDOR_NAME \"DEPOT_NAME\",");
            strSQL.Append("    DEPOT.VENDOR_MST_PK \"DEPOT_PK\",");
            strSQL.Append("    CARRIER.CUSTOMER_ID \"CARRIER_ID\",");
            strSQL.Append("    CARRIER.CUSTOMER_NAME \"CARRIER_NAME\",");
            strSQL.Append("    CARRIER.CUSTOMER_MST_PK \"CARRIER_PK\",");
            strSQL.Append("    COUNTRY.COUNTRY_ID \"COUNTRY_ID\",");
            strSQL.Append("    COUNTRY.COUNTRY_NAME \"COUNTRY_NAME\",");
            strSQL.Append("    COUNTRY.COUNTRY_MST_PK \"COUNTRY_MST_PK\",");
            strSQL.Append("    JOB_IMP.DA_NUMBER \"DA_NUMBER\",");
            strSQL.Append("    JOB_IMP.CLEARANCE_ADDRESS, ");
            strSQL.Append("    JOB_IMP.JC_AUTO_MANUAL,JOB_IMP.HBL_HAWB_SURRENDERED HBLSURR,JOB_IMP.MBL_MAWB_SURRENDERED MBLSURR,");
            strSQL.Append("    JOB_IMP.MBL_MAWB_SURRDT MBLSURRDT,JOB_IMP.HBL_HAWB_SURRDT HBLSURRDT, ");
            strSQL.Append("    JOB_IMP.SB_NUMBER,JOB_IMP.SB_DATE, ");
            strSQL.Append("   CURR.CURRENCY_ID,JOB_IMP.PAGE_NR,");
            //'
            strSQL.Append("   CURR.CURRENCY_MST_PK BASE_CURRENCY_FK,");
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
            strSQL.Append(" FROM ");
            strSQL.Append("    JOB_CARD_TRN JOB_IMP,");
            strSQL.Append("    PORT_MST_TBL POD,");
            strSQL.Append("    PORT_MST_TBL POL,");
            //strSQL.Append("    PORT_MST_TBL PFD,")
            //strSQL.Append("    PORT_MST_TBL POO,")
            strSQL.Append("    PLACE_MST_TBL PFD,");
            strSQL.Append("    PLACE_MST_TBL POO,");
            strSQL.Append("    CUSTOMER_MST_TBL CUST,");
            strSQL.Append("    CUSTOMER_MST_TBL CONSIGNEE,");
            strSQL.Append("    CUSTOMER_MST_TBL SHIPPER,");
            strSQL.Append("    CUSTOMER_MST_TBL NOTIFY1,");
            strSQL.Append("    CUSTOMER_MST_TBL NOTIFY2,");
            strSQL.Append("    PLACE_MST_TBL DEL_PLACE,");
            strSQL.Append("    OPERATOR_MST_TBL OPRATOR,");
            strSQL.Append("    AGENT_MST_TBL CLAGNT, ");
            strSQL.Append("    AGENT_MST_TBL CBAGNT,");
            strSQL.Append("    AGENT_MST_TBL POLAGNT,");
            strSQL.Append("    COMMODITY_GROUP_MST_TBL COMM, ");
            strSQL.Append("    VESSEL_VOYAGE_TBL V, ");
            strSQL.Append("    VESSEL_VOYAGE_TRN VVT, ");
            strSQL.Append("    VENDOR_MST_TBL  DEPOT,");
            strSQL.Append("    CUSTOMER_MST_TBL  CARRIER,");
            strSQL.Append("    COUNTRY_MST_TBL COUNTRY,");
            strSQL.Append("    CURRENCY_TYPE_MST_TBL CURR,");
            strSQL.Append("    EMPLOYEE_MST_TBL        EMP, ");
            strSQL.Append("    EMPLOYEE_MST_TBL        CONS_SE ");
            //CONSIGNEE SALES PERSON
            strSQL.Append("    WHERE ");
            strSQL.Append("    JOB_IMP.MASTER_JC_FK         = " + MasterJCPk);
            strSQL.Append("    AND JOB_IMP.PORT_MST_POL_FK          =  POL.PORT_MST_PK");
            //strSQL.Append("    AND JOB_IMP.POO_FK                   =  POO.PORT_MST_PK(+)") ''
            //strSQL.Append("    AND PFD.PORT_MST_PK(+) = JOB_IMP.PFD_FK ")
            strSQL.Append("    AND JOB_IMP.POO_FK                   =  POO.PLACE_PK(+)");
            //'
            strSQL.Append("    AND PFD.PLACE_PK(+) = JOB_IMP.PFD_FK ");
            strSQL.Append("    AND JOB_IMP.PORT_MST_POD_FK          =  POD.PORT_MST_PK");
            strSQL.Append("    AND JOB_IMP.DEL_PLACE_MST_FK         =  DEL_PLACE.PLACE_PK(+)");
            strSQL.Append("    AND JOB_IMP.CUST_CUSTOMER_MST_FK     =  CUST.CUSTOMER_MST_PK(+) ");
            strSQL.Append("    AND JOB_IMP.CARRIER_MST_FK          =  OPRATOR.OPERATOR_MST_PK");
            strSQL.Append("    AND JOB_IMP.SHIPPER_CUST_MST_FK      =  SHIPPER.CUSTOMER_MST_PK(+)");
            strSQL.Append("    AND JOB_IMP.CONSIGNEE_CUST_MST_FK    =  CONSIGNEE.CUSTOMER_MST_PK(+)");
            strSQL.Append("    AND JOB_IMP.NOTIFY1_CUST_MST_FK      =  NOTIFY1.CUSTOMER_MST_PK(+)");
            strSQL.Append("    AND JOB_IMP.NOTIFY2_CUST_MST_FK      =  NOTIFY2.CUSTOMER_MST_PK(+)");
            strSQL.Append("    AND JOB_IMP.CL_AGENT_MST_FK          =  CLAGNT.AGENT_MST_PK(+)");
            strSQL.Append("    AND JOB_IMP.CB_AGENT_MST_FK          =  CBAGNT.AGENT_MST_PK(+)");
            strSQL.Append("    AND JOB_IMP.POL_AGENT_MST_FK         =  POLAGNT.AGENT_MST_PK(+)");
            strSQL.Append("    AND JOB_IMP.COMMODITY_GROUP_FK       =  COMM.COMMODITY_GROUP_PK(+)");
            strSQL.Append("    AND JOB_IMP.TRANSPORTER_DEPOT_FK     =  DEPOT.VENDOR_MST_PK(+)");
            strSQL.Append("    AND JOB_IMP.TRANSPORTER_CARRIER_FK   =  CARRIER.CUSTOMER_MST_PK(+)");
            strSQL.Append("    AND JOB_IMP.COUNTRY_ORIGIN_FK        =  COUNTRY.COUNTRY_MST_PK(+)");
            strSQL.Append("    AND VVT.VESSEL_VOYAGE_TBL_FK         = V.VESSEL_VOYAGE_TBL_PK(+)");
            strSQL.Append("    AND JOB_IMP.VOYAGE_TRN_FK            = VVT.VOYAGE_TRN_PK(+)");
            strSQL.Append("    AND consignee.REP_EMP_MST_FK=CONS_SE.EMPLOYEE_MST_PK(+) ");
            strSQL.Append("    AND JOB_IMP.EXECUTIVE_MST_FK=EMP.EMPLOYEE_MST_PK(+) ");
            strSQL.Append("    AND CURR.CURRENCY_MST_PK(+) = JOB_IMP.BASE_CURRENCY_MST_FK");

            Int16 RecPerPage = 1;
            TotalRecords = TotalPage;
            TotalPage = TotalRecords / RecPerPage;
            if (TotalRecords % RecPerPage != 0)
            {
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
            try
            {
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
        #endregion

        #region "Save Function"
        public ArrayList Save(ref DataSet M_DataSet,DataSet dsContainerData,DataSet dsFreightDetails,bool isEdting,string jobCardRefNumber,string userLocation,string employeeID,long JobCardPK,DataSet dsOtherCharges,int PageNr,
       long MasterJCPk, string POOPK = "", string CargoType = "")
        {
            objVesselVoyage.ConfigurationPK = M_Configuration_PK;
            objVesselVoyage.CREATED_BY = M_CREATED_BY_FK;
            arrMessage.Clear();
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            OracleTransaction TRAN1 = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            Int16 intIns = default(Int16);
            string str = null;
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            OracleCommand insContainerDetails = new OracleCommand();
            OracleCommand updContainerDetails = new OracleCommand();

            OracleCommand insFreightDetails = new OracleCommand();
            OracleCommand updFreightDetails = new OracleCommand();
            OracleCommand delFreightDetails = new OracleCommand();

            OracleCommand insOtherChargesDetails = new OracleCommand();
            OracleCommand updOtherChargesDetails = new OracleCommand();
            OracleCommand delOtherChargesDetails = new OracleCommand();

            //If isEdting = False Then
            //    jobCardRefNumber = GenerateProtocolKey("JOB CARD IMP (SEA)", userLocation, employeeID, Date.Now, , , , M_Last_Modified_By_Fk)
            //End If
            try
            {
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_CARD_TRN_TBL_INS";
                var _with2 = _with1.Parameters;

                insCommand.Parameters.Add("JOBCARD_REF_NO_IN", jobCardRefNumber).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("JOBCARD_DATE_IN", OracleDbType.Date, 20, "jobcard_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOBCARD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("JOB_CARD_STATUS_IN", 1).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("JOB_CARD_CLOSED_ON_IN", OracleDbType.Date, 20, "job_card_closed_on").Direction = ParameterDirection.Input;
                insCommand.Parameters["JOB_CARD_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "remarks").Direction = ParameterDirection.Input;
                insCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "cargo_type").Direction = ParameterDirection.Input;
                insCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

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

                insCommand.Parameters.Add("CARRIER_MST_FK_IN", OracleDbType.Int32, 10, "operator_mst_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CARRIER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "vessel_name").Direction = ParameterDirection.Input;
                insCommand.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VOYAGE_IN", OracleDbType.Varchar2, 10, "voyage").Direction = ParameterDirection.Input;
                insCommand.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VOYAGE_FK_IN", OracleDbType.Int32, 10, "VoyagePK").Direction = ParameterDirection.Input;
                insCommand.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["ETA_DATE"].ToString()))
                {
                    insCommand.Parameters.Add("ETA_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("ETA_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["ETA_DATE"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["ETD_DATE"].ToString()))
                {
                    insCommand.Parameters.Add("ETD_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("ETD_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["ETD_DATE"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["arrival_date"].ToString()))
                {
                    insCommand.Parameters.Add("ARRIVAL_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("ARRIVAL_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["arrival_date"])).Direction = ParameterDirection.Input;
                }
                insCommand.Parameters["ARRIVAL_DATE_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["departure_date"].ToString()))
                {
                    insCommand.Parameters.Add("DEPARTURE_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insCommand.Parameters.Add("DEPARTURE_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["departure_date"])).Direction = ParameterDirection.Input;
                }
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

                insCommand.Parameters.Add("WEIGHT_MASS_IN", OracleDbType.Varchar2, 2000, "weight_mass").Direction = ParameterDirection.Input;
                insCommand.Parameters["WEIGHT_MASS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("UCR_NO_IN", OracleDbType.Varchar2, 40, "UCR_NO").Direction = ParameterDirection.Input;
                insCommand.Parameters["UCR_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("HBL_HAWB_REF_NO_IN", OracleDbType.Varchar2, 20, "hbl_ref_no").Direction = ParameterDirection.Input;
                insCommand.Parameters["HBL_HAWB_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("HBL_HAWB_DATE_IN", OracleDbType.Date, 20, "hbl_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["HBL_HAWB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MBL_MAWB_REF_NO_IN", OracleDbType.Varchar2, 20, "mbl_ref_no").Direction = ParameterDirection.Input;
                insCommand.Parameters["MBL_MAWB_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MBL_MAWB_DATE_IN", OracleDbType.Date, 20, "mbl_date").Direction = ParameterDirection.Input;
                insCommand.Parameters["MBL_MAWB_DATE_IN"].SourceVersion = DataRowVersion.Current;

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

                insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_PK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;


                insCommand.Parameters.Add("HBL_HAWB_SURR_IN", OracleDbType.Int32, 1, "HBLSURR").Direction = ParameterDirection.Input;
                insCommand.Parameters["HBL_HAWB_SURR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MBL_MAWB_SURR_IN", OracleDbType.Int32, 1, "MBLSURR").Direction = ParameterDirection.Input;
                insCommand.Parameters["MBL_MAWB_SURR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("HBL_HAWB_SURR_DT_IN", OracleDbType.Date, 20, "HBLSURRDT").Direction = ParameterDirection.Input;
                insCommand.Parameters["HBL_HAWB_SURR_DT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MBL_MAWB_SURR_DT_IN", OracleDbType.Date, 20, "MBLSURRDT").Direction = ParameterDirection.Input;
                insCommand.Parameters["MBL_MAWB_SURR_DT_IN"].SourceVersion = DataRowVersion.Current;

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

                //nomination parameters
                insCommand.Parameters.Add("CHK_CSR_IN", OracleDbType.Int32, 1, "CHK_CSR").Direction = ParameterDirection.Input;
                insCommand.Parameters["CHK_CSR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("EXECUTIVE_MST_FK_IN", OracleDbType.Int32, 10, "SALES_EXEC_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["EXECUTIVE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                //-----------------------------------------------------------------------------------
                insCommand.Parameters.Add("JC_AUTO_MANUAL_IN", 0).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("PROCESS_TYPE_IN", 2).Direction = ParameterDirection.Input;
                insCommand.Parameters.Add("BUSINESS_TYPE_IN", 2).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_CARD_TRN_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_CARD_TRN_TBL_UPD";
                var _with4 = _with3.Parameters;
                updCommand.Parameters.Add("JOB_CARD_TRN_PK_IN", OracleDbType.Int32, 10, "JOB_CARD_TRN_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_TRN_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOBCARD_REF_NO_IN", jobCardRefNumber).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("JOBCARD_DATE_IN", OracleDbType.Date, 20, "jobcard_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOBCARD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOB_CARD_STATUS_IN", OracleDbType.Int32, 1, "job_card_status").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("JOB_CARD_CLOSED_ON_IN", OracleDbType.Date, 20, "job_card_closed_on").Direction = ParameterDirection.Input;
                updCommand.Parameters["JOB_CARD_CLOSED_ON_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 100, "remarks").Direction = ParameterDirection.Input;
                updCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VESSEL_NAME_IN", OracleDbType.Varchar2, 25, "vessel_name").Direction = ParameterDirection.Input;
                updCommand.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VOYAGE_IN", OracleDbType.Varchar2, 10, "voyage").Direction = ParameterDirection.Input;
                updCommand.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VOYAGE_FK_IN", OracleDbType.Int32, 10, "VoyagePK").Direction = ParameterDirection.Input;
                updCommand.Parameters["VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;

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

                updCommand.Parameters.Add("UCR_NO_IN", OracleDbType.Varchar2, 40, "ucr_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["UCR_NO_IN"].SourceVersion = DataRowVersion.Current;

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


                updCommand.Parameters.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200, "del_address").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MARKS_NUMBERS_IN", OracleDbType.Varchar2, 2000, "marks_numbers").Direction = ParameterDirection.Input;
                updCommand.Parameters["MARKS_NUMBERS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("GOODS_DESCRIPTION_IN", OracleDbType.Varchar2, 4000, "goods_description").Direction = ParameterDirection.Input;
                updCommand.Parameters["GOODS_DESCRIPTION_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 4, "version_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SB_DATE_IN", OracleDbType.Date, 20, "sb_date").Direction = ParameterDirection.Input;
                updCommand.Parameters["SB_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("SB_NO_IN", OracleDbType.Varchar2, 20, "sb_number").Direction = ParameterDirection.Input;
                updCommand.Parameters["SB_NO_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LC_SHIPMENT_IN", OracleDbType.Int32, 1, "LC_SHIPMENT").Direction = ParameterDirection.Input;
                updCommand.Parameters["LC_SHIPMENT_IN"].SourceVersion = DataRowVersion.Current;

                //'nomination parameters
                updCommand.Parameters.Add("CHK_CSR_IN", OracleDbType.Int32, 1, "CHK_CSR").Direction = ParameterDirection.Input;
                updCommand.Parameters["CHK_CSR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("EXECUTIVE_MST_FK_IN", OracleDbType.Int32, 10, "SALES_EXEC_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["EXECUTIVE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PROCESS_TYPE_IN", 2).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("BUSINESS_TYPE_IN", 2).Direction = ParameterDirection.Input;
                updCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "cargo_type").Direction = ParameterDirection.Input;
                updCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10, "cust_customer_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CUST_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PORT_MST_POL_FK_IN", OracleDbType.Int32, 10, "port_mst_pol_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PORT_MST_POD_FK_IN", OracleDbType.Int32, 10, "port_mst_pod_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CARRIER_MST_FK_IN", OracleDbType.Int32, 10, "operator_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CARRIER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WEIGHT_MASS_IN", OracleDbType.Varchar2, 2000, "weight_mass").Direction = ParameterDirection.Input;
                updCommand.Parameters["WEIGHT_MASS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("HBL_HAWB_REF_NO_IN", OracleDbType.Varchar2, 20, "hbl_ref_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["HBL_HAWB_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["hbl_date"].ToString()))
                {
                    updCommand.Parameters.Add("HBL_HAWB_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("HBL_HAWB_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["hbl_date"])).Direction = ParameterDirection.Input;
                }

                updCommand.Parameters.Add("MBL_MAWB_REF_NO_IN", OracleDbType.Varchar2, 20, "mbl_ref_no").Direction = ParameterDirection.Input;
                updCommand.Parameters["MBL_MAWB_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["mbl_date"].ToString()))
                {
                    updCommand.Parameters.Add("MBL_MAWB_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updCommand.Parameters.Add("MBL_MAWB_DATE_IN", Convert.ToDateTime(M_DataSet.Tables[0].Rows[0]["mbl_date"])).Direction = ParameterDirection.Input;
                }

                updCommand.Parameters.Add("CLEARANCE_ADDRESS_IN", OracleDbType.Varchar2, 200, "clearance_address").Direction = ParameterDirection.Input;
                updCommand.Parameters["CLEARANCE_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("HBL_HAWB_SURR_IN", OracleDbType.Int32, 1, "HBLSURR").Direction = ParameterDirection.Input;
                updCommand.Parameters["HBL_HAWB_SURR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MBL_MAWB_SURR_IN", OracleDbType.Int32, 1, "MBLSURR").Direction = ParameterDirection.Input;
                updCommand.Parameters["MBL_MAWB_SURR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("HBL_HAWB_SURR_DT_IN", OracleDbType.Date, 20, "HBLSURRDT").Direction = ParameterDirection.Input;
                updCommand.Parameters["HBL_HAWB_SURR_DT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MBL_MAWB_SURR_DT_IN", OracleDbType.Date, 20, "MBLSURRDT").Direction = ParameterDirection.Input;
                updCommand.Parameters["MBL_MAWB_SURR_DT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BASE_CURRENCY_FK_IN", OracleDbType.Int32, 10, "base_currency_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["BASE_CURRENCY_FK_IN"].SourceVersion = DataRowVersion.Current;

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

                updCommand.Parameters.Add("POL_AGENT_MST_FK_IN", OracleDbType.Int32, 10, "pol_agent_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["POL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DEL_PLACE_MST_FK_IN", OracleDbType.Int32, 10, "del_place_mst_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                //------------------------------------------------------------------------------------
                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_CARD_TRN_PK").Direction = ParameterDirection.Output;
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
                        RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                        //Added by sivachandran - To Rollback Protocal Nr when the Transaction failed.
                    }
                    return arrMessage;
                }
                else
                {
                    if (isEdting == false)
                    {
                        JobCardPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    }
                }
                ///
                if (JobCardPK != 0)
                {
                    OracleCommand updCmdUser = new OracleCommand();
                    updCmdUser.Transaction = TRAN;
                    str = " update JOB_CARD_TRN job set job.page_nr=" + PageNr;
                    str += " ,job.CONSOLE = 2";
                    if (MasterJCPk != 0)
                    {
                        str += " ,job.MASTER_JC_FK=" + MasterJCPk;
                    }
                    if (!string.IsNullOrEmpty(POOPK) & (POOPK != null))
                    {
                        str += " ,job.Poo_Fk=" + POOPK;
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
                _with7.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_CONT_INS";
                var _with8 = _with7.Parameters;
                insContainerDetails.Parameters.Add("JOB_CARD_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 16, "container_number").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "seal_number").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

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

                insContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Varchar2, 200, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "gen_land_date").Direction = ParameterDirection.Input;
                insContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                insContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_CONT_PK").Direction = ParameterDirection.Output;
                insContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with9 = updContainerDetails;
                _with9.Connection = objWK.MyConnection;
                _with9.CommandType = CommandType.StoredProcedure;
                _with9.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_CONT_UPD";
                var _with10 = _with9.Parameters;
                updContainerDetails.Parameters.Add("JOB_TRN_CONT_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_CONT_PK").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["JOB_TRN_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("JOB_CARD_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", OracleDbType.Varchar2, 16, "container_number").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("SEAL_NUMBER_IN", OracleDbType.Varchar2, 20, "seal_number").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

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

                updContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", OracleDbType.Varchar2, 200, "COMMODITY_MST_FKS").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("LOAD_DATE_IN", OracleDbType.Date, 20, "gen_land_date").Direction = ParameterDirection.Input;
                updContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                updContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with11 = objWK.MyDataAdapter;
                _with11.InsertCommand = insContainerDetails;
                _with11.InsertCommand.Transaction = TRAN;

                _with11.UpdateCommand = updContainerDetails;
                _with11.UpdateCommand.Transaction = TRAN;

                RecAfct = _with11.Update(dsContainerData.Tables[0]);

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

                var _with12 = insFreightDetails;
                _with12.Connection = objWK.MyConnection;
                _with12.CommandType = CommandType.StoredProcedure;
                if (Convert.ToInt32(CargoType) == 4)
                {
                    _with12.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_FD_INS";
                }
                else
                {
                    _with12.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_FD_INS";
                }

                var _with13 = _with12.Parameters;

                if (Convert.ToInt32(CargoType) == 4)
                {
                    insFreightDetails.Parameters.Add("JOB_CARD_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    insFreightDetails.Parameters.Add("JOB_CARD_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                }


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
                if (Convert.ToInt32(CargoType) == 4)
                {
                    insFreightDetails.Parameters.Add("RATE_PERBASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                insFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 100, "roe").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                //If CargoType = 4 Then
                //    insFreightDetails.Parameters.Add("JOB_CARD_SEA_IMP_CONT_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_CONT_PK").Direction = ParameterDirection.Input
                //    insFreightDetails.Parameters("JOB_CARD_SEA_IMP_CONT_PK_IN").SourceVersion = DataRowVersion.Current
                //Else
                insFreightDetails.Parameters.Add("JOB_TRN_CONT_FK_IN", OracleDbType.Int32, 10, "JOB_TRN_CONT_PK").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["JOB_TRN_CONT_FK_IN"].SourceVersion = DataRowVersion.Current;
                // End If

                insFreightDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 50, "SURCHARGE").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_FD_PK").Direction = ParameterDirection.Output;
                insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with14 = updFreightDetails;
                _with14.Connection = objWK.MyConnection;
                _with14.CommandType = CommandType.StoredProcedure;


                if (Convert.ToInt32(CargoType )== 4)
                {
                    _with14.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_FD_UPD";
                }
                else
                {
                    _with14.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_FD_UPD";
                }
                var _with15 = _with14.Parameters;
                //If CargoType = 4 Then
                //   updFreightDetails.Parameters.Add("JOB_TRN_SEA_IMP_FD_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_FD_PK").Direction = ParameterDirection.Input
                //  updFreightDetails.Parameters("JOB_TRN_SEA_IMP_FD_PK_IN").SourceVersion = DataRowVersion.Current
                //Else
                updFreightDetails.Parameters.Add("JOB_TRN_FD_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_FD_PK").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["JOB_TRN_FD_PK_IN"].SourceVersion = DataRowVersion.Current;
                //End If
                //   If CargoType = 4 Then
                //updFreightDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input
                //Else
                updFreightDetails.Parameters.Add("JOB_CARD_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                //End If

                if (Convert.ToInt32(CargoType) == 4)
                {
                    updFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                else
                {
                    updFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                }

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

                updFreightDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with16 = delFreightDetails;
                _with16.Connection = objWK.MyConnection;
                _with16.CommandType = CommandType.StoredProcedure;
                //If CargoType = 4 Then
                //  .CommandText = objWK.MyUserName & ".JOB_CARD_TRN_PKG.JOB_TRN_FD_DEL"
                //Else
                _with16.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_FD_DEL";
                //End If

                //  If CargoType = 4 Then
                //   delFreightDetails.Parameters.Add("JOB_TRN_FD_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_FD_PK").Direction = ParameterDirection.Input
                //   delFreightDetails.Parameters("JOB_TRN_FD_PK_IN").SourceVersion = DataRowVersion.Current
                //Else
                delFreightDetails.Parameters.Add("JOB_TRN_FD_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_FD_PK").Direction = ParameterDirection.Input;
                delFreightDetails.Parameters["JOB_TRN_FD_PK_IN"].SourceVersion = DataRowVersion.Current;
                //End If

                // If CargoType = 4 Then
                //Else

                delFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                //End If

                var _with17 = objWK.MyDataAdapter;

                _with17.InsertCommand = insFreightDetails;
                _with17.InsertCommand.Transaction = TRAN;

                _with17.UpdateCommand = updFreightDetails;
                _with17.UpdateCommand.Transaction = TRAN;
                if (Convert.ToInt32(CargoType) == 4)
                {
                }
                else
                {
                    _with17.DeleteCommand = delFreightDetails;
                    _with17.DeleteCommand.Transaction = TRAN;
                }

                //RecAfct = .Update(dsFreightDetails)
                RecAfct = _with17.Update(dsFreightDetails.Tables[0]);

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

                var _with18 = insOtherChargesDetails;
                _with18.Connection = objWK.MyConnection;
                _with18.CommandType = CommandType.StoredProcedure;
                _with18.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_OTH_CHRG_INS";

                var _with19 = _with18.Parameters;
                insOtherChargesDetails.Parameters.Add("JOB_CARD_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                insOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_OTH_PK").Direction = ParameterDirection.Output;
                insOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with20 = updOtherChargesDetails;
                _with20.Connection = objWK.MyConnection;
                _with20.CommandType = CommandType.StoredProcedure;
                _with20.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_OTH_CHRG_UPD";
                var _with21 = _with20.Parameters;

                updOtherChargesDetails.Parameters.Add("JOB_TRN_OTH_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_OTH_PK").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["JOB_TRN_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("JOB_CARD_TRN_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

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

                var _with22 = delOtherChargesDetails;
                _with22.Connection = objWK.MyConnection;
                _with22.CommandType = CommandType.StoredProcedure;
                _with22.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_OTH_CHRG_DEL";

                delOtherChargesDetails.Parameters.Add("JOB_TRN_OTH_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_OTH_PK").Direction = ParameterDirection.Input;
                delOtherChargesDetails.Parameters["JOB_TRN_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                delOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with23 = objWK.MyDataAdapter;

                _with23.InsertCommand = insOtherChargesDetails;
                _with23.InsertCommand.Transaction = TRAN;

                _with23.UpdateCommand = updOtherChargesDetails;
                _with23.UpdateCommand.Transaction = TRAN;

                _with23.DeleteCommand = delOtherChargesDetails;
                _with23.DeleteCommand.Transaction = TRAN;

                RecAfct = _with23.Update(dsOtherCharges);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    if (isEdting == false)
                    {
                        RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                    }
                    return arrMessage;
                }
                else
                {
                    arrMessage.Clear();
                    arrMessage.Add("All Data Saved Successfully");
                    TRAN.Commit();
                    return arrMessage;
                }

            }
            catch (OracleException oraexp)
            {
                if (isEdting == false)
                {
                    RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                }
                throw oraexp;
            }
            catch (Exception ex)
            {
                if (isEdting == false)
                {
                    RollbackProtocolKey("JOB CARD IMP (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), jobCardRefNumber, System.DateTime.Now);
                }
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }
        #endregion

        #region "Save Commodity Details"
        public ArrayList saveCommDetails(ref DataSet dsContainerData)
        {
            arrMessage.Clear();
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand InsCommDetails = new OracleCommand();
            int nRowCnt = 0;
            int i = 0;
            int RecAfct = 0;
            try
            {
                for (nRowCnt = 0; nRowCnt <= dsContainerData.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    if (!string.IsNullOrEmpty(dsContainerData.Tables[0].Rows[nRowCnt]["COMMODITY_MST_FKS"].ToString()))
                    {
                        var _with24 = InsCommDetails;
                        _with24.Transaction = TRAN;
                        _with24.Connection = objWK.MyConnection;
                        _with24.CommandType = CommandType.StoredProcedure;
                        _with24.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOBCARD_COMMODITY_DTL_INS";
                        _with24.Parameters.Clear();
                        var _with25 = _with24.Parameters;
                        InsCommDetails.Parameters.Add("JOB_TRN_SEA_IMP_CONT_PK_IN", dsContainerData.Tables[0].Rows[nRowCnt]["JOB_TRN_CONT_PK"]).Direction = ParameterDirection.Input;
                        InsCommDetails.Parameters["JOB_TRN_SEA_IMP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;
                        InsCommDetails.Parameters.Add("COMMODITY_MST_FKS_IN", dsContainerData.Tables[0].Rows[nRowCnt]["COMMODITY_MST_FKS"]).Direction = ParameterDirection.Input;
                        InsCommDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;
                        InsCommDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOBCARD_COMM_DTL_PK").Direction = ParameterDirection.Output;
                        InsCommDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        RecAfct = _with24.ExecuteNonQuery();
                    }
                }
                if (RecAfct > 0)
                {
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
                TRAN.Rollback();
                throw oraexp;
                arrMessage.Add(oraexp.Message);
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                arrMessage.Add(ex.Message);
                throw ex;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }
        #endregion
        #region "Save MasterJobcard"
        public ArrayList SaveMjc(ref DataSet M_DataSet,string MSTJCRefNo,string Location,string EmpPk,long MSTJobCardPK,int NoOfHbls, string sid = "", string polid = "", string podid = "")
        {
            arrMessage.Clear();
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand SelectCommand = new OracleCommand();
            objWK.MyCommand.Transaction = TRAN;
            objVesselVoyage.ConfigurationPK = M_Configuration_PK;
            objVesselVoyage.CREATED_BY = M_CREATED_BY_FK;
            OracleCommand insCommand = new OracleCommand();
            bool chkflag = false;
            Int32 RecAfct = default(Int32);
            Int16 intIns = default(Int16);
            string str = null;

            MSTJCRefNo = GenerateProtocolKey("MASTER JC SEA IMPORT", Convert.ToInt64(Location), Convert.ToInt32(EmpPk), DateTime.Now,"" ,"" , polid, CREATED_BY,new WorkFlow(), sid,
            podid);
            if (MSTJCRefNo == "Protocol Not Defined.")
            {
                arrMessage.Add("Protocol Not Defined.");
                return arrMessage;
            }
            else if (MSTJCRefNo.Length > 20)
            {
                arrMessage.Add("Protocol should be less than 20 Characters");
                return arrMessage;
            }
            try
            {
                var _with26 = insCommand;
                _with26.Connection = objWK.MyConnection;
                _with26.CommandType = CommandType.StoredProcedure;
                _with26.CommandText = objWK.MyUserName + ".MASTER_JC_SEA_IMP_TBL_PKG.MASTER_JC_SEA_IMP_TBL_INS";
                var _with27 = _with26.Parameters;
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

                insCommand.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(CREATED_BY)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("OPERATOR_MST_FK_IN", OracleDbType.Int32, 10, "OPERATOR_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VOYAGE_TRN_FK_IN", OracleDbType.Int32, 10, "VOYAGE_TRN_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["VOYAGE_TRN_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("POD_ATA_IN", OracleDbType.Date, 20, "POD_ATA").Direction = ParameterDirection.Input;
                insCommand.Parameters["POD_ATA_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("COMMODITY_GROUP_FK_IN", OracleDbType.Int32, 10, "COMMODITY_GROUP_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(ConfigurationPK)).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("POD_ETA_IN", OracleDbType.Date, 20, "POD_ETA").Direction = ParameterDirection.Input;
                insCommand.Parameters["POD_ETA_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "MASTER_JC_SEA_IMP_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with28 = objWK.MyDataAdapter;
                _with28.InsertCommand = insCommand;
                _with28.InsertCommand.Transaction = TRAN;
                if ((M_DataSet.GetChanges(DataRowState.Added) != null))
                {
                    chkflag = true;
                }
                else
                {
                    chkflag = false;
                }
                RecAfct = _with28.Update(M_DataSet);
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    if (chkflag)
                    {
                        RollbackProtocolKey("MASTER JC SEA IMPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNo, System.DateTime.Now);
                    }
                    return arrMessage;
                }
                else
                {
                    MSTJobCardPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    ///
                    if (MSTJobCardPK != 0)
                    {
                        OracleCommand updCmdUser = new OracleCommand();
                        updCmdUser.Transaction = TRAN;
                        str = " update MASTER_JC_SEA_IMP_TBL MJOB set MJOB.NO_HBLS=" + NoOfHbls;
                        str += " WHERE MJOB.MASTER_JC_SEA_IMP_PK=" + MSTJobCardPK;
                        var _with29 = updCmdUser;
                        _with29.Connection = objWK.MyConnection;
                        _with29.Transaction = TRAN;
                        _with29.CommandType = CommandType.Text;
                        _with29.CommandText = str;
                        intIns = Convert.ToInt16(_with29.ExecuteNonQuery());
                    }
                    ///
                    arrMessage.Add("All Data Saved Successfully");
                    TRAN.Commit();
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                if (chkflag)
                {
                    RollbackProtocolKey("MASTER JC SEA IMPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNo, System.DateTime.Now);
                }
                throw oraexp;
            }
            catch (Exception ex)
            {
                if (chkflag)
                {
                    RollbackProtocolKey("MASTER JC SEA IMPORT", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), MSTJCRefNo, System.DateTime.Now);
                }
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }
        #endregion

        #region "Save Container Details"

        public ArrayList SaveContainerDtls(ref DataSet dsContainerData, string JobCardPK, string VslVoyPK = "", string Vessel_Name = "", string Voy = "")
        {
            arrMessage.Clear();
            Int32 RecAfct = default(Int32);
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updContainerDetails = new OracleCommand();
            int nRowCnt = 0;
            int intIns = 0;
            int i = 0;
            string str = null;
            Array JOBPK = null;
            JOBPK = JobCardPK.Split(',');
            try
            {
                if (!string.IsNullOrEmpty(VslVoyPK) & (VslVoyPK != null))
                {
                    for (i = 0; i <= JOBPK.Length - 1; i++)
                    {
                        OracleCommand updCmdUser = new OracleCommand();
                        updCmdUser.Transaction = TRAN;
                        str = " update JOB_CARD_TRN job set job.sec_voyage_trn_fk=" + VslVoyPK;
                        str += " ,job.sec_vessel_name =  '" + Vessel_Name + "'";
                        str += " ,job.sec_voyage = '" + Voy + "'";
                        str += " WHERE job.JOB_CARD_TRN_PK=" + JOBPK.GetValue(i);
                        var _with30 = updCmdUser;
                        _with30.Connection = objWK.MyConnection;
                        _with30.Transaction = TRAN;
                        _with30.CommandType = CommandType.Text;
                        _with30.CommandText = str;
                        intIns = _with30.ExecuteNonQuery();
                    }
                }

                for (nRowCnt = 0; nRowCnt <= dsContainerData.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    var _with31 = updContainerDetails;
                    _with31.Transaction = TRAN;
                    _with31.Connection = objWK.MyConnection;
                    _with31.CommandType = CommandType.StoredProcedure;
                    _with31.CommandText = objWK.MyUserName + ".JOB_CARD_TRN_PKG.JOB_TRN_CONT_UPD";
                    _with31.Parameters.Clear();
                    var _with32 = _with31.Parameters;
                    updContainerDetails.Parameters.Add("JOB_TRN_CONT_PK_IN", dsContainerData.Tables[0].Rows[nRowCnt]["JOB_TRN_CONT_PK"]).Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["JOB_TRN_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("JOB_CARD_TRN_FK_IN", dsContainerData.Tables[0].Rows[nRowCnt]["JOB_CARD_TRN_PK"]).Direction = ParameterDirection.Input;

                    updContainerDetails.Parameters.Add("CONTAINER_NUMBER_IN", dsContainerData.Tables[0].Rows[nRowCnt]["container_number"]).Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["CONTAINER_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", dsContainerData.Tables[0].Rows[nRowCnt]["container_type_mst_fk"]).Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("SEAL_NUMBER_IN", dsContainerData.Tables[0].Rows[nRowCnt]["seal_number"]).Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["SEAL_NUMBER_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("VOLUME_IN_CBM_IN", dsContainerData.Tables[0].Rows[nRowCnt]["volume_in_cbm"]).Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("GROSS_WEIGHT_IN", dsContainerData.Tables[0].Rows[nRowCnt]["gross_weight"]).Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("NET_WEIGHT_IN", dsContainerData.Tables[0].Rows[nRowCnt]["net_weight"]).Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("CHARGEABLE_WEIGHT_IN", dsContainerData.Tables[0].Rows[nRowCnt]["chargeable_weight"]).Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("PACK_TYPE_MST_FK_IN", dsContainerData.Tables[0].Rows[nRowCnt]["pack_type_mst_fk"]).Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["PACK_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("PACK_COUNT_IN", dsContainerData.Tables[0].Rows[nRowCnt]["pack_count"]).Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("COMMODITY_MST_FK_IN", dsContainerData.Tables[0].Rows[nRowCnt]["commodity_mst_fk"]).Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("COMMODITY_MST_FKS_IN", dsContainerData.Tables[0].Rows[nRowCnt]["COMMODITY_MST_FKS"]).Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["COMMODITY_MST_FKS_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("LOAD_DATE_IN", dsContainerData.Tables[0].Rows[nRowCnt]["gen_land_date"]).Direction = ParameterDirection.Input;
                    updContainerDetails.Parameters["LOAD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    updContainerDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updContainerDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    RecAfct = _with31.ExecuteNonQuery();
                }
                if (RecAfct > 0)
                {
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
                TRAN.Rollback();
                throw oraexp;
                arrMessage.Add(oraexp.Message);
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                arrMessage.Add(ex.Message);
                throw ex;
            }
            finally
            {
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
            string JCPKs = "";
            Array MJCPKArray = null;
            objWK.OpenConnection();
            string SRet_Value = null;
            arrMessage.Clear();
            MJCPKArray = MJCIMPPK.Split(',');
            TRAN = objWK.MyConnection.BeginTransaction();
            try
            {
                for (i = 0; i <= MJCPKArray.Length - 1; i++)
                {
                    var _with33 = Cmd;
                    _with33.Parameters.Clear();
                    _with33.Transaction = TRAN;
                    _with33.Connection = objWK.MyConnection;
                    _with33.CommandType = CommandType.StoredProcedure;
                    _with33.CommandText = objWK.MyUserName + ".MASTER_JC_SEA_EXP_TBL_PKG.MJC_IMP_DECONSOLE";
                    var _with34 = _with33.Parameters;
                    _with34.Add("MJC_SEA_IMP_PK_IN", MJCPKArray.GetValue(i)).Direction = ParameterDirection.Input;
                    _with34.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, SRet_Value).Direction = ParameterDirection.Output;
                    RAF = Cmd.ExecuteNonQuery();
                    if (string.IsNullOrEmpty(JCPKs))
                    {
                        JCPKs = Convert.ToString(MJCPKArray.GetValue(i));
                    }
                    else
                    {
                        JCPKs = JCPKs + "," + MJCPKArray.GetValue(i);
                    }
                }
            }
            catch (OracleException oraexp)
            {
                if (oraexp.ErrorCode == 20999)
                {
                    arrMessage.Add("20999");
                    TRAN.Rollback();
                }
                else
                {
                    arrMessage.Add(oraexp.Message);
                    TRAN.Rollback();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            if (arrMessage.Count > 0)
            {
                return arrMessage;
                TRAN.Rollback();
            }
            else
            {
                arrMessage.Add("Saved");
                TRAN.Commit();
                //Push to financial system if realtime is selected
                if (!string.IsNullOrEmpty(JCPKs))
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
                        //    objPush.UpdateCostCentre(schDtls[10], schDtls[2], schDtls[6], schDtls[4],errGen, schDtls[5].ToString().ToUpper(), schDtls[0].ToString().ToUpper(), , , JCPKs);
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
        }

        #endregion

        #region "Getcontainerdata"
        public DataSet GetContainer_Data(string jobCardPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT JOB.JOB_CARD_TRN_PK,CONT_TRN.JOB_TRN_CONT_PK,");
            sb.Append("       CONT_TRN.CONTAINER_TYPE_MST_FK,");
            sb.Append("       CONT_TRN.CONTAINER_NUMBER,");
            sb.Append("       CONT_TRN.SEAL_NUMBER,");
            sb.Append("       '' FETCH_COMM,");
            sb.Append("       CONT_TRN.COMMODITY_MST_FK,");
            sb.Append("       CONT_TRN.COMMODITY_MST_FKS,");
            sb.Append("       CONT_TRN.PACK_TYPE_MST_FK,");
            sb.Append("       CONT_TRN.PACK_COUNT,");
            sb.Append("       CONT_TRN.VOLUME_IN_CBM,");
            sb.Append("       CONT_TRN.GROSS_WEIGHT,");
            sb.Append("       CONT_TRN.NET_WEIGHT,");
            sb.Append("       CONT_TRN.CHARGEABLE_WEIGHT,");
            //sb.Append("       TO_CHAR(CONT_TRN.GEN_LAND_DATE,DATETIMEFORMAT24)gen_land_date,")
            sb.Append("        TO_DATE(CONT_TRN.LOAD_DATE, DATEFORMAT)gen_land_date,");
            sb.Append("       CTMT.CONTAINER_TYPE_MST_ID,");
            sb.Append("       'true' sel");
            sb.Append("  FROM JOB_CARD_TRN   JOB,");
            sb.Append("       JOB_TRN_CONT   CONT_TRN,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CTMT,");
            sb.Append("       PACK_TYPE_MST_TBL      PTMT,");
            sb.Append("       COMMODITY_MST_TBL      COMM");
            sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = CONT_TRN.JOB_CARD_TRN_FK(+)");
            sb.Append("   AND CONT_TRN.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND CONT_TRN.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK(+)");
            sb.Append("   AND CONT_TRN.COMMODITY_MST_FK = COMM.COMMODITY_MST_PK(+)");
            //sb.Append("   AND JOB.JOB_CARD_TRN_PK=" & jobCardPK)
            sb.Append("   AND JOB.JOB_CARD_TRN_PK IN (" + jobCardPK + ")");
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
        #endregion

        #region "Fetch Grid Details"
        public DataSet FetchGridDetails(string Cargotype = "", string bizType = "", string POLPK = "", string PODPK = "", string VslVoyPK = "", string ShipperPK = "", string ConsigneePK = "", string HblNr = "", string MblNr = "",Int32 CurrentPage = 0,
       Int32 TotalPage = 0, Int32 flag = 0)
        {
            WorkFlow objWF = new WorkFlow();
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strSQL = null;
            sb.Append("SELECT DISTINCT JOB.JOB_CARD_TRN_PK,");
            sb.Append("                MJC.MASTER_JC_SEA_IMP_PK,");
            sb.Append("                JOB.MBL_MAWB_REF_NO MBL_REF_NO,");
            sb.Append("                JOB.HBL_HAWB_REF_NO HBL_REF_NO,");
            sb.Append("                MJC.MASTER_JC_REF_NO,");
            sb.Append("                JOB.JOBCARD_REF_NO,");
            sb.Append("                DECODE(JOB.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN (NVL(JOB.VESSEL_NAME, '') || '/' ||");
            sb.Append("                       NVL(JOB.VOYAGE_FLIGHT_NO, '') = '/') THEN");
            sb.Append("                   ''");
            sb.Append("                  ELSE");
            sb.Append("                   NVL(JOB.VESSEL_NAME, '') || '/' || NVL(JOB.VOYAGE_FLIGHT_NO, '')");
            sb.Append("                END) AS VESVOYAGE,");
            sb.Append("                JOB.ETA_DATE,");
            sb.Append("                POL.PORT_ID POLID,");
            sb.Append("                POD.PORT_ID PODID,");
            sb.Append("                CMT.CUSTOMER_NAME,");
            sb.Append("                JOB.PAGE_NR ");
            sb.Append("  FROM JOB_CARD_TRN  JOB,");
            sb.Append("       MASTER_JC_SEA_IMP_TBL MJC,");
            sb.Append("       PORT_MST_TBL          POL,");
            sb.Append("       PORT_MST_TBL          POD,");
            sb.Append("       USER_MST_TBL          UMT,");
            sb.Append("       CUSTOMER_MST_TBL      CMT");
            sb.Append(" WHERE JOB.MASTER_JC_FK = MJC.MASTER_JC_SEA_IMP_PK(+)");
            sb.Append("   AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND JOB.SHIPPER_CUST_MST_FK=CMT.CUSTOMER_MST_PK");
            sb.Append("   AND JOB.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            sb.Append("   AND JOB.MBL_MAWB_REF_NO IS NOT NULL");
            sb.Append("   AND JOB.CONSOLE = 2");
            sb.Append("   AND JOB.BUSINESS_TYPE=2 ");
            sb.Append("   AND JOB.PROCESS_TYPE=2 ");

            if (flag == 0)
            {
                sb.Append("AND 1=2");
            }
            if (Cargotype != "0")
            {
                sb.Append("AND JOB.CARGO_TYPE=" + Cargotype);
            }
            if (!string.IsNullOrEmpty(POLPK))
            {
                sb.Append("AND JOB.PORT_MST_POL_FK=" + POLPK);
            }
            if (!string.IsNullOrEmpty(PODPK))
            {
                sb.Append("AND JOB.PORT_MST_POD_FK=" + PODPK);
            }
            if (!string.IsNullOrEmpty(VslVoyPK))
            {
                sb.Append("AND JOB.VOYAGE_TRN_FK=" + VslVoyPK);
            }
            if (!string.IsNullOrEmpty(ShipperPK))
            {
                sb.Append("AND JOB.SHIPPER_CUST_MST_FK=" + ShipperPK);
            }
            if (!string.IsNullOrEmpty(ConsigneePK))
            {
                sb.Append("AND JOB.CONSIGNEE_CUST_MST_FK=" + ConsigneePK);
            }
            if (HblNr.Trim().Length > 0)
            {
                sb.Append(" AND UPPER(JOB.HBL_HAWB_REF_NO) LIKE '%" + HblNr.ToUpper().Replace("'", "''") + "%'");
            }
            if (MblNr.Trim().Length > 0)
            {
                sb.Append(" AND UPPER(JOB.MBL_MAWB_REF_NO) LIKE '%" + MblNr.ToUpper().Replace("'", "''") + "%'");
            }
            sb.Append("   ORDER BY JOB_CARD_TRN_PK DESC");
            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strSQL = sb.ToString();
            strCount.Append(" SELECT COUNT(*)  from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
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
            System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
            sqlstr2.Append(" Select * from ");
            sqlstr2.Append( "  ( Select ROWNUM SR_NO, q.* from ");
            sqlstr2.Append("  (" + sb.ToString() + " ");
            sqlstr2.Append("  ) q )  WHERE \"SR_NO\"  BETWEEN " + start + " AND " + last + "");
            strSQL = sqlstr2.ToString();
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

        #region "Fetch NoOfHbls"
        public int FetchNoOfHbls(string MJobpk)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSQL.Append( " SELECT MJOB.NO_HBLS FROM ");
            strSQL.Append( " MASTER_JC_SEA_IMP_TBL MJOB");
            strSQL.Append( " WHERE MJOB.MASTER_JC_SEA_IMP_PK = " + MJobpk + " ");
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(strSQL.ToString()));
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

        #region "Fetch Frunction"
        public DataSet FetchFunction(string bizType = "", string POLPK = "", string PODPK = "", string VslVoyPK = "", string HblNr = "", string MblNr = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            OracleDataAdapter DA = new OracleDataAdapter();
            DataSet MainDS = new DataSet();
            WorkFlow objWF = new WorkFlow();

            try
            {
                sb.Append(" SELECT MJOB.MASTER_JC_SEA_IMP_PK JOBPK,");
                sb.Append("       MJOB.MASTER_JC_SEA_IMP_PK MJOBPK,");
                sb.Append("       JOB.MBL_MAWB_REF_NO REF_NO,");
                sb.Append("       JOB.MBL_MAWB_DATE MBL_DATE,");
                sb.Append("       POL.PORT_ID POLID,");
                sb.Append("       POD.PORT_ID PODID,");
                sb.Append("       MAX(JOB.ETA_DATE) ETA_DATE,");
                sb.Append("       MAX(JOB.ARRIVAL_DATE) ARRIVAL_DATE,");
                sb.Append("       ''COMMODITY_ID,");
                sb.Append("       ''PACK_TYPE_ID,");
                sb.Append("       NVL(SUM(JOB_TRN.PACK_COUNT), 0) QTY,");
                sb.Append("       NVL(SUM(JOB_TRN.VOLUME_IN_CBM), 0) VOLUME,");
                sb.Append("       NVL(SUM(JOB_TRN.GROSS_WEIGHT), 0) GROSS_WEIGHT,");
                //sb.Append("       NVL(SUM(JOB_TRN.NET_WEIGHT), 0) NET_WEIGHT,")
                sb.Append("        SUM((CASE   WHEN JOB.CARGO_TYPE = 1 OR JOB.CARGO_TYPE = 4 THEN ");
                sb.Append("        NVL(JOB_TRN.NET_WEIGHT, 0)");
                sb.Append("        ELSE   NVL(JOB_TRN.CHARGEABLE_WEIGHT, 0)");
                sb.Append("        END)) NET_WEIGHT,");
                sb.Append("       '' SEL,");
                sb.Append("       '' SAVEMJC");
                sb.Append("  FROM JOB_CARD_TRN  JOB,");
                sb.Append("       JOB_TRN_CONT  JOB_TRN,");
                sb.Append("       MASTER_JC_SEA_IMP_TBL MJOB,");
                sb.Append("       PORT_MST_TBL          POL,");
                sb.Append("       PORT_MST_TBL          POD,");
                sb.Append("       USER_MST_TBL          UMT,");
                sb.Append("       COMMODITY_MST_TBL     CMT,");
                sb.Append("       PACK_TYPE_MST_TBL     PTMT");
                sb.Append(" WHERE JOB.JOB_CARD_TRN_PK = JOB_TRN.JOB_CARD_TRN_FK");
                sb.Append("   AND JOB.MASTER_JC_FK = MJOB.MASTER_JC_SEA_IMP_PK");
                sb.Append("   AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND JOB_TRN.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
                sb.Append("   AND JOB_TRN.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK(+)");
                sb.Append("   AND JOB.CREATED_BY_FK = UMT.USER_MST_PK");
                //sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " & Session("LOGED_IN_LOC_FK"))
                sb.Append("   AND POD.LOCATION_MST_FK IN (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L WHERE L.LOCATION_MST_PK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ")");
                sb.Append("   AND JOB.CONSOLE = 2");
                sb.Append("   AND MJOB.NO_HBLS =");
                sb.Append("     (SELECT COUNT(*) FROM JOB_CARD_TRN J  WHERE J.MASTER_JC_FK = MJOB.MASTER_JC_SEA_IMP_PK AND J.BUSINESS_TYPE=2 AND J.PROCESS_TYPE=2)");
                //sb.Append("          JOB.ARRIVAL_DATE,JOB.CARGO_TYPE,")
                //sb.Append("          CMT.COMMODITY_ID")
                if (!string.IsNullOrEmpty(POLPK))
                {
                    sb.Append("  AND JOB.PORT_MST_POL_FK=" + POLPK);
                }
                if (!string.IsNullOrEmpty(PODPK))
                {
                    sb.Append("   AND JOB.PORT_MST_POD_FK=" + PODPK);
                }
                if (!string.IsNullOrEmpty(VslVoyPK))
                {
                    sb.Append("   AND JOB.VOYAGE_TRN_FK=" + VslVoyPK);
                }
                if (HblNr.Trim().Length > 0)
                {
                    sb.Append( " AND UPPER(JOB.HBL_HAWB_REF_NO) LIKE '%" + HblNr.ToUpper().Replace("'", "''") + "%'");
                }
                if (MblNr.Trim().Length > 0)
                {
                    sb.Append( " AND UPPER(JOB.MBL_MAWB_REF_NO) LIKE '%" + MblNr.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" AND JOB.BUSINESS_TYPE = " + bizType);
                sb.Append(" AND JOB.PROCESS_TYPE = 2");
                sb.Append(" GROUP BY MJOB.MASTER_JC_SEA_IMP_PK,");
                sb.Append("          JOB.MBL_MAWB_REF_NO,");
                sb.Append("          JOB.MBL_MAWB_DATE,");
                sb.Append("          POL.PORT_ID,");
                sb.Append("          POD.PORT_ID");
                //sb.Append("          JOB.ETA_DATE,")
                //sb.Append("          JOB.ARRIVAL_DATE")

                //'Modified by Mayur for Getting Master JobCards List apart from Quick Entry for Deconsolidation 
                //sb.Append("          ORDER BY JOB.MBL_DATE DESC")
                sb.Append(" UNION ");
                sb.Append(" SELECT M.MASTER_JC_SEA_IMP_PK JOBPK, ");
                sb.Append("       M.MASTER_JC_SEA_IMP_PK MJOBPK, ");
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
                sb.Append("       SUM((CASE");
                sb.Append("             WHEN JOB.CARGO_TYPE = 1 OR JOB.CARGO_TYPE = 4 THEN");
                sb.Append("              NVL(JOB_TRN.NET_WEIGHT, 0)");
                sb.Append("             ELSE");
                sb.Append("              NVL(JOB_TRN.CHARGEABLE_WEIGHT, 0)");
                sb.Append("           END)) NET_WEIGHT,");
                sb.Append("       '' SEL,");
                sb.Append("       '' SAVEMJC");
                sb.Append("  FROM JOB_CARD_TRN  JOB,");
                sb.Append("       JOB_TRN_CONT  JOB_TRN,");
                sb.Append("       MASTER_JC_SEA_IMP_TBL M,");
                sb.Append("       PORT_MST_TBL          POL,");
                sb.Append("       PORT_MST_TBL          POD,");
                sb.Append("       AGENT_MST_TBL         AMT,");
                sb.Append("       VESSEL_VOYAGE_TRN     VVT,");
                sb.Append("       VESSEL_VOYAGE_TBL     VST,");
                sb.Append("       OPERATOR_MST_TBL      OMT");
                sb.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = M.PORT_MST_POD_FK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = JOB_TRN.JOB_CARD_TRN_FK");
                sb.Append("   AND JOB.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK");
                sb.Append("   AND OMT.OPERATOR_MST_PK(+) = M.OPERATOR_MST_FK");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND M.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND POD.LOCATION_MST_FK IN (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L WHERE L.LOCATION_MST_PK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ")");
                sb.Append("   AND M.MASTER_JC_STATUS = 1");
                if (!string.IsNullOrEmpty(POLPK))
                {
                    sb.Append("  AND JOB.PORT_MST_POL_FK=" + POLPK);
                }
                if (!string.IsNullOrEmpty(PODPK))
                {
                    sb.Append("   AND JOB.PORT_MST_POD_FK=" + PODPK);
                }
                if (!string.IsNullOrEmpty(VslVoyPK))
                {
                    sb.Append("   AND JOB.VOYAGE_TRN_FK=" + VslVoyPK);
                }
                if (HblNr.Trim().Length > 0)
                {
                    sb.Append( " AND UPPER(JOB.HBL_HAWB_REF_NO) LIKE '%" + HblNr.ToUpper().Replace("'", "''") + "%'");
                }
                if (MblNr.Trim().Length > 0)
                {
                    sb.Append( " AND UPPER(JOB.MBL_MAWB_REF_NO) LIKE '%" + MblNr.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" AND JOB.BUSINESS_TYPE = " + bizType);
                sb.Append(" AND JOB.PROCESS_TYPE = 2");
                sb.Append(" GROUP BY M.MASTER_JC_SEA_IMP_PK,");
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
                sb.Append(" SELECT JOB.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       0 JOB_TRN_PK,");
                sb.Append("       MJOB.MASTER_JC_SEA_IMP_PK MJOBPK,");
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
                //sb.Append("       JOB_TRN.NET_WEIGHT NET_WEIGHT,")
                sb.Append("        SUM((CASE  WHEN JOB.CARGO_TYPE = 1 OR JOB.CARGO_TYPE = 4 THEN ");
                sb.Append("        NVL(JOB_TRN.NET_WEIGHT, 0)");
                sb.Append("        ELSE  NVL(JOB_TRN.CHARGEABLE_WEIGHT, 0)");
                sb.Append("        END)) NET_WEIGHT,");
                sb.Append("       '' SEL");
                sb.Append("  FROM JOB_CARD_TRN JOB,");
                sb.Append("       MASTER_JC_SEA_IMP_TBL MJOB,");
                sb.Append("       JOB_TRN_CONT JOB_TRN,");
                sb.Append("       PORT_MST_TBL         POL,");
                sb.Append("       PORT_MST_TBL         POD,");
                sb.Append("       USER_MST_TBL          UMT,");
                sb.Append("       COMMODITY_MST_TBL    CMT,");
                sb.Append("       PACK_TYPE_MST_TBL    PTMT,");
                sb.Append("       CUSTOMER_MST_TBL    CUST,");
                sb.Append("       CUSTOMER_MST_TBL    SHIP");
                sb.Append(" WHERE  JOB.JOB_CARD_TRN_PK = JOB_TRN.JOB_CARD_TRN_FK");
                sb.Append("   AND JOB.MASTER_JC_FK = MJOB.MASTER_JC_SEA_IMP_PK");
                sb.Append("   AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
                sb.Append("   AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
                sb.Append("   AND JOB_TRN.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
                sb.Append("   AND JOB_TRN.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK(+)");
                sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK=CUST.CUSTOMER_MST_PK(+)");
                sb.Append("   AND JOB.SHIPPER_CUST_MST_FK=SHIP.CUSTOMER_MST_PK");
                sb.Append("   AND JOB.CREATED_BY_FK = UMT.USER_MST_PK");
                //sb.Append("   AND UMT.DEFAULT_LOCATION_FK = " & Session("LOGED_IN_LOC_FK"))
                sb.Append(" AND POD.LOCATION_MST_FK IN (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L WHERE L.LOCATION_MST_PK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ")");
                sb.Append("   AND JOB.CONSOLE = 2");
                sb.Append("   AND MJOB.NO_HBLS =");
                sb.Append("     (SELECT COUNT(*) FROM JOB_CARD_TRN J  WHERE J.MASTER_JC_FK = MJOB.MASTER_JC_SEA_IMP_PK AND J.BUSINESS_TYPE=2 AND J.PROCESS_TYPE=2)");
                if (!string.IsNullOrEmpty(POLPK))
                {
                    sb.Append("  AND JOB.PORT_MST_POL_FK=" + POLPK);
                }
                if (!string.IsNullOrEmpty(PODPK))
                {
                    sb.Append("   AND JOB.PORT_MST_POD_FK=" + PODPK);
                }
                if (!string.IsNullOrEmpty(VslVoyPK))
                {
                    sb.Append("   AND JOB.VOYAGE_TRN_FK=" + VslVoyPK);
                }
                if (HblNr.Trim().Length > 0)
                {
                    sb.Append( " AND UPPER(JOB.HBL_HAWB_REF_NO) LIKE '%" + HblNr.ToUpper().Replace("'", "''") + "%'");
                }
                if (MblNr.Trim().Length > 0)
                {
                    sb.Append( " AND UPPER(JOB.MBL_MAWB_REF_NO) LIKE '%" + MblNr.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" AND JOB.BUSINESS_TYPE = " + bizType);
                sb.Append(" AND JOB.PROCESS_TYPE = 2");
                sb.Append( " GROUP BY JOB.JOB_CARD_TRN_PK, ");
                sb.Append( " MJOB.MASTER_JC_SEA_IMP_PK, ");
                sb.Append( " JOB.HBL_HAWB_REF_NO, ");
                sb.Append( " JOB.HBL_HAWB_DATE, ");
                sb.Append( " POL.PORT_ID, ");
                sb.Append( " POD.PORT_ID, ");
                sb.Append( " JOB.ETA_DATE, ");
                sb.Append( " JOB.ARRIVAL_DATE, ");
                sb.Append( " CUST.CUSTOMER_NAME, ");
                sb.Append( " SHIP.CUSTOMER_NAME ");

                sb.Append(" UNION");
                sb.Append(" SELECT JOB.JOB_CARD_TRN_PK JOBPK,");
                sb.Append("       0 JOB_TRN_PK,");
                sb.Append("       M.MASTER_JC_SEA_IMP_PK MJOBPK,");
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
                sb.Append("       SUM((CASE");
                sb.Append("         WHEN JOB.CARGO_TYPE = 1 OR JOB.CARGO_TYPE = 4 THEN");
                sb.Append("          NVL(JOB_TRN.NET_WEIGHT, 0)");
                sb.Append("         ELSE");
                sb.Append("          NVL(JOB_TRN.CHARGEABLE_WEIGHT, 0)");
                sb.Append("       END)) NET_WEIGHT,");
                sb.Append("       '' SEL");
                sb.Append("  FROM JOB_CARD_TRN  JOB,");
                sb.Append("       JOB_TRN_CONT  JOB_TRN,");
                sb.Append("       MASTER_JC_SEA_IMP_TBL M,");
                sb.Append("       PORT_MST_TBL          POL,");
                sb.Append("       PORT_MST_TBL          POD,");
                sb.Append("       AGENT_MST_TBL         AMT,");
                sb.Append("       VESSEL_VOYAGE_TRN     VVT,");
                sb.Append("       VESSEL_VOYAGE_TBL     VST,");
                sb.Append("       OPERATOR_MST_TBL      OMT,");
                sb.Append("       COMMODITY_MST_TBL     CMT,");
                sb.Append("       PACK_TYPE_MST_TBL     PTMT,");
                sb.Append("       CUSTOMER_MST_TBL      CUST,");
                sb.Append("       CUSTOMER_MST_TBL      SHIP");
                sb.Append(" WHERE POL.PORT_MST_PK = M.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = M.PORT_MST_POD_FK");
                sb.Append("   AND JOB.JOB_CARD_TRN_PK = JOB_TRN.JOB_CARD_TRN_FK");
                sb.Append("   AND JOB.MASTER_JC_FK = M.MASTER_JC_SEA_IMP_PK");
                sb.Append("   AND AMT.AGENT_MST_PK(+) = M.LOAD_AGENT_MST_FK");
                sb.Append("   AND OMT.OPERATOR_MST_PK(+) = M.OPERATOR_MST_FK");
                sb.Append("   AND VVT.VESSEL_VOYAGE_TBL_FK = VST.VESSEL_VOYAGE_TBL_PK(+)");
                sb.Append("   AND M.VOYAGE_TRN_FK = VVT.VOYAGE_TRN_PK(+)");
                sb.Append("   AND JOB_TRN.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
                sb.Append("   AND JOB_TRN.PACK_TYPE_MST_FK = PTMT.PACK_TYPE_MST_PK(+)");
                sb.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                sb.Append("   AND JOB.SHIPPER_CUST_MST_FK = SHIP.CUSTOMER_MST_PK");
                sb.Append("   AND M.MASTER_JC_STATUS = 1");
                sb.Append("   AND POD.LOCATION_MST_FK IN (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L WHERE L.LOCATION_MST_PK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ")");
                if (!string.IsNullOrEmpty(POLPK))
                {
                    sb.Append("  AND JOB.PORT_MST_POL_FK=" + POLPK);
                }
                if (!string.IsNullOrEmpty(PODPK))
                {
                    sb.Append("   AND JOB.PORT_MST_POD_FK=" + PODPK);
                }
                if (!string.IsNullOrEmpty(VslVoyPK))
                {
                    sb.Append("   AND JOB.VOYAGE_TRN_FK=" + VslVoyPK);
                }
                if (HblNr.Trim().Length > 0)
                {
                    sb.Append( " AND UPPER(JOB.HBL_HAWB_REF_NO) LIKE '%" + HblNr.ToUpper().Replace("'", "''") + "%'");
                }
                if (MblNr.Trim().Length > 0)
                {
                    sb.Append( " AND UPPER(JOB.MBL_MAWB_REF_NO) LIKE '%" + MblNr.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" AND JOB.BUSINESS_TYPE = " + bizType);
                sb.Append(" AND JOB.PROCESS_TYPE = 2");
                sb.Append( " GROUP BY JOB.JOB_CARD_TRN_PK , ");
                sb.Append( " M.MASTER_JC_SEA_IMP_PK , ");
                sb.Append( " JOB.HBL_HAWB_REF_NO , ");
                sb.Append( " JOB.HBL_HAWB_DATE , ");
                sb.Append( " POL.PORT_ID , ");
                sb.Append( " POD.PORT_ID , ");
                sb.Append( " JOB.ETA_DATE, ");
                sb.Append( " JOB.ARRIVAL_DATE, ");
                sb.Append( " CUST.CUSTOMER_NAME , ");
                sb.Append( " SHIP.CUSTOMER_NAME ");

                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "CHILDDETAILS");

                DataRelation rel_Details = new DataRelation("MJCDETAILS", new DataColumn[] { MainDS.Tables[0].Columns["MJOBPK"] }, new DataColumn[] { MainDS.Tables[1].Columns["MJOBPK"] });
                rel_Details.Nested = true;
                MainDS.Relations.Add(rel_Details);
                return MainDS;
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

        #region "Fetch MJCRefNr"
        public string FetchMJCRefNr(string MJobpk)
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strSQL.Append( " SELECT MJOB.MASTER_JC_REF_NO FROM ");
            strSQL.Append( " MASTER_JC_SEA_IMP_TBL MJOB");
            strSQL.Append( " WHERE MJOB.MASTER_JC_SEA_IMP_PK = " + MJobpk + " ");
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
        #endregion

        #region " Enhance Search Functionality FetchForMblRefInMbllist "
        public string FetchForMblRefInImplist(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strLOCATION_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            strJobCardPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLOCATION_IN = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_MBL_REF_FOR_MBL_LIST.GET_MBL_REF_JOBCARDIMP";
                var _with35 = SCM.Parameters;
                _with35.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with35.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with35.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with35.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
                _with35.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
        #endregion

        #region " Enhance Search Functionality FetchForHblRefInMbllist "
        public string FetchForHblRefInImplist(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strLOCATION_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            strJobCardPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLOCATION_IN = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_MBL_REF_FOR_MBL_LIST.GET_HBL_REF_JOBCARDIMP";
                var _with36 = SCM.Parameters;
                _with36.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with36.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with36.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with36.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
                _with36.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
        #endregion
        #region " Enhance Search Functionality FetchForSHPRefInMbllist "
        public string FetchForSHPRefInImplist(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strLOCATION_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            strJobCardPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLOCATION_IN = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_MBL_REF_FOR_MBL_LIST.GET_SHP_REF_JOBCARDIMP";
                var _with37 = SCM.Parameters;
                _with37.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "": strSERACH_IN)).Direction = ParameterDirection.Input;
                _with37.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with37.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with37.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
                _with37.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
        #endregion
        #region " Enhance Search Functionality FetchForCNSRefInMbllist "
        public string FetchForCNSRefInImplist(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strLOCATION_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            strJobCardPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLOCATION_IN = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_MBL_REF_FOR_MBL_LIST.GET_CNS_REF_JOBCARDIMP";
                var _with38 = SCM.Parameters;
                _with38.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "": strSERACH_IN)).Direction = ParameterDirection.Input;
                _with38.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with38.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with38.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOCATION_IN) ? strLOCATION_IN : "")).Direction = ParameterDirection.Input;
                _with38.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
        #endregion
    }
}