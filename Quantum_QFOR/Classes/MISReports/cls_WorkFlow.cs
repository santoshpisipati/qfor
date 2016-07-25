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

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsWorkFlow : CommonFeatures
    {
        /// <summary>
        /// The m_ data set
        /// </summary>
        private static DataSet M_DataSet = new DataSet();

        /// <summary>
        /// Gets my data set.
        /// </summary>
        /// <value>
        /// My data set.
        /// </value>
        public static DataSet MyDataSet
        {
            get { return M_DataSet; }
        }

        /// <summary>
        /// Mains the query.
        /// </summary>
        /// <param name="bussinessType">Type of the bussiness.</param>
        /// <param name="LocFk">The loc fk.</param>
        /// <param name="addConditions">The add conditions.</param>
        /// <param name="addConditions1">The add conditions1.</param>
        /// <param name="startPage">The start page.</param>
        /// <param name="endPage">The end page.</param>
        /// <param name="ShipStatus">The ship status.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public string MainQuery(string bussinessType, string LocFk, string addConditions, string addConditions1, Int32 startPage, Int32 endPage, string ShipStatus = "", Int16 BizType = 0)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                if (bussinessType == "SE")
                {
                    strQuery.Append("   SELECT MAIN.*");
                    if (BizType == 3)
                    {
                        strQuery.Append("   FROM (SELECT Q.* ");
                    }
                    else
                    {
                        strQuery.Append("   FROM (SELECT ROWNUM AS \"Sl. Nr.\"   , Q.* ");
                    }
                    strQuery.Append("   FROM (SELECT * FROM  (SELECT DISTINCT JHDR.JOBCARD_REF_NO \"Job Card Int32\",");
                    strQuery.Append("   CMT.CUSTOMER_ID \"Cust.ID\",");
                    strQuery.Append("   CMT.CUSTOMER_NAME \"Customer Name\",");
                    if (BizType == 3)
                    {
                        strQuery.Append("   POL.PORT_ID \"From\",");
                        strQuery.Append("   POD.PORT_ID \"To\",");
                    }
                    else
                    {
                        strQuery.Append("   POL.PORT_ID \"From\",");
                        strQuery.Append("   POD.PORT_ID \"To\",");
                    }
                    strQuery.Append("   TO_CHAR(EHDR.ENQUIRY_DATE,dateFormatYY) \"Enquiry\",");
                    strQuery.Append("   TO_CHAR(QHDR.QUOTATION_DATE,dateFormatYY) \"Quotation\",");
                    strQuery.Append("   TO_CHAR(BHDR.BOOKING_DATE,dateFormatYY) \"Booking\",");
                    strQuery.Append("   TO_CHAR(JHDR.JOBCARD_DATE,dateFormatYY) \"Job Card\",");
                    strQuery.Append("   TO_CHAR(JHDR.DEPARTURE_DATE,dateFormatYY) ATD,");
                    if (BizType == 3)
                    {
                        strQuery.Append("   TO_CHAR(HBL.HBL_DATE,dateFormatYY) \"HBL/HAWB\",");
                        strQuery.Append("   TO_CHAR(MBL.MBL_DATE,dateFormatYY) \"MBL/MAWB\",");
                    }
                    else
                    {
                        strQuery.Append("   TO_CHAR(HBL.HBL_DATE,dateFormatYY) \"HBL\",");
                        strQuery.Append("   TO_CHAR(MBL.MBL_DATE,dateFormatYY) \"MBL\",");
                    }
                    strQuery.Append(" (select TO_CHAR(t.created_on,dateFormatYY) from TRACK_N_TRACE_TBL t where t.job_card_fk=JHDR.JOB_CARD_TRN_PK and t.status='Arrival Date' and t.process=1 and rownum=1) \"ATA\",");
                    //strQuery.Append(" (select TO_CHAR(t.created_on,dateFormatYY) from TRACK_N_TRACE_TBL t where t.job_card_fk=JHDR.JOB_CARD_TRN_PK and t.status='Cargo Arrival Notice Generated'and t.process=1) ""CAN""," & vbCrLf)
                    //strQuery.Append(" (select TO_CHAR(t.created_on,dateFormatYY) from TRACK_N_TRACE_TBL t where t.job_card_fk=JHDR.JOB_CARD_TRN_PK and t.status='Delivery Order Generated' and t.process=1) ""DO"" ," & vbCrLf)
                    strQuery.Append(" (SELECT TO_CHAR(T.CREATED_ON,DATEFORMATYY) from TRACK_N_TRACE_TBL T,CAN_MST_TBL CAN,JOB_CARD_TRN JC WHERE JHDR.JOBCARD_REF_NO=JC.JOBCARD_REF_NO AND T.DOC_REF_NO=CAN.CAN_REF_NO AND CAN.JOB_CARD_FK=JC.JOB_CARD_TRN_PK AND T.STATUS='Cargo Arrival Notice Generated' AND ROWNUM=1) \"CAN\",");
                    strQuery.Append(" (SELECT TO_CHAR(T.CREATED_ON,DATEFORMATYY) FROM TRACK_N_TRACE_TBL T,DELIVERY_ORDER_MST_TBL DEL,JOB_CARD_TRN JC WHERE JHDR.JOBCARD_REF_NO=JC.JOBCARD_REF_NO AND T.DOC_REF_NO=DEL.DELIVERY_ORDER_REF_NO AND DEL.JOB_CARD_MST_FK=JC.JOB_CARD_TRN_PK AND T.STATUS='Delivery Order Generated' AND ROWNUM=1) \"DO\", ");
                    strQuery.Append(" (select TO_CHAR(max(t.created_on),dateFormatYY) from TRACK_N_TRACE_TBL t where t.job_card_fk=JHDR.JOB_CARD_TRN_PK and t.status like'Gate-Out%' and t.process=1) \"Gate-Out\" ,");
                    strQuery.Append("   (SELECT TO_CHAR(MAX(INV.INVOICE_DATE),dateFormatYY) ");
                    strQuery.Append("   FROM consol_invoice_tbl INV,");
                    strQuery.Append("   consol_invoice_trn_tbl INVTRN");
                    strQuery.Append("   WHERE INV.consol_invoice_pk =");
                    strQuery.Append("   INVTRN.consol_invoice_fk");
                    strQuery.Append("   and INVTRN.JOB_CARD_FK(+) =");
                    strQuery.Append("   JHDR.JOB_CARD_TRN_PK) \"Invoice\",");
                    strQuery.Append("   COMM.COMMODITY_GROUP_CODE \"Comm. Grp.\",");
                    //'
                    strQuery.Append("  (SELECT STATUS");
                    strQuery.Append("        FROM (SELECT T.STATUS, T.JOB_CARD_FK");
                    strQuery.Append("         FROM TRACK_N_TRACE_TBL T");
                    strQuery.Append("         WHERE T.BIZ_TYPE = 2");
                    strQuery.Append("         AND T.PROCESS = 1");
                    strQuery.Append("         ORDER BY T.CREATED_ON DESC)");
                    strQuery.Append("          WHERE ROWNUM = 1");
                    strQuery.Append("          AND JOB_CARD_FK =");
                    strQuery.Append("          JHDR.JOB_CARD_TRN_PK) \"Shipment. St.\",");
                    strQuery.Append("         'Sea' as \"Biz.Type\",");
                    //'
                    //added by manoj k sethi
                    strQuery.Append("   qhdr.quotation_ref_no,");
                    strQuery.Append("   QUOTATION_MST_PK,");
                    strQuery.Append("   qhdr.status,");
                    strQuery.Append("   BHDR.BOOKING_MST_PK,");
                    strQuery.Append("   BHDR.BOOKING_REF_NO,");
                    strQuery.Append("   hbl_exp_tbl_pk,");
                    strQuery.Append("   hbl_status,");
                    strQuery.Append("   hbl.hbl_ref_no,");
                    strQuery.Append("   mbl_exp_tbl_pk,");
                    strQuery.Append("   EHDR.Enquiry_BKG_SEA_PK,");
                    //strQuery.Append("(select    INV.CUSTOMER_MST_FK" & vbCrLf)
                    //strQuery.Append("           from consol_invoice_tbl inv" & vbCrLf)
                    //strQuery.Append("           where INV.CONSOL_INVOICE_PK =" & vbCrLf)
                    //strQuery.Append("          (SELECT (MAX(INV.CONSOL_INVOICE_PK))" & vbCrLf)
                    //strQuery.Append("           FROM consol_invoice_tbl     INV," & vbCrLf)
                    //strQuery.Append("           JOB_CARD_SEA_EXP_TBL   JHDR," & vbCrLf)
                    //strQuery.Append("           consol_invoice_trn_tbl INVTRN" & vbCrLf)
                    //strQuery.Append("           WHERE INV.consol_invoice_pk =" & vbCrLf)
                    //strQuery.Append("           INVTRN.consol_invoice_fk" & vbCrLf)
                    //strQuery.Append("           and INVTRN.JOB_CARD_FK(+) =" & vbCrLf)
                    //strQuery.Append("           JHDR.JOB_CARD_SEA_EXP_PK))""CUSTOMERfk""," & vbCrLf)
                    strQuery.Append("           (select max(INV.CONSOL_INVOICE_PK)");
                    strQuery.Append("            from consol_invoice_tbl inv,");
                    //strQuery.Append("            where INV.CONSOL_INVOICE_PK =" & vbCrLf)
                    //strQuery.Append("           (SELECT (MAX(INV.CONSOL_INVOICE_PK))" & vbCrLf)
                    //strQuery.Append("           FROM consol_invoice_tbl     INV," & vbCrLf)
                    //strQuery.Append("           JOB_CARD_SEA_EXP_TBL   JHDR," & vbCrLf)
                    strQuery.Append("          consol_invoice_trn_tbl INVTRN");
                    strQuery.Append("           WHERE INV.consol_invoice_pk =");
                    strQuery.Append("           INVTRN.consol_invoice_fk");
                    strQuery.Append("            and INVTRN.JOB_CARD_FK(+) =");
                    strQuery.Append("           JHDR.JOB_CARD_TRN_PK)\"CONSOLPK\",");
                    strQuery.Append("   BHDR.CARGO_TYPE ,");
                    strQuery.Append(" null \"CAN_PK\",");
                    strQuery.Append(" null \"DO_PK\",");
                    strQuery.Append(" JHDR.JOB_CARD_TRN_PK, QHDR.QUOTATION_TYPE ");
                    //strQuery.Append("   consol_invoice_pk" & vbCrLf)
                    //Ended by Manoj K sethi
                    strQuery.Append("   FROM MBL_EXP_TBL               MBL,");
                    //strQuery.Append("    consol_invoice_tbl INV," & vbCrLf)
                    strQuery.Append("   HBL_EXP_TBL               HBL,");
                    strQuery.Append("   JOB_CARD_TRN         JHDR,");
                    //add by latha for fetching location wise
                    strQuery.Append("   USER_MST_TBL     UMT,");
                    strQuery.Append("   BOOKING_MST_TBL           BHDR,");
                    strQuery.Append("   BOOKING_TRN   BTRN,");
                    strQuery.Append("   QUOTATION_MST_TBL         QHDR,");
                    strQuery.Append("   QUOTATION_DTL_TBL    QTRN,");
                    strQuery.Append("   ENQUIRY_BKG_SEA_TBL       EHDR,");
                    strQuery.Append("   CUSTOMER_MST_TBL          CMT,");
                    strQuery.Append("   PORT_MST_TBL              POL,");
                    strQuery.Append("   PORT_MST_TBL              POD,");
                    strQuery.Append("   COMMODITY_GROUP_MST_TBL    COMM,");
                    strQuery.Append("   VESSEL_VOYAGE_TBL         VVT,");
                    strQuery.Append("   VESSEL_VOYAGE_TRN         VTT,");
                    //'
                    strQuery.Append("   CONSOL_INVOICE_TBL     CIT,");
                    strQuery.Append("   CONSOL_INVOICE_TRN_TBL CITT,");
                    strQuery.Append("   COLLECTIONS_TBL        CT,");
                    strQuery.Append("   COLLECTIONS_TRN_TBL    CTT,");
                    strQuery.Append("   JOB_TRN_COST   JTAEC,");
                    strQuery.Append("   INV_SUPPLIER_TBL       IST,");
                    strQuery.Append("   INV_SUPPLIER_TRN_TBL   ISTT,");
                    strQuery.Append("   PAYMENTS_TBL           PT, ");
                    strQuery.Append("   PAYMENT_TRN_TBL        PTT,");
                    strQuery.Append("   INV_AGENT_TBL  IASET,");
                    strQuery.Append("   BOOKING_CRO_TBL           BCR ");
                    //'
                    strQuery.Append("   WHERE BHDR.BOOKING_MST_PK = JHDR.BOOKING_MST_FK");
                    strQuery.Append("   AND BHDR.BOOKING_MST_PK = BCR.BOOKING_MST_FK(+)");
                    strQuery.Append("   AND JHDR.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                    strQuery.Append("   AND JHDR.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK(+)");
                    strQuery.Append("   AND BHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    strQuery.Append("   AND BHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    //strQuery.Append("   AND BHDR.AIRLINE_MST_FK = AIR.AIRLINE_MST_PK(+)" & vbCrLf)
                    strQuery.Append("   AND BTRN.BOOKING_MST_FK(+) = BHDR.BOOKING_MST_PK");
                    strQuery.Append("   AND BHDR.STATUS<>3 ");
                    strQuery.Append("   AND BHDR.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    strQuery.Append("   AND BTRN.TRANS_REF_NO = QHDR.QUOTATION_REF_NO(+)");
                    strQuery.Append("   AND QTRN.QUOTATION_MST_FK(+) = QHDR.QUOTATION_MST_PK");
                    strQuery.Append("   AND QTRN.TRANS_REF_NO = EHDR.ENQUIRY_REF_NO(+)");
                    strQuery.Append("   AND COMM.COMMODITY_GROUP_PK(+)=JHDR.COMMODITY_GROUP_FK ");
                    strQuery.Append("   AND JHDR.VOYAGE_TRN_FK = VTT.VOYAGE_TRN_PK");
                    strQuery.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VTT.VESSEL_VOYAGE_TBL_FK");
                    //'
                    strQuery.Append("   AND NVL(IST.APPROVED,0) <> 3 ");
                    strQuery.Append("   AND CIT.CHK_INVOICE(+) <> 2 ");
                    //strQuery.Append("   AND PT.CHK_INVOICE <> 3 " & vbCrLf)
                    strQuery.Append("   AND CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK(+)");
                    strQuery.Append("   AND CITT.JOB_CARD_FK(+) = JHDR.JOB_CARD_TRN_PK");
                    strQuery.Append("   AND CTT.COLLECTIONS_TBL_FK = CT.COLLECTIONS_TBL_PK(+)");
                    strQuery.Append("   AND CTT.INVOICE_REF_NR(+) = CIT.INVOICE_REF_NO");
                    strQuery.Append("   AND JHDR.JOB_CARD_TRN_PK = JTAEC.JOB_CARD_TRN_FK(+)");
                    strQuery.Append("   AND ISTT.INV_SUPPLIER_TBL_FK = IST.INV_SUPPLIER_PK(+)");
                    strQuery.Append("   AND ISTT.JOB_TRN_EST_FK(+) = JTAEC.JOB_TRN_COST_PK");
                    strQuery.Append("   AND PTT.PAYMENTS_TBL_FK = PT.PAYMENT_TBL_PK(+)");
                    strQuery.Append("   AND PTT.INV_SUPPLIER_TBL_FK(+) = IST.INV_SUPPLIER_PK");
                    strQuery.Append("   AND IASET.JOB_CARD_FK(+) = JHDR.JOB_CARD_TRN_PK");
                    strQuery.Append(" AND JHDR.BUSINESS_TYPE = 2 ");
                    strQuery.Append(" AND JHDR.PROCESS_TYPE = 1 ");
                    //'
                    strQuery.Append(addConditions);
                    strQuery.Append(addConditions1);

                    if (BizType == 3)
                    {
                        strQuery.Append(" ) WHERE 1=1");
                    }
                    else
                    {
                        strQuery.Append(" ORDER BY TO_DATE(\"ATD\", '" + dateFormat + "') DESC,TO_DATE(\"Job Card\", '" + dateFormat + "') DESC) WHERE 1=1  ");
                    }
                    if (ShipStatus == "CAN")
                    {
                        strQuery.Append(" AND \"CAN\" IS NULL");
                    }
                    if (ShipStatus == "DO")
                    {
                        strQuery.Append(" AND \"DO\" IS NULL");
                    }
                    if (ShipStatus == "Gate Out")
                    {
                        strQuery.Append(" AND \"Gate-Out\" IS NULL");
                    }
                    strQuery.Append("   ) Q) MAIN");
                }
                else if (bussinessType == "SI")
                {
                    strQuery.Append("   Select *");
                    if (BizType == 3)
                    {
                        strQuery.Append("   FROM (SELECT Q.* ");
                    }
                    else
                    {
                        strQuery.Append("   FROM (SELECT ROWNUM AS \"Sl. Nr.\"   , Q.* ");
                    }
                    strQuery.Append("from (Select distinct jc.JOBCARD_REF_NO \"Job Card Int32\",");
                    strQuery.Append(" CO.CUSTOMER_ID \"Cust.ID\",");
                    strQuery.Append(" CO.CUSTOMER_NAME \"Customer Name\",");
                    if (BizType == 3)
                    {
                        strQuery.Append(" POL.PORT_ID \"From\",");
                        strQuery.Append(" POD.PORT_ID \"To\",");
                    }
                    else
                    {
                        strQuery.Append(" POL.PORT_ID \"From\",");
                        strQuery.Append(" POD.PORT_ID \"To\",");
                    }
                    strQuery.Append(" null \"Enquiry\",");
                    strQuery.Append(" null \"Quotation\",");
                    strQuery.Append(" null \"Booking\",");
                    strQuery.Append(" TO_CHAR(JC.JOBCARD_DATE,");
                    strQuery.Append("dateFormatYY) \"Job Card\",");
                    strQuery.Append("(select TO_CHAR(max(t.created_on),dateFormatYY) from TRACK_N_TRACE_TBL t where t.job_card_fk=jc.JOB_CARD_TRN_PK and t.process=2 and t.status like'Sail%') ATD,");
                    if (BizType == 3)
                    {
                        strQuery.Append(" null \"HBL/HAWB\",");
                        strQuery.Append(" null \"MBL/MAWB\",");
                    }
                    else
                    {
                        strQuery.Append(" null \"HBL\",");
                        strQuery.Append(" null \"MBL\",");
                    }
                    strQuery.Append(" TO_CHAR(JC.ARRIVAL_DATE,dateFormatYY) \"ATA\" ,");
                    strQuery.Append("  TO_CHAR(CAN.CAN_DATE, dateFormatYY) \"CAN\",");
                    strQuery.Append("  TO_CHAR(DMT.DELIVERY_ORDER_DATE, dateFormatYY) \"DO\" ,");
                    strQuery.Append(" (select TO_CHAR(max(t.created_on),dateFormatYY) from TRACK_N_TRACE_TBL t where t.job_card_fk=jc.JOB_CARD_TRN_PK and t.status like 'Gate-Out%' and t.process=2) \"Gate-Out\" ,");
                    strQuery.Append("(SELECT TO_CHAR(MAX(INV.INVOICE_DATE),");
                    strQuery.Append(" dateFormatYY) ");
                    strQuery.Append(" FROM consol_invoice_tbl INV,");
                    strQuery.Append(" consol_invoice_trn_tbl INVTRN");
                    strQuery.Append(" WHERE INV.consol_invoice_pk =");
                    strQuery.Append(" INVTRN.consol_invoice_fk");
                    strQuery.Append(" and INVTRN.JOB_CARD_FK(+) =");
                    strQuery.Append(" jc.JOB_CARD_TRN_PK) \"Invoice\",");
                    strQuery.Append(" COMM.COMMODITY_GROUP_CODE \"Comm. Grp.\",");
                    //'
                    strQuery.Append("  (SELECT STATUS");
                    strQuery.Append("        FROM (SELECT T.STATUS, T.JOB_CARD_FK");
                    strQuery.Append("         FROM TRACK_N_TRACE_TBL T");
                    strQuery.Append("         WHERE T.BIZ_TYPE = 2");
                    strQuery.Append("         AND T.PROCESS = 2");
                    strQuery.Append("         ORDER BY T.CREATED_ON DESC)");
                    strQuery.Append("          WHERE ROWNUM = 1");
                    strQuery.Append("          AND JOB_CARD_FK =");
                    strQuery.Append("          JC.JOB_CARD_TRN_PK) \"Shipment. St.\",");
                    strQuery.Append("         'Sea' as \"Biz.Type\",");
                    //'
                    strQuery.Append(" null \"quotation_ref_no\",");
                    strQuery.Append(" null \"QUOTATION_MST_PK\", ");
                    //strQuery.Append(" null ""quotation_Status""," & vbCrLf)
                    strQuery.Append(" null \"status\",");
                    strQuery.Append(" null \"BOOKING_MST_PK\",");
                    strQuery.Append(" null \"BOOKING_REF_NO\",");
                    strQuery.Append(" null \"hbl_exp_tbl_pk\",");
                    strQuery.Append(" null \"hbl_status\",");
                    strQuery.Append(" null \"hbl_ref_no\",");
                    strQuery.Append(" null \"mbl_exp_tbl_pk\",");
                    strQuery.Append(" null \"Enquiry_BKG_SEA_PK\",");
                    strQuery.Append(" (select max(INV.CONSOL_INVOICE_PK)");
                    strQuery.Append("  from consol_invoice_tbl     inv,");
                    strQuery.Append("  consol_invoice_trn_tbl INVTRN");
                    strQuery.Append("  WHERE INV.consol_invoice_pk =");
                    strQuery.Append("  INVTRN.consol_invoice_fk");
                    strQuery.Append("  and INVTRN.JOB_CARD_FK(+) =");
                    strQuery.Append("  jc.JOB_CARD_TRN_PK) \"CONSOLPK\",");
                    strQuery.Append("  JC.CARGO_TYPE, ");
                    strQuery.Append("  CAN.CAN_PK, ");
                    strQuery.Append("  DMT.DELIVERY_ORDER_PK \"DO_PK\" , ");
                    strQuery.Append("  JC.JOB_CARD_TRN_PK \"JOB_PK\" , 0  ");
                    strQuery.Append("  from JOB_CARD_TRN     JC,");
                    strQuery.Append("  COMMODITY_GROUP_MST_TBL   COMM,");
                    strQuery.Append("  JOB_TRN_CONT cont,");
                    strQuery.Append("  CUSTOMER_MST_TBL     SH,");
                    strQuery.Append("  CUSTOMER_MST_TBL     CO,");
                    strQuery.Append("  PORT_MST_TBL         POL,");
                    strQuery.Append("  PORT_MST_TBL         POD,");
                    strQuery.Append("  AGENT_MST_TBL        POLA,");
                    strQuery.Append("  USER_MST_TBL         UMT,");
                    //'
                    strQuery.Append("   CAN_MST_TBL CAN,");
                    strQuery.Append("   DELIVERY_ORDER_MST_TBL DMT, ");
                    strQuery.Append("   CONSOL_INVOICE_TBL     CIT,");
                    strQuery.Append("   CONSOL_INVOICE_TRN_TBL CITT,");
                    strQuery.Append("   COLLECTIONS_TBL        CT,");
                    strQuery.Append("   COLLECTIONS_TRN_TBL    CTT,");
                    strQuery.Append("   JOB_TRN_COST        JTAEC,");
                    strQuery.Append("   INV_SUPPLIER_TBL       IST,");
                    strQuery.Append("   INV_SUPPLIER_TRN_TBL   ISTT,");
                    strQuery.Append("   PAYMENTS_TBL           PT, ");
                    strQuery.Append("   PAYMENT_TRN_TBL        PTT, ");
                    strQuery.Append("   VESSEL_VOYAGE_TBL         VVT,");
                    strQuery.Append("   VESSEL_VOYAGE_TRN         VTT,");
                    strQuery.Append("   INV_AGENT_TBL  IASET");
                    //'
                    strQuery.Append(" where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                    strQuery.Append(" AND jc.job_card_TRN_pk = cont.job_card_TRN_fk(+)");
                    strQuery.Append(" and COMM.COMMODITY_GROUP_PK(+) = jc.COMMODITY_GROUP_FK");
                    strQuery.Append("   AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                    strQuery.Append("   AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    strQuery.Append("   AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    strQuery.Append("   AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)");
                    strQuery.Append("   AND jc.CREATED_BY_FK = UMT.USER_MST_PK");
                    strQuery.Append("   AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)");
                    strQuery.Append("   AND JC.CARGO_TYPE = 1 ");
                    strQuery.Append("   AND JC.VOYAGE_TRN_FK = VTT.VOYAGE_TRN_PK");
                    strQuery.Append("   AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VTT.VESSEL_VOYAGE_TBL_FK");
                    //'
                    strQuery.Append("   AND CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK(+)");
                    strQuery.Append("   AND CITT.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                    strQuery.Append("   AND CTT.COLLECTIONS_TBL_FK = CT.COLLECTIONS_TBL_PK(+)");
                    strQuery.Append("   AND CTT.INVOICE_REF_NR(+) = CIT.INVOICE_REF_NO");
                    strQuery.Append("   AND JC.JOB_CARD_TRN_PK = JTAEC.JOB_CARD_TRN_FK(+)");
                    strQuery.Append("   AND ISTT.INV_SUPPLIER_TBL_FK = IST.INV_SUPPLIER_PK(+)");
                    strQuery.Append("   AND ISTT.JOB_TRN_EST_FK(+) = JTAEC.JOB_TRN_COST_PK");
                    strQuery.Append("   AND PTT.PAYMENTS_TBL_FK = PT.PAYMENT_TBL_PK(+)");
                    strQuery.Append("   AND PTT.INV_SUPPLIER_TBL_FK(+) = IST.INV_SUPPLIER_PK");
                    strQuery.Append("   AND IASET.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                    strQuery.Append("   AND CAN.JOB_CARD_FK(+)=JC.JOB_CARD_TRN_PK");
                    strQuery.Append("   AND DMT.JOB_CARD_MST_FK(+)=JC.JOB_CARD_TRN_PK");
                    strQuery.Append(" AND JC.BUSINESS_TYPE = 2 ");
                    strQuery.Append(" AND JC.PROCESS_TYPE = 2 ");
                    //'
                    strQuery.Append(addConditions);
                    strQuery.Append(addConditions1);
                    strQuery.Append("   and (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK in(" + LocFk + "))");
                    if (BizType == 3)
                    {
                        strQuery.Append(" ) )q ");
                    }
                    else
                    {
                        strQuery.Append(" )  ORDER BY TO_DATE(\"ATA\", '" + dateFormat + "') DESC,TO_DATE(\"Job Card\", '" + dateFormat + "') DESC)q ");
                    }
                    //If ShipStatus <> "All" Then
                    //    strQuery.Append("  WHERE Q.""Shipment.St."" LIKE '%" & ShipStatus.Replace("'", "''") & "%' " & vbCrLf)
                    //End If
                    strQuery.Append("  )");
                    // strQuery.Append("SELECT MAIN.*" & vbCrLf)
                    // strQuery.Append("  FROM (SELECT ROWNUM ""Sl.Nr."", Q.*" & vbCrLf)
                    //strQuery.Append("          FROM (SELECT * FROM  (SELECT DISTINCT JHDR.JOBCARD_REF_NO ""Document Nr.""," & vbCrLf)
                    //strQuery.Append("                                CMT.CUSTOMER_ID ""Cust.ID""," & vbCrLf)
                    //strQuery.Append("                                CMT.CUSTOMER_NAME CUSTOMERNAME," & vbCrLf)
                    //strQuery.Append("                                POL.PORT_ID POL," & vbCrLf)
                    //strQuery.Append("                                POD.PORT_ID POD," & vbCrLf)
                    //strQuery.Append("                                null ""Enquiry""," & vbCrLf)
                    //strQuery.Append("                                null ""Quotation""," & vbCrLf)
                    //strQuery.Append("                                null ""Booking""," & vbCrLf)

                    //'strQuery.Append("                                JHDR.JOBCARD_DATE ""Job Card""," & vbCrLf)
                    //strQuery.Append("                                TO_CHAR(JHDR.JOBCARD_DATE, '" & dateFormat & "') ""Job Card""," & vbCrLf)
                    //strQuery.Append("                                null ATD," & vbCrLf)
                    //strQuery.Append("                                null HBL," & vbCrLf)
                    //strQuery.Append("                                null MBL," & vbCrLf)
                    //strQuery.Append("                                (SELECT TO_CHAR(MAX(INV.INVOICE_DATE),'" & dateFormat & "')" & vbCrLf)
                    //strQuery.Append("                                   FROM INV_CUST_SEA_IMP_TBL INV" & vbCrLf)
                    //strQuery.Append("                                  WHERE INV.JOB_CARD_SEA_IMP_FK(+) =" & vbCrLf)
                    //strQuery.Append("                                        JHDR.JOB_CARD_SEA_IMP_PK) ""Invoice""," & vbCrLf)
                    ///added by manoj k sethi
                    //'strQuery.Append("   qhdr.quotation_ref_no," & vbCrLf)
                    //'strQuery.Append("   quotation_sea_pk," & vbCrLf)
                    //'strQuery.Append("   BHDR.BOOKING_SEA_PK," & vbCrLf)
                    //'strQuery.Append("   BHDR.BOOKING_REF_NO," & vbCrLf)
                    //'strQuery.Append("   hbl_exp_tbl_pk," & vbCrLf)
                    //'strQuery.Append("   hbl_status," & vbCrLf)
                    //'strQuery.Append("   hbl.hbl_ref_no," & vbCrLf)
                    //'strQuery.Append("   mbl_exp_tbl_pk," & vbCrLf)
                    //strQuery.Append("                               COMM.COMMODITY_GROUP_CODE ""Comm.Group""" & vbCrLf)
                    //strQuery.Append("                  FROM ")

                    //strQuery.Append("                  JOB_CARD_SEA_IMP_TBL      JHDR," & vbCrLf)
                    //'add by latha for fetching location wise
                    //strQuery.Append("                       USER_MST_TBL     UMT," & vbCrLf)
                    //strQuery.Append("                       CUSTOMER_MST_TBL          CMT," & vbCrLf)
                    //'manoj
                    //'strQuery.Append("   QUOTATION_SEA_TBL         QHDR," & vbCrLf)
                    //'strQuery.Append("   QUOTATION_TRN_SEA_FCL_LCL QTRN," & vbCrLf)
                    //'
                    //strQuery.Append("                       PORT_MST_TBL              POL," & vbCrLf)
                    //strQuery.Append("                       PORT_MST_TBL              POD," & vbCrLf)
                    //strQuery.Append("                       COMMODITY_GROUP_MST_TBL    COMM," & vbCrLf)
                    //strQuery.Append("                       VESSEL_VOYAGE_TBL         VVT," & vbCrLf)
                    //strQuery.Append("                       VESSEL_VOYAGE_TRN         VTT" & vbCrLf)
                    //strQuery.Append("      WHERE JHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)" & vbCrLf)
                    //strQuery.Append("      AND JHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)" & vbCrLf)
                    //strQuery.Append("      AND JHDR.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)" & vbCrLf)
                    //strQuery.Append("      AND COMM.COMMODITY_GROUP_PK(+)=JHDR.COMMODITY_GROUP_FK " & vbCrLf)
                    //strQuery.Append("      AND JHDR.VOYAGE_TRN_FK = VTT.VOYAGE_TRN_PK(+)" & vbCrLf)
                    //strQuery.Append("      AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VTT.VESSEL_VOYAGE_TBL_FK" & vbCrLf)
                    //strQuery.Append(addConditions)

                    // Added By : ANAND Reason : To Fetch all the Data For SEA IMPORT

                    //strQuery.Append("       CMT.CUSTOMER_ID ""Cust.ID""," & vbCrLf)
                    //strQuery.Append("       CMT.CUSTOMER_NAME CUSTOMERNAME," & vbCrLf)
                    //strQuery.Append("       POL.PORT_ID POL," & vbCrLf)
                    //strQuery.Append("       POD.PORT_ID POD," & vbCrLf)
                    //strQuery.Append("       TO_CHAR(EHDR.ENQUIRY_DATE," & vbCrLf)
                    //strQuery.Append("       'dd/MM/yyyy') ""Enquiry""," & vbCrLf)
                    //strQuery.Append("       TO_CHAR(QHDR.QUOTATION_DATE," & vbCrLf)
                    //strQuery.Append("       'dd/MM/yyyy') ""Quotation""," & vbCrLf)
                    //strQuery.Append("       null ""Booking""," & vbCrLf)
                    //strQuery.Append("       TO_CHAR(JHDR.JOBCARD_DATE," & vbCrLf)
                    //strQuery.Append("       'dd/MM/yyyy') ""Job Card""," & vbCrLf)
                    //strQuery.Append("       TO_CHAR(JHDR.DEPARTURE_DATE," & vbCrLf)
                    //strQuery.Append("       'dd/MM/yyyy') ATD," & vbCrLf)
                    //strQuery.Append("       null ""HBL""," & vbCrLf)
                    //strQuery.Append("       null ""MBL""," & vbCrLf)
                    //strQuery.Append("      (SELECT TO_CHAR(MAX(INV.INVOICE_DATE)," & vbCrLf)
                    //strQuery.Append("       'dd/MM/yyyy')" & vbCrLf)
                    //strQuery.Append("      FROM consol_invoice_tbl INV," & vbCrLf)
                    //strQuery.Append("      consol_invoice_trn_tbl INVTRN" & vbCrLf)
                    //strQuery.Append("      WHERE INV.consol_invoice_pk =" & vbCrLf)
                    //strQuery.Append("      INVTRN.consol_invoice_fk" & vbCrLf)
                    //strQuery.Append("      and INVTRN.JOB_CARD_FK(+) =" & vbCrLf)
                    //strQuery.Append("      JHDR.JOB_CARD_SEA_IMP_PK) ""Invoice""," & vbCrLf)
                    //strQuery.Append("      COMM.COMMODITY_GROUP_CODE ""Comm.Group""," & vbCrLf)
                    //strQuery.Append("      qhdr.quotation_ref_no," & vbCrLf)
                    //strQuery.Append("      quotation_sea_pk," & vbCrLf)
                    //strQuery.Append("      null ""BOOKING_SEA_PK""," & vbCrLf)
                    //strQuery.Append("      null ""BOOKING_REF_NO""," & vbCrLf)
                    //strQuery.Append("      null ""hbl_exp_tbl_pk""," & vbCrLf)
                    //strQuery.Append("      null ""hbl_status""," & vbCrLf)
                    //strQuery.Append("      null ""hbl_ref_no""," & vbCrLf)
                    //strQuery.Append("      null ""mbl_exp_tbl_pk""," & vbCrLf)
                    //strQuery.Append("      EHDR.Enquiry_BKG_SEA_PK," & vbCrLf)
                    //strQuery.Append("      (select max(INV.CONSOL_INVOICE_PK)" & vbCrLf)
                    //strQuery.Append("      from consol_invoice_tbl inv," & vbCrLf)
                    //strQuery.Append("      consol_invoice_trn_tbl INVTRN" & vbCrLf)
                    //strQuery.Append("      WHERE INV.consol_invoice_pk =" & vbCrLf)
                    //strQuery.Append("      INVTRN.consol_invoice_fk " & vbCrLf)
                    //strQuery.Append("      and INVTRN.JOB_CARD_FK(+) =" & vbCrLf)
                    //strQuery.Append("      JHDR.JOB_CARD_SEA_IMP_PK) ""CONSOLPK""" & vbCrLf)
                    //strQuery.Append("      FROM JOB_CARD_SEA_IMP_TBL JHDR," & vbCrLf)
                    //strQuery.Append("      USER_MST_TBL UMT," & vbCrLf)
                    //strQuery.Append("      CUSTOMER_MST_TBL CMT," & vbCrLf)
                    //strQuery.Append("      PORT_MST_TBL POL," & vbCrLf)
                    //strQuery.Append("      PORT_MST_TBL POD," & vbCrLf)
                    //strQuery.Append("      COMMODITY_GROUP_MST_TBL COMM," & vbCrLf)
                    //strQuery.Append("      VESSEL_VOYAGE_TBL  VVT," & vbCrLf)
                    //strQuery.Append("      VESSEL_VOYAGE_TRN  VTT," & vbCrLf)
                    //strQuery.Append("      BOOKING_SEA_TBL    BHDR," & vbCrLf)
                    //strQuery.Append("      BOOKING_TRN_SEA_FCL_LCL BTRN," & vbCrLf)
                    //strQuery.Append("      QUOTATION_SEA_TBL QHDR," & vbCrLf)
                    //strQuery.Append("      QUOTATION_TRN_SEA_FCL_LCL QTRN," & vbCrLf)
                    //strQuery.Append("      ENQUIRY_BKG_SEA_TBL       EHDR" & vbCrLf)

                    //strQuery.Append("      WHERE(JHDR.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK)" & vbCrLf)
                    //strQuery.Append("      AND COMM.COMMODITY_GROUP_PK(+) =JHDR.COMMODITY_GROUP_FK" & vbCrLf)
                    //strQuery.Append("      AND JHDR.VOYAGE_TRN_FK = VTT.VOYAGE_TRN_PK(+)" & vbCrLf)
                    //strQuery.Append("      and BHDR.Vessel_Voyage_Fk= VTT.VOYAGE_TRN_PK" & vbCrLf)
                    //strQuery.Append("       and jhdr.voyage_trn_fk=BHDR.Vessel_Voyage_Fk" & vbCrLf)
                    //strQuery.Append("       and BHDR.Vessel_Voyage_Fk = VTT.VOYAGE_TRN_PK" & vbCrLf)
                    //strQuery.Append("       and JHDR.CONSIGNEE_CUST_MST_FK =4861" & vbCrLf)
                    //strQuery.Append("   and (jhdr.PORT_MST_POD_FK " & vbCrLf)
                    //strQuery.Append("IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " & LocFk & ")  and jhdr.JC_AUTO_MANUAL = 1)" & vbCrLf)
                    //strQuery.Append("      AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VTT.VESSEL_VOYAGE_TBL_FK" & vbCrLf)
                    //strQuery.Append("      AND BHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)" & vbCrLf)
                    //strQuery.Append("      AND BHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)" & vbCrLf)
                    //strQuery.Append("      AND BTRN.BOOKING_SEA_FK(+) = BHDR.BOOKING_SEA_PK" & vbCrLf)
                    //strQuery.Append("      AND BHDR.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)" & vbCrLf)
                    //strQuery.Append("      AND BTRN.TRANS_REF_NO = QHDR.QUOTATION_REF_NO(+)" & vbCrLf)
                    //strQuery.Append("      AND QTRN.QUOTATION_SEA_FK(+) = QHDR.QUOTATION_SEA_PK" & vbCrLf)
                    //strQuery.Append("      AND QTRN.TRANS_REF_NO = EHDR.ENQUIRY_REF_NO(+)" & vbCrLf)
                    //strQuery.Append("      AND UMT.DEFAULT_LOCATION_FK ='" & LocFk & "'" & vbCrLf)
                    //strQuery.Append("      AND JHDR.CREATED_BY_FK = UMT.USER_MST_PK" & vbCrLf)
                    //strQuery.Append("      AND to_char(JHDR.JOBCARD_DATE) BETWEEN" & vbCrLf)
                    //strQuery.Append("      TO_DATE('01/01/2008', DATEFORMAT) and" & vbCrLf)
                    //strQuery.Append("      to_date('09/04/2008', DATEFORMAT)" & vbCrLf)
                    //strQuery.Append(" ORDER BY TO_DATE(""Job Card"", '" & dateFormat & "') DESC,""Document Nr."" DESC    )")
                    //strQuery.Append("   ) Q) MAIN")
                    //  strQuery.Append("   WHERE ""Sl.Nr.""    BETWEEN " & startPage & " AND " & endPage & vbCrLf)
                }
                else if (bussinessType == "AE")
                {
                    strQuery.Append("   SELECT MAIN.*");
                    if (BizType == 3)
                    {
                        strQuery.Append("   FROM (SELECT Q.* ");
                    }
                    else
                    {
                        strQuery.Append("   FROM (SELECT ROWNUM AS \"Sl. Nr.\"   , Q.* ");
                    }
                    strQuery.Append("   FROM (SELECT * FROM  (SELECT DISTINCT JHDR.JOBCARD_REF_NO \"Job Card Int32\",");
                    strQuery.Append("                                CMT.CUSTOMER_ID \"Cust.ID\",");
                    strQuery.Append("                                CMT.CUSTOMER_NAME \"Customer Name\",");
                    if (BizType == 3)
                    {
                        strQuery.Append("                                POL.PORT_ID \"From\",");
                        strQuery.Append("                                POD.PORT_ID \"To\",");
                    }
                    else
                    {
                        strQuery.Append("                                POL.PORT_ID \"From\",");
                        strQuery.Append("                                POD.PORT_ID \"To\",");
                    }
                    strQuery.Append("                                TO_CHAR(EHDR.ENQUIRY_DATE,dateFormatYY) \"Enquiry\",");
                    strQuery.Append("                                TO_CHAR(QHDR.QUOTATION_DATE,dateFormatYY) \"Quotation\",");
                    strQuery.Append("                                TO_CHAR(BHDR.BOOKING_DATE,dateFormatYY) \"Booking\",");
                    strQuery.Append("                                TO_CHAR(JHDR.JOBCARD_DATE,dateFormatYY) \"Job Card\",");
                    strQuery.Append("                                TO_CHAR(JHDR.DEPARTURE_DATE,dateFormatYY) ATD,");
                    if (BizType == 3)
                    {
                        strQuery.Append("                                TO_CHAR(HAWB.HAWB_DATE,dateFormatYY) \"HBL/HAWB\",");
                        strQuery.Append("                                TO_CHAR(MAWB.MAWB_DATE,dateFormatYY) \"MBL/MAWB\",");
                    }
                    else
                    {
                        strQuery.Append("                                TO_CHAR(HAWB.HAWB_DATE,dateFormatYY) \"HAWB\",");
                        strQuery.Append("                                TO_CHAR(MAWB.MAWB_DATE,dateFormatYY) \"MAWB\",");
                    }
                    strQuery.Append(" (select TO_CHAR(t.created_on,dateFormatYY) from TRACK_N_TRACE_TBL t where t.job_card_fk=JHDR.JOB_CARD_TRN_PK and t.status='Arrival Date' and t.process=1 and rownum=1) \"ATA\" ,");
                    //strQuery.Append(" (select TO_CHAR(t.created_on,dateFormatYY) from TRACK_N_TRACE_TBL t where t.job_card_fk=JHDR.JOB_CARD_TRN_PK and t.status='Cargo Arrival Notice Generated' and t.process=1) ""CAN""," & vbCrLf)
                    //strQuery.Append(" (select TO_CHAR(t.created_on,dateFormatYY) from TRACK_N_TRACE_TBL t where t.job_card_fk=JHDR.JOB_CARD_TRN_PK and t.status='Delivery Order Generated' and t.process=1) ""DO"" ," & vbCrLf)
                    strQuery.Append(" (SELECT TO_CHAR(T.CREATED_ON,DATEFORMATYY) from TRACK_N_TRACE_TBL T,CAN_MST_TBL CAN,JOB_CARD_TRN JC WHERE JHDR.JOBCARD_REF_NO=JC.JOBCARD_REF_NO AND T.DOC_REF_NO=CAN.CAN_REF_NO AND CAN.JOB_CARD_FK=JC.JOB_CARD_TRN_PK AND T.STATUS='Cargo Arrival Notice Generated' AND ROWNUM=1) \"CAN\",");
                    strQuery.Append(" (SELECT TO_CHAR(T.CREATED_ON,DATEFORMATYY) FROM TRACK_N_TRACE_TBL T,DELIVERY_ORDER_MST_TBL DEL,JOB_CARD_TRN JC WHERE JHDR.JOBCARD_REF_NO=JC.JOBCARD_REF_NO AND T.DOC_REF_NO=DEL.DELIVERY_ORDER_REF_NO AND DEL.JOB_CARD_MST_FK=JC.JOB_CARD_TRN_PK AND T.STATUS='Delivery Order Generated' AND ROWNUM=1) \"DO\", ");
                    strQuery.Append(" (select TO_CHAR(max(t.created_on),dateFormatYY) from TRACK_N_TRACE_TBL t where t.job_card_fk=JHDR.JOB_CARD_TRN_PK and t.status like 'Gate-Out%' and t.process=1) \"Gate-Out\" ,");
                    strQuery.Append("                                (SELECT TO_CHAR(MAX(INV.INVOICE_DATE),dateFormatYY) ");
                    //strQuery.Append("                                   FROM INV_CUST_AIR_EXP_TBL INV" & vbCrLf)
                    //strQuery.Append("                                  WHERE INV.JOB_CARD_AIR_EXP_FK(+) =" & vbCrLf)
                    strQuery.Append("                                   FROM consol_invoice_tbl INV,");
                    strQuery.Append("                                 consol_invoice_trn_tbl INVTRN");
                    strQuery.Append("                                  WHERE INV.consol_invoice_pk =");
                    strQuery.Append("                                  INVTRN.consol_invoice_fk");
                    strQuery.Append("                                  and INVTRN.JOB_CARD_FK(+) =");
                    strQuery.Append("                                        JHDR.JOB_CARD_TRN_PK) \"Invoice\",");
                    strQuery.Append("                                        COMM.COMMODITY_GROUP_CODE \"Comm. Grp.\",");
                    //'
                    strQuery.Append("  (SELECT STATUS");
                    strQuery.Append("        FROM (SELECT T.STATUS, T.JOB_CARD_FK");
                    strQuery.Append("         FROM TRACK_N_TRACE_TBL T");
                    strQuery.Append("         WHERE T.BIZ_TYPE = 1");
                    strQuery.Append("         AND T.PROCESS = 1");
                    strQuery.Append("         ORDER BY T.CREATED_ON DESC)");
                    strQuery.Append("          WHERE ROWNUM = 1");
                    strQuery.Append("          AND JOB_CARD_FK =");
                    strQuery.Append("          JHDR.JOB_CARD_TRN_PK) \"Shipment. St.\",");
                    strQuery.Append("         'Air' as \"Biz.Type\",");
                    //'
                    //Added by Manoj K Sethi
                    strQuery.Append("        qhdr.quotation_ref_no,");
                    strQuery.Append("        qhdr.QUOTATION_MST_PK \"quotation_sea_pk\",");
                    strQuery.Append("        qhdr.status,");
                    strQuery.Append("        BHDR.BOOKING_MST_PK \"BOOKING_SEA_PK\",");
                    strQuery.Append("        BHDR.BOOKING_REF_NO,");
                    strQuery.Append("   hawb_exp_tbl_pk \"hbl_exp_tbl_pk\",");
                    strQuery.Append("   hawb_status \"hbl_status\",");
                    strQuery.Append("   hawb.hawb_ref_no \"hbl_ref_no\",");
                    strQuery.Append("   mawb_exp_tbl_pk \"mbl_exp_tbl_pk\",");
                    strQuery.Append("   EHDR.Enquiry_BKG_AIR_PK \"Enquiry_BKG_SEA_PK\",");
                    strQuery.Append("           (select max(INV.CONSOL_INVOICE_PK)");
                    strQuery.Append("            from consol_invoice_tbl INV,");
                    strQuery.Append("       consol_invoice_trn_tbl INVTRN");
                    strQuery.Append("            where INV.consol_invoice_pk =");
                    //strQuery.Append("           (SELECT (MAX(INV.CONSOL_INVOICE_PK))" & vbCrLf)
                    //strQuery.Append("           FROM consol_invoice_tbl     INV," & vbCrLf)
                    //strQuery.Append("           JOB_CARD_AIR_EXP_TBL   JHDR," & vbCrLf)
                    strQuery.Append("          INVTRN.consol_invoice_fk");
                    //strQuery.Append("           WHERE INV.JOB_CARD_AIR_EXP_FK(+) =" & vbCrLf)
                    //strQuery.Append("           JHDR.JOB_CARD_AIR_EXP_PK" & vbCrLf)
                    strQuery.Append("            and INVTRN.JOB_CARD_FK(+) =");
                    strQuery.Append("           JHDR.JOB_CARD_TRN_PK)\"CONSOLPK\",");
                    strQuery.Append("           BHDR.CARGO_TYPE ,");
                    strQuery.Append("           NULL \"CAN_PK\" ,");
                    strQuery.Append("           NULL \"DO_PK\" ,");
                    strQuery.Append("           JHDR.JOB_CARD_TRN_PK \"JOB_PK\" , QHDR.QUOTATION_TYPE ");
                    //Ended by Manoj K Sethi
                    strQuery.Append("FROM");
                    strQuery.Append("                  Mawb_EXP_TBL         MAWB,");
                    strQuery.Append("                       hawb_EXP_TBL         HAWB,");
                    strQuery.Append("                       JOB_CARD_TRN        JHDR,");
                    //add by latha for fetching location wise
                    strQuery.Append("                       USER_MST_TBL     UMT,");
                    strQuery.Append("                       BOOKING_MST_TBL      BHDR,");
                    strQuery.Append("                       BOOKING_TRN       BTRN,");
                    strQuery.Append("                       QUOTATION_MST_TBL    QHDR,");
                    strQuery.Append("                       QUOTATION_DTL_TBL    QTRN,");
                    strQuery.Append("                       ENQUIRY_BKG_Air_TBL  EHDR,");
                    strQuery.Append("                       CUSTOMER_MST_TBL     CMT,");
                    strQuery.Append("                       PORT_MST_TBL         POL,");
                    strQuery.Append("                       PORT_MST_TBL         POD,");
                    strQuery.Append("                       AIRLINE_MST_TBL      AIR,");
                    // Added
                    strQuery.Append("                       COMMODITY_GROUP_MST_TBL    COMM,");
                    //'
                    strQuery.Append("   CONSOL_INVOICE_TBL     CIT,");
                    strQuery.Append("   CONSOL_INVOICE_TRN_TBL CITT,");
                    strQuery.Append("   COLLECTIONS_TBL        CT,");
                    strQuery.Append("   COLLECTIONS_TRN_TBL    CTT,");
                    strQuery.Append("   JOB_TRN_COST   JTAEC,");
                    strQuery.Append("   INV_SUPPLIER_TBL       IST,");
                    strQuery.Append("   INV_SUPPLIER_TRN_TBL   ISTT,");
                    strQuery.Append("   PAYMENTS_TBL           PT, ");
                    strQuery.Append("   PAYMENT_TRN_TBL        PTT, ");
                    strQuery.Append("   INV_AGENT_TBL  IASET");
                    //'
                    strQuery.Append("                 WHERE BHDR.BOOKING_MST_PK = JHDR.BOOKING_MST_FK");
                    strQuery.Append("                 AND BHDR.STATUS<>3 ");
                    strQuery.Append("                   AND JHDR.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                    strQuery.Append("                   AND JHDR.MBL_MAWB_FK = MAWB.MAWB_EXP_TBL_PK(+)");
                    strQuery.Append("                   AND BHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    strQuery.Append("                   AND BHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    //strQuery.Append("                   AND BHDR.AIRLINE_MST_FK = AIR.AIRLINE_MST_PK(+)" & vbCrLf)
                    strQuery.Append("                   AND BTRN.BOOKING_MST_FK(+) = BHDR.BOOKING_MST_PK");
                    strQuery.Append("                   AND BHDR.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    strQuery.Append("                   AND BTRN.TRANS_REF_NO = QHDR.QUOTATION_REF_NO(+)");
                    strQuery.Append("                   AND QTRN.QUOTATION_MST_FK(+) = QHDR.QUOTATION_MST_PK");
                    strQuery.Append("                   AND QTRN.TRANS_REF_NO = EHDR.ENQUIRY_REF_NO(+)");
                    strQuery.Append("                   AND COMM.COMMODITY_GROUP_PK(+)=JHDR.COMMODITY_GROUP_FK ");
                    //'
                    strQuery.Append("   AND CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK(+)");
                    strQuery.Append("   AND CITT.JOB_CARD_FK(+) = JHDR.JOB_CARD_TRN_PK");
                    strQuery.Append("   AND CTT.COLLECTIONS_TBL_FK = CT.COLLECTIONS_TBL_PK(+)");
                    strQuery.Append("   AND CTT.INVOICE_REF_NR(+) = CIT.INVOICE_REF_NO");
                    strQuery.Append("   AND JHDR.JOB_CARD_TRN_PK = JTAEC.JOB_CARD_TRN_FK(+)");
                    strQuery.Append("   AND ISTT.INV_SUPPLIER_TBL_FK = IST.INV_SUPPLIER_PK(+)");
                    strQuery.Append("   AND ISTT.JOB_TRN_EST_FK(+) = JTAEC.JOB_TRN_COST_PK");
                    strQuery.Append("   AND PTT.PAYMENTS_TBL_FK = PT.PAYMENT_TBL_PK(+)");
                    strQuery.Append("   AND PTT.INV_SUPPLIER_TBL_FK(+) = IST.INV_SUPPLIER_PK");
                    strQuery.Append("   AND IASET.JOB_CARD_FK(+) = JHDR.JOB_CARD_TRN_PK");
                    strQuery.Append(" AND JHDR.BUSINESS_TYPE =1 ");
                    strQuery.Append(" AND JHDR.PROCESS_TYPE = 1 ");
                    //'
                    strQuery.Append(addConditions);
                    strQuery.Append(addConditions1);
                    if (BizType == 3)
                    {
                        strQuery.Append(" ) WHERE 1=1");
                    }
                    else
                    {
                        strQuery.Append(" ORDER BY TO_DATE(\"ATD\", '" + dateFormat + "') DESC, TO_DATE(\"Job Card\", '" + dateFormat + "') DESC) WHERE 1=1");
                    }
                    if (ShipStatus == "CAN")
                    {
                        strQuery.Append(" AND \"CAN\" IS NULL");
                    }
                    if (ShipStatus == "DO")
                    {
                        strQuery.Append(" AND \"DO\" IS NULL");
                    }
                    if (ShipStatus == "Gate Out")
                    {
                        strQuery.Append(" AND \"Gate-Out\" IS NULL");
                    }
                    //strQuery.Append("   ) Q) MAIN")
                    //If ShipStatus <> "All" Then
                    //    strQuery.Append("   ) Q")
                    //    strQuery.Append(" WHERE Q.""Shipment.St."" LIKE '%" & ShipStatus.Replace("'", "''") & "%' " & vbCrLf)
                    //    strQuery.Append("  ) MAIN")
                    //Else
                    strQuery.Append("   ) Q) MAIN");
                    //End If
                    // strQuery.Append("   WHERE ""Sl.Nr.""    BETWEEN " & startPage & " AND " & endPage & vbCrLf)
                }
                else if (bussinessType == "AI")
                {
                    strQuery.Append("Select *");
                    if (BizType == 3)
                    {
                        strQuery.Append("   FROM (SELECT Q.* ");
                    }
                    else
                    {
                        strQuery.Append("   FROM (SELECT ROWNUM AS \"Sl. Nr.\"   , Q.* ");
                    }

                    strQuery.Append("from (SELECT DISTINCT JC.JOBCARD_REF_NO \"Job Card Int32\",");
                    strQuery.Append("  CO.CUSTOMER_ID \"Cust.ID\",");
                    strQuery.Append("  CO.CUSTOMER_NAME \"Customer Name\",");
                    if (BizType == 3)
                    {
                        strQuery.Append("                                POL.PORT_ID \"From\",");
                        strQuery.Append("                                POD.PORT_ID \"To\",");
                    }
                    else
                    {
                        strQuery.Append("                                POL.PORT_ID \"From\",");
                        strQuery.Append("                                POD.PORT_ID \"To\",");
                    }
                    strQuery.Append("  null \"Enquiry\",");
                    strQuery.Append("  null \"Quotation\",");
                    strQuery.Append("  null \"Booking\",");
                    strQuery.Append("  TO_CHAR(JC.JOBCARD_DATE,");
                    strQuery.Append("  dateFormatYY) \"Job Card\",");
                    strQuery.Append("(select TO_CHAR(max(t.created_on),dateFormatYY) from TRACK_N_TRACE_TBL t where t.job_card_fk=jc.JOB_CARD_TRN_PK and t.process=2 and t.status like'Sail%')ATD,");
                    if (BizType == 3)
                    {
                        strQuery.Append("  null \"HBL/HAWB\",");
                        strQuery.Append("  null \"MBL/MAWB\",");
                    }
                    else
                    {
                        strQuery.Append("  null \"HAWB\",");
                        strQuery.Append("  null \"MAWB\",");
                    }
                    strQuery.Append("  TO_CHAR(JC.ARRIVAL_DATE,");
                    strQuery.Append("  dateFormatYY) ATA,");
                    strQuery.Append("  TO_CHAR(CAN.CAN_DATE, dateFormatYY) \"CAN\",");
                    strQuery.Append("  TO_CHAR(DMT.DELIVERY_ORDER_DATE, dateFormatYY) \"DO\" ,");
                    strQuery.Append(" (select TO_CHAR(max(t.created_on),dateFormatYY) from TRACK_N_TRACE_TBL t where t.job_card_fk=JC.JOB_CARD_TRN_PK and t.status like 'Gate-Out%' and t.process=2) \"Gate-Out\" ,");
                    strQuery.Append("  (SELECT TO_CHAR(MAX(INV.INVOICE_DATE),");
                    strQuery.Append("  dateFormatYY)  ");
                    strQuery.Append("  FROM INV_CUST_Air_IMP_TBL INV");
                    strQuery.Append("  WHERE INV.JOB_CARD_AIR_IMP_FK(+) =");
                    strQuery.Append("  JC.JOB_CARD_TRN_PK) \"Invoice\",");
                    strQuery.Append("  COMM.COMMODITY_GROUP_CODE \"Comm. Grp.\",");
                    //'
                    strQuery.Append("  (SELECT STATUS");
                    strQuery.Append("        FROM (SELECT T.STATUS, T.JOB_CARD_FK");
                    strQuery.Append("         FROM TRACK_N_TRACE_TBL T");
                    strQuery.Append("         WHERE T.BIZ_TYPE = 1");
                    strQuery.Append("         AND T.PROCESS = 2");
                    strQuery.Append("         ORDER BY T.CREATED_ON DESC)");
                    strQuery.Append("          WHERE ROWNUM = 1");
                    strQuery.Append("          AND JOB_CARD_FK =");
                    strQuery.Append("          JC.JOB_CARD_TRN_PK) \"Shipment. St.\",");
                    strQuery.Append("         'Air' as \"Biz.Type\",");
                    //'
                    strQuery.Append("  null quotation_ref_no,");
                    strQuery.Append("  null QUOTATION_MST_PK,");
                    strQuery.Append("  null Status,");
                    strQuery.Append("  null BOOKING_MST_PK,");
                    strQuery.Append("  null BOOKING_REF_NO,");
                    strQuery.Append("  null hbl_exp_tbl_pk,");
                    strQuery.Append("  null hbl_status,");
                    strQuery.Append("  null hbl_ref_no,");
                    strQuery.Append("  null mbl_exp_tbl_pk,");
                    strQuery.Append("  null Enquiry_BKG_SEA_PK,");

                    //added by manoj
                    strQuery.Append(" (select max(INV.INV_CUST_AIR_IMP_PK)");
                    strQuery.Append(" from INV_CUST_Air_IMP_TBL     INV,");
                    strQuery.Append(" consol_invoice_trn_tbl INVTRN");
                    strQuery.Append(" where INV.JOB_CARD_AIR_IMP_FK(+) =");
                    strQuery.Append(" JC.JOB_CARD_TRN_PK) \"CONSOLPK\",");
                    strQuery.Append(" 0 CARGO_TYPE, ");
                    strQuery.Append(" CAN.CAN_PK , ");
                    strQuery.Append(" DMT.DELIVERY_ORDER_PK \"DO_PK\"  ,");
                    strQuery.Append(" JC.JOB_CARD_TRN_PK \"JOB_PK\"  , 0 ");
                    //end
                    //strQuery.Append(" (select max(INV.CONSOL_INVOICE_PK)" & vbCrLf)
                    //strQuery.Append(" from consol_invoice_tbl     INV," & vbCrLf)
                    //strQuery.Append(" consol_invoice_trn_tbl INVTRN" & vbCrLf)
                    //strQuery.Append(" where INV.consol_invoice_pk =" & vbCrLf)
                    //strQuery.Append(" INVTRN.consol_invoice_fk" & vbCrLf)
                    //strQuery.Append(" and INVTRN.JOB_CARD_FK(+) = " & vbCrLf)
                    //strQuery.Append(" JC.JOB_CARD_AIR_IMP_PK) ""CONSOLPK"" " & vbCrLf)
                    strQuery.Append(" from JOB_CARD_TRN  JC,");
                    strQuery.Append(" CUSTOMER_MST_TBL     SH,");
                    strQuery.Append(" CUSTOMER_MST_TBL     CO,");
                    strQuery.Append(" PORT_MST_TBL         POL,");
                    strQuery.Append(" PORT_MST_TBL         POD,");
                    strQuery.Append(" AGENT_MST_TBL        POLA,");
                    strQuery.Append(" USER_MST_TBL         UMT,");
                    strQuery.Append(" COMMODITY_GROUP_MST_TBL COMM,");
                    //'
                    strQuery.Append("   CAN_MST_TBL CAN,");
                    strQuery.Append("   DELIVERY_ORDER_MST_TBL DMT, ");
                    strQuery.Append("   CONSOL_INVOICE_TBL     CIT,");
                    strQuery.Append("   CONSOL_INVOICE_TRN_TBL CITT,");
                    strQuery.Append("   COLLECTIONS_TBL        CT,");
                    strQuery.Append("   COLLECTIONS_TRN_TBL    CTT,");
                    strQuery.Append("   JOB_TRN_COST   JTAEC,");
                    strQuery.Append("   INV_SUPPLIER_TBL       IST,");
                    strQuery.Append("   INV_SUPPLIER_TRN_TBL   ISTT,");
                    strQuery.Append("   PAYMENTS_TBL           PT, ");
                    strQuery.Append("   PAYMENT_TRN_TBL        PTT,");
                    strQuery.Append("   INV_AGENT_TBL  IASET");
                    //'
                    strQuery.Append(" where JC.CONSIGNEE_CUST_MST_FK = CO.CUSTOMER_MST_PK(+)");
                    strQuery.Append(" AND COMM.COMMODITY_GROUP_PK(+) = JC.COMMODITY_GROUP_FK");
                    strQuery.Append(" AND JC.SHIPPER_CUST_MST_FK = SH.CUSTOMER_MST_PK(+)");
                    strQuery.Append(" AND JC.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    strQuery.Append(" AND JC.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    strQuery.Append(" AND JC.POL_AGENT_MST_FK = POLA.AGENT_MST_PK(+)");
                    strQuery.Append(" AND (JC.JOB_CARD_STATUS IS NULL OR JC.JOB_CARD_STATUS = 1)");
                    strQuery.Append(" AND jc.CREATED_BY_FK = UMT.USER_MST_PK");
                    //'
                    strQuery.Append("   AND CITT.CONSOL_INVOICE_FK = CIT.CONSOL_INVOICE_PK(+)");
                    strQuery.Append("   AND CITT.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                    strQuery.Append("   AND CTT.COLLECTIONS_TBL_FK = CT.COLLECTIONS_TBL_PK(+)");
                    strQuery.Append("   AND CTT.INVOICE_REF_NR(+) = CIT.INVOICE_REF_NO");
                    strQuery.Append("   AND JC.JOB_CARD_TRN_PK = JTAEC.JOB_CARD_TRN_FK(+)");
                    strQuery.Append("   AND ISTT.INV_SUPPLIER_TBL_FK = IST.INV_SUPPLIER_PK(+)");
                    strQuery.Append("   AND ISTT.JOB_TRN_EST_FK(+) = JTAEC.JOB_TRN_COST_PK");
                    strQuery.Append("   AND PTT.PAYMENTS_TBL_FK = PT.PAYMENT_TBL_PK(+)");
                    strQuery.Append("   AND PTT.INV_SUPPLIER_TBL_FK(+) = IST.INV_SUPPLIER_PK");
                    strQuery.Append("   AND IASET.JOB_CARD_FK(+) = JC.JOB_CARD_TRN_PK");
                    strQuery.Append("   AND CAN.JOB_CARD_FK(+)=JC.JOB_CARD_TRN_PK");
                    strQuery.Append("   AND DMT.JOB_CARD_MST_FK(+)=JC.JOB_CARD_TRN_PK");
                    strQuery.Append(" AND JC.BUSINESS_TYPE = 1 ");
                    strQuery.Append(" AND JC.PROCESS_TYPE = 2 ");
                    //'
                    strQuery.Append(addConditions);
                    strQuery.Append(addConditions1);
                    strQuery.Append(" AND (JC.PORT_MST_POD_FK IN (SELECT T.PORT_MST_FK  FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK in(" + LocFk + ") )");
                    if (BizType == 3)
                    {
                        strQuery.Append(" ) )q");
                    }
                    else
                    {
                        strQuery.Append(" )  ORDER BY TO_DATE(\"ATA\", '" + dateFormat + "') DESC,TO_DATE(\"Job Card\", '" + dateFormat + "') DESC)q");
                    }
                    //If ShipStatus <> "All" Then
                    //    strQuery.Append(" WHERE Q.""Shipment.St."" LIKE '%" & ShipStatus.Replace("'", "''") & "%' " & vbCrLf)
                    //End If
                    strQuery.Append("  )");
                    //strQuery.Append("   SELECT MAIN.*" & vbCrLf)
                    //strQuery.Append("    FROM (SELECT ROWNUM AS ""Sl.Nr.""   , Q.* ")
                    //strQuery.Append("   FROM (SELECT * FROM  (SELECT DISTINCT JHDR.JOBCARD_REF_NO ""Document Nr.""," & vbCrLf)
                    //strQuery.Append("                                CMT.CUSTOMER_ID     ""Cust.ID""," & vbCrLf)
                    //strQuery.Append("                                CMT.CUSTOMER_NAME   CUSTOMERNAME," & vbCrLf)
                    //strQuery.Append("                                POL.PORT_ID         AOO," & vbCrLf)
                    //strQuery.Append("                                POD.PORT_ID         AOD," & vbCrLf)
                    //strQuery.Append("                                null                ""Enquiry""," & vbCrLf)
                    //strQuery.Append("                                null                ""Quotation""," & vbCrLf)
                    //strQuery.Append("                                null                ""Booking""," & vbCrLf)
                    //strQuery.Append("                                JHDR.JOBCARD_DATE ""Job Card""," & vbCrLf)
                    //strQuery.Append("                                TO_CHAR(JHDR.JOBCARD_DATE, '" & dateFormat & "') ""Job Card""," & vbCrLf)
                    //strQuery.Append("                                TO_CHAR(JHDR.DEPARTURE_DATE," & vbCrLf)
                    //strQuery.Append("                                'dd/MM/yyyy') ATD," & vbCrLf)
                    //strQuery.Append("                                null HAWB," & vbCrLf)
                    //strQuery.Append("                                null MAWB," & vbCrLf)
                    //strQuery.Append("                                (SELECT TO_CHAR(MAX(INV.INVOICE_DATE),'" & dateFormat & "')" & vbCrLf)
                    //strQuery.Append("                                   FROM INV_CUST_Air_IMP_TBL INV" & vbCrLf)
                    //strQuery.Append("                                  WHERE INV.JOB_CARD_Air_IMP_FK(+) =" & vbCrLf)
                    //strQuery.Append("                                        JHDR.JOB_CARD_Air_IMP_PK) ""Invoice""," & vbCrLf)
                    //'Added by Manoj K Sethi
                    //strQuery.Append("        qhdr.quotation_ref_no," & vbCrLf)
                    //strQuery.Append("        qhdr.quotation_air_pk," & vbCrLf)
                    //strQuery.Append("        qhdr.status," & vbCrLf)
                    //strQuery.Append("        BHDR.BOOKING_Air_PK, " & vbCrLf)
                    //strQuery.Append("        BHDR.BOOKING_REF_NO," & vbCrLf)
                    //strQuery.Append("   hawb_exp_tbl_pk," & vbCrLf)
                    //strQuery.Append("   hawb_status," & vbCrLf)
                    //strQuery.Append("   hawb.hawb_ref_no," & vbCrLf)
                    //strQuery.Append("   mawb_exp_tbl_pk," & vbCrLf)
                    //strQuery.Append("   COMM.COMMODITY_GROUP_CODE ""Comm.Group""," & vbCrLf)
                    //strQuery.Append("   null quotation_ref_no," & vbCrLf)
                    //strQuery.Append("   null quotation_air_pk," & vbCrLf)
                    //strQuery.Append("   null status," & vbCrLf)
                    //strQuery.Append("   null BOOKING_Air_PK," & vbCrLf)
                    //strQuery.Append("   null BOOKING_REF_NO," & vbCrLf)
                    //strQuery.Append("   null hawb_exp_tbl_pk," & vbCrLf)
                    //strQuery.Append("   null hawb_status," & vbCrLf)
                    //strQuery.Append("   null hawb_ref_no," & vbCrLf)
                    //strQuery.Append("   null mawb_exp_tbl_pk," & vbCrLf)
                    //strQuery.Append("   null Enquiry_BKG_AIR_PK," & vbCrLf)
                    //strQuery.Append("   (select max(INV.CONSOL_INVOICE_PK)" & vbCrLf)
                    //strQuery.Append("   from consol_invoice_tbl     INV," & vbCrLf)
                    //strQuery.Append("   consol_invoice_trn_tbl INVTRN" & vbCrLf)
                    //strQuery.Append("   where INV.consol_invoice_pk =" & vbCrLf)
                    //strQuery.Append("   INVTRN.consol_invoice_fk" & vbCrLf)
                    //strQuery.Append("   and INVTRN.JOB_CARD_FK(+) =" & vbCrLf)
                    //strQuery.Append("   JHDR.JOB_CARD_AIR_IMP_PK) ""CONSOLPK""" & vbCrLf)
                    //strQuery.Append("                  FROM JOB_CARD_Air_IMP_TBL JHDR," & vbCrLf)
                    //add by latha for fetching location wise
                    //strQuery.Append("                       USER_MST_TBL     UMT," & vbCrLf)
                    //strQuery.Append("                       CUSTOMER_MST_TBL     CMT," & vbCrLf)
                    //strQuery.Append("                       PORT_MST_TBL         POL," & vbCrLf)
                    //strQuery.Append("                       PORT_MST_TBL         POD," & vbCrLf)
                    //
                    //strQuery.Append("                       QUOTATION_Air_TBL    QHDR," & vbCrLf)
                    //strQuery.Append("                       AIRLINE_MST_TBL      AIR," & vbCrLf)
                    //strQuery.Append("                        COMMODITY_GROUP_MST_TBL    COMM" & vbCrLf)

                    //strQuery.Append("                 WHERE JHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)" & vbCrLf)
                    //strQuery.Append("                   AND JHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)" & vbCrLf)
                    //strQuery.Append("                   AND JHDR.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)" & vbCrLf)
                    //strQuery.Append("                   AND COMM.COMMODITY_GROUP_PK(+) = JHDR.COMMODITY_GROUP_FK" & vbCrLf)
                    //strQuery.Append("   and (jhdr.PORT_MST_POD_FK " & vbCrLf)
                    //strQuery.Append("IN (SELECT T.PORT_MST_FK FROM LOC_PORT_MAPPING_TRN T WHERE T.LOCATION_MST_FK= " & LocFk & ")  and jhdr.JC_AUTO_MANUAL = 1)" & vbCrLf)
                    //strQuery.Append(addConditions)

                    //strQuery.Append(" ORDER BY TO_DATE(""Job Card"", '" & dateFormat & "') DESC,""Document Nr."" DESC    )")
                    //strQuery.Append("   ) Q) MAIN")
                    // strQuery.Append("   WHERE ""Sl.Nr.""    BETWEEN " & startPage & " AND " & endPage & vbCrLf)
                }

                return strQuery.ToString();
                //'Manjunath  PTS ID:Sep-02  26/09/2011
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

        /// <summary>
        /// Gets the total record count.
        /// </summary>
        /// <param name="bussinessType">Type of the bussiness.</param>
        /// <param name="strConditions">The string conditions.</param>
        /// <returns></returns>
        public int GetTotalRecordCount(string bussinessType, string strConditions)
        {
            StringBuilder strSqlRecordCound = new StringBuilder();

            Int32 totRecords = 0;

            WorkFlow objTotRecCount = new WorkFlow();
            try
            {
                if (bussinessType == "SE")
                {
                    strSqlRecordCound.Append("select count(*)");
                    strSqlRecordCound.Append("  from (SELECT DISTINCT JHDR.JOBCARD_REF_NO ,");
                    strSqlRecordCound.Append("                        CMT.CUSTOMER_ID ,");
                    strSqlRecordCound.Append("                        CMT.CUSTOMER_NAME CUSTOMERNAME,");
                    strSqlRecordCound.Append("                        POL.PORT_ID POL,");
                    strSqlRecordCound.Append("                        POD.PORT_ID POD,");
                    strSqlRecordCound.Append("                        TO_CHAR(EHDR.ENQUIRY_DATE, '" + dateFormat + "'),");
                    strSqlRecordCound.Append("                        TO_CHAR(QHDR.QUOTATION_DATE, '" + dateFormat + "') ,");
                    strSqlRecordCound.Append("                        TO_CHAR(BHDR.BOOKING_DATE, '" + dateFormat + "') ,");
                    strSqlRecordCound.Append("                        TO_CHAR(JHDR.JOBCARD_DATE, '" + dateFormat + "') ,");
                    strSqlRecordCound.Append("                        TO_CHAR(JHDR.DEPARTURE_DATE, '" + dateFormat + "') ATD,");
                    strSqlRecordCound.Append("                        TO_CHAR(HBL.HBL_DATE, '" + dateFormat + "') HBL,");
                    strSqlRecordCound.Append("                        TO_CHAR(MBL.MBL_DATE, '" + dateFormat + "') MBL,");
                    strSqlRecordCound.Append("                        (SELECT MAX(INV.INVOICE_DATE)");
                    strSqlRecordCound.Append("                           FROM INV_CUST_SEA_EXP_TBL INV");
                    strSqlRecordCound.Append("                          WHERE INV.JOB_CARD_TRN_FK(+) =");
                    strSqlRecordCound.Append("                                JHDR.JOB_CARD_TRN_PK) ,");
                    strSqlRecordCound.Append("                        COMM.COMMODITY_GROUP_CODE ");
                    strSqlRecordCound.Append("          FROM MBL_EXP_TBL               MBL,");
                    strSqlRecordCound.Append("               HBL_EXP_TBL               HBL,");
                    strSqlRecordCound.Append("               JOB_CARD_TRN      JHDR,");
                    //add by latha for fetching location wise
                    strSqlRecordCound.Append("                       USER_MST_TBL     UMT,");

                    strSqlRecordCound.Append("               BOOKING_MST_TBL           BHDR,");
                    strSqlRecordCound.Append("               BOOKING_TRN               BTRN,");
                    strSqlRecordCound.Append("               QUOTATION_MST_TBL         QHDR,");
                    strSqlRecordCound.Append("               QUOTATION_DTL_TBL   QTRN,");
                    strSqlRecordCound.Append("               ENQUIRY_BKG_SEA_TBL       EHDR,");
                    strSqlRecordCound.Append("               CUSTOMER_MST_TBL          CMT,");
                    strSqlRecordCound.Append("               PORT_MST_TBL              POL,");
                    strSqlRecordCound.Append("               PORT_MST_TBL              POD,");
                    strSqlRecordCound.Append("               COMMODITY_GROUP_MST_TBL   COMM,");
                    strSqlRecordCound.Append("               VESSEL_VOYAGE_TBL         VVT,");
                    strSqlRecordCound.Append("               VESSEL_VOYAGE_TRN         VTT");
                    strSqlRecordCound.Append("         WHERE BHDR.BOOKING_MST_PK = JHDR.BOOKING_MST_FK");
                    strSqlRecordCound.Append("           AND JHDR.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                    strSqlRecordCound.Append("           AND JHDR.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK(+)");
                    strSqlRecordCound.Append("           AND BHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    strSqlRecordCound.Append("           AND BHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    //strSqlRecordCound.Append("           AND BHDR.AIRLINE_MST_FK = AIR.AIRLINE_MST_PK(+)" & vbCrLf)
                    strSqlRecordCound.Append("           AND BTRN.BOOKING_MST_FK(+) = BHDR.BOOKING_MST_PK");
                    strSqlRecordCound.Append("           AND BHDR.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    strSqlRecordCound.Append("           AND BTRN.TRANS_REF_NO = QHDR.QUOTATION_REF_NO(+)");
                    strSqlRecordCound.Append("           AND QTRN.QUOTATION_MST_FK(+) = QHDR.QUOTATION_MST_PK");
                    strSqlRecordCound.Append("           AND QTRN.TRANS_REF_NO = EHDR.ENQUIRY_REF_NO(+)");
                    strSqlRecordCound.Append("           AND COMM.COMMODITY_GROUP_PK(+) = JHDR.COMMODITY_GROUP_FK   ");
                    strSqlRecordCound.Append("           AND JHDR.VOYAGE_TRN_FK = VTT.VOYAGE_TRN_PK");
                    strSqlRecordCound.Append("           AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VTT.VESSEL_VOYAGE_TBL_FK");
                    strSqlRecordCound.Append(strConditions);
                    strSqlRecordCound.Append("                 ORDER BY CMT.CUSTOMER_ID)");

                    totRecords = Convert.ToInt32(objTotRecCount.ExecuteScaler(strSqlRecordCound.ToString()));
                }
                else if (bussinessType == "SI")
                {
                    strSqlRecordCound.Append("select count(*)");
                    strSqlRecordCound.Append("  from (SELECT DISTINCT JHDR.JOBCARD_REF_NO,");
                    strSqlRecordCound.Append("                        CMT.CUSTOMER_ID,");
                    strSqlRecordCound.Append("                        CMT.CUSTOMER_NAME CUSTOMERNAME,");
                    strSqlRecordCound.Append("                        POL.PORT_ID POL,");
                    strSqlRecordCound.Append("                        POD.PORT_ID POD,");
                    strSqlRecordCound.Append("                        ");
                    strSqlRecordCound.Append("                        (SELECT MAX(INV.INVOICE_DATE)");
                    strSqlRecordCound.Append("                           FROM INV_CUST_SEA_IMP_TBL INV");
                    strSqlRecordCound.Append("                          WHERE INV.JOB_CARD_TRN_FK(+) =");
                    strSqlRecordCound.Append("                                JHDR.JOB_CARD_TRN_PK) ,");
                    strSqlRecordCound.Append("                        COMM.COMMODITY_GROUP_CODE");
                    strSqlRecordCound.Append("          FROM JOB_CARD_TRN    JHDR,");
                    //add by latha for fetching location wise
                    strSqlRecordCound.Append("                       USER_MST_TBL     UMT,");
                    strSqlRecordCound.Append("               CUSTOMER_MST_TBL        CMT,");
                    strSqlRecordCound.Append("               PORT_MST_TBL            POL,");
                    strSqlRecordCound.Append("               PORT_MST_TBL            POD,");
                    strSqlRecordCound.Append("               COMMODITY_GROUP_MST_TBL COMM,");
                    strSqlRecordCound.Append("               VESSEL_VOYAGE_TBL         VVT,");
                    strSqlRecordCound.Append("               VESSEL_VOYAGE_TRN         VTT");
                    strSqlRecordCound.Append("         WHERE JHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    strSqlRecordCound.Append("           AND JHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    strSqlRecordCound.Append("           AND JHDR.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    strSqlRecordCound.Append("           AND COMM.COMMODITY_GROUP_PK(+) = JHDR.COMMODITY_GROUP_FK  ");
                    strSqlRecordCound.Append("           AND JHDR.VOYAGE_TRN_FK = VTT.VOYAGE_TRN_PK(+)");
                    strSqlRecordCound.Append("           AND VVT.VESSEL_VOYAGE_TBL_PK(+) = VTT.VESSEL_VOYAGE_TBL_FK");
                    strSqlRecordCound.Append(strConditions);
                    strSqlRecordCound.Append("         ORDER BY CMT.CUSTOMER_ID)");

                    totRecords = Convert.ToInt32(objTotRecCount.ExecuteScaler(strSqlRecordCound.ToString()));
                }
                else if (bussinessType == "AE")
                {
                    strSqlRecordCound.Append("SELECT COUNT(*)");
                    strSqlRecordCound.Append("  FROM (SELECT DISTINCT JHDR.JOBCARD_REF_NO ,");
                    strSqlRecordCound.Append("                        CMT.CUSTOMER_ID ,");
                    strSqlRecordCound.Append("                        CMT.CUSTOMER_NAME CUSTOMERNAME,");
                    strSqlRecordCound.Append("                        POL.PORT_ID AOO,");
                    strSqlRecordCound.Append("                        POD.PORT_ID AOD,");
                    strSqlRecordCound.Append("                       ");
                    strSqlRecordCound.Append("                        (SELECT MAX(INV.INVOICE_DATE)");
                    strSqlRecordCound.Append("                           FROM INV_CUST_AIR_EXP_TBL INV");
                    strSqlRecordCound.Append("                          WHERE INV.JOB_CARD_FK(+) =");
                    strSqlRecordCound.Append("                                JHDR.JOB_CARD_TRN_PK) ,");
                    strSqlRecordCound.Append("                        COMM.COMMODITY_GROUP_CODE");
                    strSqlRecordCound.Append("          FROM Mawb_EXP_TBL            MAWB,");
                    strSqlRecordCound.Append("               hawb_EXP_TBL            HAWB,");
                    strSqlRecordCound.Append("               JOB_CARD_TRN   JHDR,");
                    //add by latha for fetching location wise
                    strSqlRecordCound.Append("                       USER_MST_TBL     UMT,");
                    strSqlRecordCound.Append("               BOOKING_MST_TBL         BHDR,");
                    strSqlRecordCound.Append("               BOOKING_TRN             BTRN,");
                    strSqlRecordCound.Append("               QUOTATION_MST_TBL       QHDR,");
                    strSqlRecordCound.Append("               QUOTATION_DTL_TBL       QTRN,");
                    strSqlRecordCound.Append("               ENQUIRY_BKG_Air_TBL     EHDR,");
                    strSqlRecordCound.Append("               CUSTOMER_MST_TBL        CMT,");
                    strSqlRecordCound.Append("               PORT_MST_TBL            POL,");
                    strSqlRecordCound.Append("               PORT_MST_TBL            POD,");
                    strSqlRecordCound.Append("               AIRLINE_MST_TBL         AIR,");
                    strSqlRecordCound.Append("               COMMODITY_GROUP_MST_TBL COMM");
                    strSqlRecordCound.Append("         WHERE BHDR.BOOKING_MST_PK = JHDR.BOOKING_MST_FK");
                    strSqlRecordCound.Append("           AND JHDR.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                    strSqlRecordCound.Append("           AND JHDR.MBL_MAWB_FK = MAWB.MAWB_EXP_TBL_PK(+)");
                    strSqlRecordCound.Append("           AND BHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    strSqlRecordCound.Append("           AND BHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    //strSqlRecordCound.Append("           AND BHDR.AIRLINE_MST_FK = AIR.AIRLINE_MST_PK(+)" & vbCrLf)
                    strSqlRecordCound.Append("           AND BTRN.BOOKING_MST_FK(+) = BHDR.BOOKING_MST_PK");
                    strSqlRecordCound.Append("           AND BHDR.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    strSqlRecordCound.Append("           AND BTRN.TRANS_REF_NO = QHDR.QUOTATION_REF_NO(+)");
                    strSqlRecordCound.Append("           AND QTRN.QUOTATION_MST_FK(+) = QHDR.QUOTATION_MST_PK");
                    strSqlRecordCound.Append("           AND QTRN.TRANS_REF_NO = EHDR.ENQUIRY_REF_NO(+)");
                    strSqlRecordCound.Append("           AND COMM.COMMODITY_GROUP_PK(+) = JHDR.COMMODITY_GROUP_FK   ");
                    strSqlRecordCound.Append(strConditions);
                    strSqlRecordCound.Append("         ORDER BY CMT.CUSTOMER_ID)");

                    totRecords = Convert.ToInt32(objTotRecCount.ExecuteScaler(strSqlRecordCound.ToString()));
                }
                else if (bussinessType == "AI")
                {
                    strSqlRecordCound.Append("select count(*) from (");
                    strSqlRecordCound.Append("SELECT DISTINCT JHDR.JOBCARD_REF_NO ,");
                    strSqlRecordCound.Append("                CMT.CUSTOMER_ID ,");
                    strSqlRecordCound.Append("                CMT.CUSTOMER_NAME CUSTOMERNAME,");
                    strSqlRecordCound.Append("                POL.PORT_ID AOO,");
                    strSqlRecordCound.Append("                POD.PORT_ID AOD,");
                    strSqlRecordCound.Append("                            ");
                    strSqlRecordCound.Append("                null ATD,");
                    strSqlRecordCound.Append("                null HBL,");
                    strSqlRecordCound.Append("                null MBL,");
                    strSqlRecordCound.Append("                (SELECT MAX(INV.INVOICE_DATE)");
                    strSqlRecordCound.Append("                   FROM INV_CUST_Air_IMP_TBL INV");
                    strSqlRecordCound.Append("                  WHERE INV.JOB_CARD_AIR_IMP_FK(+) = JHDR.JOB_CARD_TRN_PK),");
                    strSqlRecordCound.Append("                COMM.COMMODITY_GROUP_CODE ");
                    strSqlRecordCound.Append("  FROM JOB_CARD_TRN    JHDR,");
                    //add by latha for fetching location wise
                    strSqlRecordCound.Append("                       USER_MST_TBL     UMT,");
                    strSqlRecordCound.Append("       CUSTOMER_MST_TBL        CMT,");
                    strSqlRecordCound.Append("       PORT_MST_TBL            POL,");
                    strSqlRecordCound.Append("       PORT_MST_TBL            POD,");
                    strSqlRecordCound.Append("       AIRLINE_MST_TBL         AIR,");
                    strSqlRecordCound.Append("       COMMODITY_GROUP_MST_TBL COMM");
                    strSqlRecordCound.Append(" WHERE JHDR.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    strSqlRecordCound.Append("   AND JHDR.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    strSqlRecordCound.Append("   AND JHDR.CONSIGNEE_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    strSqlRecordCound.Append("   AND COMM.COMMODITY_GROUP_PK(+) = JHDR.COMMODITY_GROUP_FK   ");
                    strSqlRecordCound.Append(strConditions);
                    strSqlRecordCound.Append(" ORDER BY CMT.CUSTOMER_ID )");

                    totRecords = Convert.ToInt32(objTotRecCount.ExecuteScaler(strSqlRecordCound.ToString()));
                }

                return totRecords;
                //'Manjunath  PTS ID:Sep-02  26/09/2011
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

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="BussinessType">Type of the bussiness.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="LocFK">The loc fk.</param>
        /// <param name="Process">The process.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="Group">The group.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="POL">The pol.</param>
        /// <param name="POD">The pod.</param>
        /// <param name="ShippingLine">The shipping line.</param>
        /// <param name="VslVoy">The VSL voy.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="Sector">The sector.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="RefNumber">The reference number.</param>
        /// <param name="ShipStatus">The ship status.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchAll(string BussinessType, Int16 CargoType, string LocFK, string Process = "", string FromDt = "", string ToDt = "", string Group = "", string CustomerID = "", string POL = "", string POD = "",
        string ShippingLine = "", string VslVoy = "", Int32 TotalPage = 0, Int32 CurrentPage = 0, string Sector = "", Int32 flag = 0, string RefType = "", string SearchType = "", string RefNumber = "", string ShipStatus = "",
        Int16 BizType = 0)
        {
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            StringBuilder addStr = new StringBuilder();
            StringBuilder addStr1 = new StringBuilder();
            //add by latha for fetching location wise
            //Dim intLoc As Int32
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            string SrOP = (SearchType == "C" ? "%" : "");
            //intLoc = CType(objPage.HttpContext.Current.Session("LOGED_IN_LOC_FK"), Int32)'comented by purnanand for PTS MIS-APR-001

            //addStr.Append(vbCrLf & " AND UMT.DEFAULT_LOCATION_FK = " & intLoc) 'End purnanand
            if (BussinessType == "AE" | BussinessType == "SE")
            {
                addStr.Append(" AND JHDR.CREATED_BY_FK = UMT.USER_MST_PK ");
            }
            else
            {
                addStr.Append(" AND JC.CREATED_BY_FK = UMT.USER_MST_PK ");
            }

            if (!string.IsNullOrEmpty(CustomerID) & CustomerID != "0")
            {
                if (BussinessType == "AE" | BussinessType == "SE")
                {
                    addStr.Append(" AND  CMT.Customer_Mst_Pk in (" + CustomerID + ")");
                }
                else
                {
                    addStr.Append(" AND  CO.Customer_Mst_Pk in (" + CustomerID + ")");
                }
            }
            //Snigdarani - 31/12/2009 - When all is selected, records should come only for logged in location and reporting location.
            //If LocFK <> 0 Then  'Added by purnanand for PTS MIS-APR-001
            //    addStr.Append("AND UMT.DEFAULT_LOCATION_FK = " & LocFK)
            //End If
            addStr.Append("AND UMT.DEFAULT_LOCATION_FK IN (" + LocFK + ")");
            if (BussinessType == "SE")
            {
                if (CargoType != 0)
                {
                    addStr.Append(" AND BHDR.CARGO_TYPE='" + CargoType + "'");
                }
            }
            else if (BussinessType == "SI")
            {
                if (CargoType != 0)
                {
                    addStr.Append(" AND JC.CARGO_TYPE='" + CargoType + "'");
                }
            }
            //End purnanand

            //Modified By Snigdharani for "AE", "AI", "SI"
            if (!string.IsNullOrEmpty(ShippingLine))
            {
                if (BussinessType == "AE")
                {
                    addStr.Append(" AND   BHDR.CARRIER_MST_FK=" + ShippingLine);
                }
                else if (BussinessType == "AI")
                {
                    addStr.Append(" AND JC.CARRIER_MST_FK=" + ShippingLine);
                }
                else if (BussinessType == "SE")
                {
                    addStr.Append(" AND  BHDR.CARRIER_MST_FK=" + ShippingLine);
                }
                else if (BussinessType == "SI")
                {
                    addStr.Append(" AND JC.CARRIER_MST_FK=" + ShippingLine);
                }
            }
            //End Snigdharani
            if (!string.IsNullOrEmpty(VslVoy))
            {
                if (BussinessType == "SE" | BussinessType == "SI")
                {
                    addStr.Append(" AND  (VVT.VESSEL_ID) in ('" + VslVoy + "')");
                }
                else if (BussinessType == "AE")
                {
                    addStr.Append(" AND UPPER(JHDR.VOYAGE_FLIGHT_NO) in ('" + VslVoy.ToUpper() + "')");
                }
                else
                {
                    addStr.Append(" AND UPPER(JC.VOYAGE_FLIGHT_NO) in ('" + VslVoy.ToUpper() + "')");
                }
            }
            //'
            if (!string.IsNullOrEmpty(RefNumber))
            {
                if (Convert.ToInt32(Process) == 1)
                {
                    if (Convert.ToInt32(RefType) == 1)
                    {
                        addStr.Append(" AND ((UPPER(POL.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(POD.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(EHDR.ENQUIRY_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(QHDR.QUOTATION_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(BHDR.BOOKING_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(JHDR.JOBCARD_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        if (BussinessType == "SE")
                        {
                            addStr.Append(" OR (UPPER(HBL.HBL_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                            addStr.Append(" OR (UPPER(MBL.MBL_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        }
                        else
                        {
                            addStr.Append(" OR (UPPER(HAWB.HAWB_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                            addStr.Append(" OR (UPPER(MAWB.MAWB_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        }
                        addStr.Append(" OR (UPPER(CIT.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(CT.COLLECTIONS_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(IST.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(PT.PAYMENT_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(IASET.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'))");
                    }
                    else if (Convert.ToInt32(RefType) == 2)
                    {
                        addStr.Append(" AND UPPER(POL.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 3)
                    {
                        addStr.Append(" AND UPPER(POD.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 4)
                    {
                        addStr.Append(" AND UPPER(EHDR.ENQUIRY_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 5)
                    {
                        addStr.Append(" AND UPPER(QHDR.QUOTATION_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 6)
                    {
                        addStr.Append(" AND UPPER(BHDR.BOOKING_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 7)
                    {
                        addStr.Append(" AND UPPER(JHDR.JOBCARD_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 8)
                    {
                        if (BussinessType == "SE")
                        {
                            addStr.Append(" AND UPPER(HBL.HBL_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                        else
                        {
                            addStr.Append(" AND UPPER(HAWB.HAWB_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                    }
                    else if (Convert.ToInt32(RefType) == 9)
                    {
                        if (BussinessType == "SE")
                        {
                            addStr.Append(" AND UPPER(MBL.MBL_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                        else
                        {
                            addStr.Append(" AND UPPER(MAWB.MAWB_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                    }
                    else if (Convert.ToInt32(RefType) == 10)
                    {
                        addStr.Append(" AND ((UPPER(CIT.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(IASET.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'))");
                    }
                    else if (Convert.ToInt32(RefType) == 11)
                    {
                        addStr.Append(" AND UPPER(CT.COLLECTIONS_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 12)
                    {
                        addStr.Append(" AND UPPER(IST.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 13)
                    {
                        addStr.Append(" AND UPPER(PT.PAYMENT_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                }
                else
                {
                    if (Convert.ToInt32(RefType) == 1)
                    {
                        addStr.Append(" AND ((UPPER(POL.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(POD.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(JC.JOBCARD_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        if (BussinessType == "SI")
                        {
                            addStr.Append(" OR (UPPER(JC.HBL_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                            addStr.Append(" OR (UPPER(JC.MBL_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        }
                        else
                        {
                            addStr.Append(" OR (UPPER(JC.HAWB_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                            addStr.Append(" OR (UPPER(JC.MAWB_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        }
                        addStr.Append(" OR (UPPER(CAN.CAN_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(DMT.DELIVERY_ORDER_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(CIT.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(CT.COLLECTIONS_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(IST.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(PT.PAYMENT_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(IASET.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'))");
                    }
                    else if (Convert.ToInt32(RefType) == 2)
                    {
                        addStr.Append(" AND UPPER(POL.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 3)
                    {
                        addStr.Append(" AND UPPER(POD.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 4)
                    {
                        addStr.Append(" AND UPPER(JC.JOBCARD_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 5)
                    {
                        if (BussinessType == "SI")
                        {
                            addStr.Append(" AND UPPER(JC.HBL_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                        else
                        {
                            addStr.Append(" AND UPPER(JC.HAWB_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                    }
                    else if (Convert.ToInt32(RefType) == 6)
                    {
                        if (BussinessType == "SI")
                        {
                            addStr.Append(" AND UPPER(JC.MBL_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                        else
                        {
                            addStr.Append(" AND UPPER(JC.MAWB_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                    }
                    else if (Convert.ToInt32(RefType) == 7)
                    {
                        addStr.Append(" AND UPPER(CAN.CAN_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 8)
                    {
                        addStr.Append(" AND UPPER(DMT.DELIVERY_ORDER_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 9)
                    {
                        addStr.Append(" AND ((UPPER(CIT.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStr.Append(" OR (UPPER(IASET.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'))");
                    }
                    else if (Convert.ToInt32(RefType) == 10)
                    {
                        addStr.Append(" AND UPPER(CT.COLLECTIONS_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 11)
                    {
                        addStr.Append(" AND UPPER(IST.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 12)
                    {
                        addStr.Append(" AND UPPER(PT.PAYMENT_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                }
            }
            //'
            //Vasava
            if (ShipStatus == "HBL")
            {
                addStr.Append(" AND HBL.HBL_DATE IS NULL ");
            }
            else if (ShipStatus == "MBL")
            {
                addStr.Append(" AND MBL.MBL_DATE IS NULL ");
            }
            else if (ShipStatus == "Invoice Generated")
            {
                addStr.Append(" AND CIT.INVOICE_DATE IS NULL ");
            }
            else if (ShipStatus == "Collection")
            {
                addStr.Append(" AND CT.COLLECTIONS_DATE IS NULL ");
            }
            else if (ShipStatus == "Payment")
            {
                addStr.Append(" AND PT.PAYMENT_DATE IS  NULL ");
            }
            else if (ShipStatus == "Voucher")
            {
                addStr.Append(" AND IST.INVOICE_DATE IS NULL ");
            }
            else if (ShipStatus == "CRO Issued")
            {
                addStr.Append(" AND BCR.CRO_DATE IS NULL ");
            }
            else if (ShipStatus == "Delivery Order")
            {
                addStr.Append(" AND DMT.DELIVERY_ORDER_DATE IS NULL ");
            }
            else if (ShipStatus == "Cargo Arrival Notice")
            {
                addStr.Append(" AND CAN.CAN_DATE IS NULL  ");
            }
            else if (ShipStatus == "HAWB")
            {
                addStr.Append("  AND HAWB.HAWB_DATE IS NULL ");
            }
            else if (ShipStatus == "MAWB")
            {
                addStr.Append("  AND MAWB.MAWB_DATE IS NULL ");
            }
            if (Convert.ToInt32(Process) == 2)
            {
                if (ShipStatus == "Surrender BL/AWB")
                {
                    if (BussinessType == "SI")
                    {
                        addStr.Append("  AND JC.MBLSURRDT IS NULL ");
                    }
                    else
                    {
                        addStr.Append("  AND JC.MAWB_SURRDT IS NULL ");
                    }
                }
                else if (ShipStatus == "Customs Clearance")
                {
                    addStr.Append("  AND JC.CRQ_DATE IS NULL ");
                }
                else if (ShipStatus == "BRO Received")
                {
                    addStr.Append("  AND JC.BRO_DATE IS NULL ");
                }
            }
            //'End
            if (!string.IsNullOrEmpty(Sector))
            {
                addStr.Append(Sector);
            }
            if (flag == 0)
            {
                addStr1.Append(" AND 1=2 ");
            }
            if (Group != "0")
            {
                if (BussinessType == "AE" | BussinessType == "SE")
                {
                    addStr.Append(" AND JHDR.COMMODITY_GROUP_FK = " + Group);
                }
                else
                {
                    addStr.Append(" AND JC.COMMODITY_GROUP_FK = " + Group);
                }
            }

            if (!string.IsNullOrEmpty(FromDt) & !string.IsNullOrEmpty(ToDt))
            {
                if (BussinessType == "AE" | BussinessType == "SE")
                {
                    addStr.Append("AND to_char(JHDR.JOBCARD_DATE) BETWEEN TO_DATE('" + FromDt + "',DATEFORMAT) and to_date('" + ToDt + "',DATEFORMAT)");
                }
                else
                {
                    addStr.Append("AND to_char(JC.JOBCARD_DATE) BETWEEN TO_DATE('" + FromDt + "',DATEFORMAT) and to_date('" + ToDt + "',DATEFORMAT)");
                }
            }

            //TotalRecords = GetTotalRecordCount(BussinessType, addStr.ToString) 'GetTotalRecordCount(addStr.ToString)

            //TotalPage = TotalRecords \ RecordsPerPage

            //If TotalRecords Mod RecordsPerPage <> 0 Then
            //    TotalPage += 1
            //End If

            //If CurrentPage > TotalPage Then
            //    CurrentPage = 1
            //End If

            //If TotalRecords = 0 Then
            //    CurrentPage = 0
            //End If

            //last = CurrentPage * RecordsPerPage
            //start = (CurrentPage - 1) * RecordsPerPage + 1

            DataSet DS = null;
            WorkFlow objWF = new WorkFlow();

            DS = objWF.GetDataSet(MainQuery(BussinessType, LocFK, addStr.ToString(), addStr1.ToString(), start, last, ShipStatus, BizType));

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

        #region "Constructor"

        //Added by Rabbani facilitate to select ALL commodity on 5/12/06
        /// <summary>
        /// Initializes a new instance of the <see cref="clsWorkFlow"/> class.
        /// </summary>
        /// <param name="SelectAll">if set to <c>true</c> [select all].</param>
        public clsWorkFlow(bool SelectAll = false)
        {
            string Sql = null;
            //commented by latha
            //Dim strSelect As String = "<All>"
            //If SelectAll Then
            //    strSelect = "<ALL>"
            //End If
            //Sql = "SELECT 0 COMMODITY_GROUP_PK,'" & strSelect & "' COMMODITY_GROUP_CODE,'0' COMMODITY_GROUP_DESC,0 VERSION_NO "
            //Sql &= vbCrLf & " FROM DUAL "
            //Sql &= vbCrLf & " UNION all "
            Sql += " SELECT CG.COMMODITY_GROUP_PK,CG.COMMODITY_GROUP_CODE, ";
            Sql += " CG.COMMODITY_GROUP_DESC,CG.VERSION_NO ";
            Sql += " FROM COMMODITY_GROUP_MST_TBL CG ";
            Sql += " WHERE CG.ACTIVE_FLAG=1 ";
            Sql += " ORDER BY COMMODITY_GROUP_CODE ";
            try
            {
                M_DataSet = (new WorkFlow()).GetDataSet(Sql);
                //'Manjunath  PTS ID:Sep-02  26/09/2011
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

        //End on 5/12/06

        #endregion "Constructor"

        #region "For Fetching DropDown Values From DataBase"

        /// <summary>
        /// Fetches the drop down value.
        /// </summary>
        /// <param name="Flag">The flag.</param>
        /// <param name="ConfigID">The configuration identifier.</param>
        /// <returns></returns>
        public DataSet FetchDropDownValue(string Flag = "", string ConfigID = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string ErrorMessage = null;
            sb.Append("SELECT T.DESCRIPTION, T.DD_ID");
            sb.Append("  FROM QFOR_DROP_DOWN_TBL T");
            sb.Append(" WHERE T.DD_FLAG = '" + Flag + "'");
            sb.Append(" AND T.CONFIG_ID  = '" + ConfigID + "'");
            sb.Append("    ORDER BY T.DROPDOWN_PK");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraexp)
            {
                ErrorMessage = oraexp.Message;
                throw oraexp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "For Fetching DropDown Values From DataBase"

        #region "Fetch for BizType BOTH"

        /// <summary>
        /// Fetches the both.
        /// </summary>
        /// <param name="BussinessType">Type of the bussiness.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="LocFK">The loc fk.</param>
        /// <param name="Process">The process.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="Group">The group.</param>
        /// <param name="CustomerID">The customer identifier.</param>
        /// <param name="POL">The pol.</param>
        /// <param name="POD">The pod.</param>
        /// <param name="ShippingLine">The shipping line.</param>
        /// <param name="VslVoy">The VSL voy.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="Sector">The sector.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="RefType">Type of the reference.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="RefNumber">The reference number.</param>
        /// <param name="ShipStatus">The ship status.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="FlightNr">The flight nr.</param>
        /// <returns></returns>
        public DataSet FetchBoth(string BussinessType, Int16 CargoType, string LocFK, string Process = "", string FromDt = "", string ToDt = "", string Group = "", string CustomerID = "", string POL = "", string POD = "",
        string ShippingLine = "", string VslVoy = "", Int32 TotalPage = 0, Int32 CurrentPage = 0, string Sector = "", Int32 flag = 0, string RefType = "", string SearchType = "", string RefNumber = "", string ShipStatus = "",
        Int16 BizType = 0, string FlightNr = "")
        {
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strMainQuery = null;

            StringBuilder addStrAir = new StringBuilder();
            StringBuilder addStrAir1 = new StringBuilder();

            StringBuilder addStrSea = new StringBuilder();
            StringBuilder addStrSea1 = new StringBuilder();

            System.Web.UI.Page objPage = new System.Web.UI.Page();
            string SrOP = (SearchType == "C" ? "%" : "");

            //Air
            if (BussinessType == "EX")
            {
                addStrAir.Append(" AND JHDR.CREATED_BY_FK = UMT.USER_MST_PK ");
            }
            else
            {
                addStrAir.Append(" AND JC.CREATED_BY_FK = UMT.USER_MST_PK ");
            }

            if (!string.IsNullOrEmpty(CustomerID) & CustomerID != "0")
            {
                if (BussinessType == "EX")
                {
                    addStrAir.Append(" AND  CMT.Customer_Mst_Pk in (" + CustomerID + ")");
                }
                else
                {
                    addStrAir.Append(" AND  CO.Customer_Mst_Pk in (" + CustomerID + ")");
                }
            }

            addStrAir.Append("AND UMT.DEFAULT_LOCATION_FK IN (" + LocFK + ")");

            if (!string.IsNullOrEmpty(ShippingLine))
            {
                if (BussinessType == "EX")
                {
                    addStrAir.Append(" AND   BHDR.Carrier_Mst_Fk=" + ShippingLine);
                }
                else if (BussinessType == "IM")
                {
                    addStrAir.Append(" AND JC.AIRLINE_MST_FK=" + ShippingLine);
                }
            }

            if (!string.IsNullOrEmpty(FlightNr))
            {
                if (BussinessType == "EX")
                {
                    addStrAir.Append(" AND UPPER(JHDR.VOYAGE_FLIGHT_NO) in ('" + FlightNr.ToUpper() + "')");
                }
                else
                {
                    addStrAir.Append(" AND UPPER(JC.VOYAGE_FLIGHT_NO) in ('" + FlightNr.ToUpper() + "')");
                }
            }

            if (!string.IsNullOrEmpty(RefNumber))
            {
                if (Convert.ToInt32(Process) == 1)
                {
                    if (Convert.ToInt32(RefType) == 1)
                    {
                        addStrAir.Append(" AND ((UPPER(POL.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(POD.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(EHDR.ENQUIRY_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(QHDR.QUOTATION_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(BHDR.BOOKING_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(JHDR.JOBCARD_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        if (BussinessType == "EX")
                        {
                            addStrAir.Append(" OR (UPPER(HAWB.HAWB_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                            addStrAir.Append(" OR (UPPER(MAWB.MAWB_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        }
                        addStrAir.Append(" OR (UPPER(CIT.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(CT.COLLECTIONS_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(IST.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(PT.PAYMENT_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(IASET.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'))");
                    }
                    else if (Convert.ToInt32(RefType) == 2)
                    {
                        addStrAir.Append(" AND UPPER(POL.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 3)
                    {
                        addStrAir.Append(" AND UPPER(POD.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 4)
                    {
                        addStrAir.Append(" AND UPPER(EHDR.ENQUIRY_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 5)
                    {
                        addStrAir.Append(" AND UPPER(QHDR.QUOTATION_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 6)
                    {
                        addStrAir.Append(" AND UPPER(BHDR.BOOKING_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 7)
                    {
                        addStrAir.Append(" AND UPPER(JHDR.JOBCARD_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 8)
                    {
                        if (BussinessType == "EX")
                        {
                            addStrAir.Append(" AND UPPER(HAWB.HAWB_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                    }
                    else if (Convert.ToInt32(RefType) == 9)
                    {
                        if (BussinessType == "EX")
                        {
                            addStrAir.Append(" AND UPPER(MAWB.MAWB_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                    }
                    else if (Convert.ToInt32(RefType) == 10)
                    {
                        addStrAir.Append(" AND ((UPPER(CIT.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(IASET.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'))");
                    }
                    else if (Convert.ToInt32(RefType) == 11)
                    {
                        addStrAir.Append(" AND UPPER(CT.COLLECTIONS_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 12)
                    {
                        addStrAir.Append(" AND UPPER(IST.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 13)
                    {
                        addStrAir.Append(" AND UPPER(PT.PAYMENT_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                }
                else
                {
                    if (Convert.ToInt32(RefType) == 1)
                    {
                        addStrAir.Append(" AND ((UPPER(POL.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(POD.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(JC.JOBCARD_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        if (BussinessType == "IM")
                        {
                            addStrAir.Append(" OR (UPPER(JC.Hbl_Hawb_Ref_No) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                            addStrAir.Append(" OR (UPPER(JC.Mbl_Mawb_Ref_No) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        }
                        addStrAir.Append(" OR (UPPER(CAN.CAN_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(DMT.DELIVERY_ORDER_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(CIT.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(CT.COLLECTIONS_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(IST.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(PT.PAYMENT_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(IASET.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'))");
                    }
                    else if (Convert.ToInt32(RefType) == 2)
                    {
                        addStrAir.Append(" AND UPPER(POL.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 3)
                    {
                        addStrAir.Append(" AND UPPER(POD.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 4)
                    {
                        addStrAir.Append(" AND UPPER(JC.JOBCARD_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 5)
                    {
                        if (BussinessType == "IM")
                        {
                            addStrAir.Append(" AND UPPER(JC.HAWB_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                    }
                    else if (Convert.ToInt32(RefType) == 6)
                    {
                        if (BussinessType == "IM")
                        {
                            addStrAir.Append(" AND UPPER(JC.Mbl_Mawb_Ref_No) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                    }
                    else if (Convert.ToInt32(RefType) == 7)
                    {
                        addStrAir.Append(" AND UPPER(CAN.CAN_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 8)
                    {
                        addStrAir.Append(" AND UPPER(DMT.DELIVERY_ORDER_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 9)
                    {
                        addStrAir.Append(" AND ((UPPER(CIT.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrAir.Append(" OR (UPPER(IASET.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'))");
                    }
                    else if (Convert.ToInt32(RefType) == 10)
                    {
                        addStrAir.Append(" AND UPPER(CT.COLLECTIONS_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 11)
                    {
                        addStrAir.Append(" AND UPPER(IST.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 12)
                    {
                        addStrAir.Append(" AND UPPER(PT.PAYMENT_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                }
            }

            if (ShipStatus == "HBL/HAWB")
            {
                addStrAir.Append("  AND HAWB.HAWB_DATE IS NULL ");
            }
            else if (ShipStatus == "MBL/MAWB")
            {
                addStrAir.Append("  AND MAWB.MAWB_DATE IS NULL ");
            }
            else if (ShipStatus == "Invoice Generated")
            {
                addStrAir.Append(" AND CIT.INVOICE_DATE IS NULL ");
            }
            else if (ShipStatus == "Collection")
            {
                addStrAir.Append(" AND CT.COLLECTIONS_DATE IS NULL ");
            }
            else if (ShipStatus == "Payment")
            {
                addStrAir.Append(" AND PT.PAYMENT_DATE IS  NULL ");
            }
            else if (ShipStatus == "Voucher")
            {
                addStrAir.Append(" AND IST.INVOICE_DATE IS NULL ");
            }
            else if (ShipStatus == "Cargo Arrival Notice")
            {
                addStrAir.Append(" AND CAN.CAN_DATE IS NULL  ");
            }
            else if (ShipStatus == "Delivery Order")
            {
                addStrAir.Append(" AND DMT.DELIVERY_ORDER_DATE IS NULL ");
                //if CRO Issued selected only sea shipment should show.
            }
            else if (ShipStatus == "CRO Issued")
            {
                addStrAir.Append(" AND 1=2 ");
            }
            if (Convert.ToInt32(Process) == 2)
            {
                if (ShipStatus == "Surrender BL/AWB")
                {
                    addStrAir.Append("  AND JC.MAWB_SURRDT IS NULL ");
                }
                else if (ShipStatus == "Customs Clearance")
                {
                    addStrAir.Append("  AND JC.CRQ_DATE IS NULL ");
                }
                else if (ShipStatus == "BRO Received")
                {
                    addStrAir.Append("  AND JC.BRO_DATE IS NULL ");
                }
            }

            if (!string.IsNullOrEmpty(Sector))
            {
                addStrAir.Append(Sector);
            }
            if (flag == 0)
            {
                addStrAir1.Append(" AND 1=2 ");
            }
            if (Group != "0")
            {
                if (BussinessType == "EX")
                {
                    addStrAir.Append(" AND JHDR.COMMODITY_GROUP_FK = " + Group);
                }
                else
                {
                    addStrAir.Append(" AND JC.COMMODITY_GROUP_FK = " + Group);
                }
            }

            if (!string.IsNullOrEmpty(FromDt) & !string.IsNullOrEmpty(ToDt))
            {
                if (BussinessType == "EX")
                {
                    addStrAir.Append("AND to_char(JHDR.JOBCARD_DATE) BETWEEN TO_DATE('" + FromDt + "',DATEFORMAT) and to_date('" + ToDt + "',DATEFORMAT)");
                }
                else
                {
                    addStrAir.Append("AND to_char(JC.JOBCARD_DATE) BETWEEN TO_DATE('" + FromDt + "',DATEFORMAT) and to_date('" + ToDt + "',DATEFORMAT)");
                }
            }

            //Sea
            if (BussinessType == "EX")
            {
                addStrSea.Append(" AND JHDR.CREATED_BY_FK = UMT.USER_MST_PK ");
            }
            else
            {
                addStrSea.Append(" AND JC.CREATED_BY_FK = UMT.USER_MST_PK ");
            }

            if (!string.IsNullOrEmpty(CustomerID) & CustomerID != "0")
            {
                if (BussinessType == "EX")
                {
                    addStrSea.Append(" AND  CMT.Customer_Mst_Pk in (" + CustomerID + ")");
                }
                else
                {
                    addStrSea.Append(" AND  CO.Customer_Mst_Pk in (" + CustomerID + ")");
                }
            }

            addStrSea.Append("AND UMT.DEFAULT_LOCATION_FK IN (" + LocFK + ")");
            if (BussinessType == "EX")
            {
                if (CargoType != 0)
                {
                    addStrSea.Append(" AND BHDR.CARGO_TYPE='" + CargoType + "'");
                }
            }

            if (!string.IsNullOrEmpty(ShippingLine))
            {
                if (BussinessType == "EX")
                {
                    addStrSea.Append(" AND  BHDR.CARRIER_MST_FK=" + ShippingLine);
                }
                else if (BussinessType == "IM")
                {
                    addStrSea.Append(" AND JC.OPERATOR_MST_FK=" + ShippingLine);
                }
            }

            if (!string.IsNullOrEmpty(VslVoy))
            {
                addStrSea.Append(" AND  (VVT.VESSEL_ID) in ('" + VslVoy + "')");
            }
            //'
            if (!string.IsNullOrEmpty(RefNumber))
            {
                if (Convert.ToInt32(Process) == 1)
                {
                    if (Convert.ToInt32(RefType) == 1)
                    {
                        addStrSea.Append(" AND ((UPPER(POL.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(POD.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(EHDR.ENQUIRY_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(QHDR.QUOTATION_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(BHDR.BOOKING_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(JHDR.JOBCARD_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        if (BussinessType == "EX")
                        {
                            addStrSea.Append(" OR (UPPER(HBL.HBL_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                            addStrSea.Append(" OR (UPPER(MBL.MBL_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        }
                        addStrSea.Append(" OR (UPPER(CIT.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(CT.COLLECTIONS_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(IST.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(PT.PAYMENT_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(IASET.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'))");
                    }
                    else if (Convert.ToInt32(RefType) == 2)
                    {
                        addStrSea.Append(" AND UPPER(POL.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 3)
                    {
                        addStrSea.Append(" AND UPPER(POD.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 4)
                    {
                        addStrSea.Append(" AND UPPER(EHDR.ENQUIRY_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 5)
                    {
                        addStrSea.Append(" AND UPPER(QHDR.QUOTATION_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 6)
                    {
                        addStrSea.Append(" AND UPPER(BHDR.BOOKING_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 7)
                    {
                        addStrSea.Append(" AND UPPER(JHDR.JOBCARD_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 8)
                    {
                        if (BussinessType == "EX")
                        {
                            addStrSea.Append(" AND UPPER(HBL.HBL_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                    }
                    else if (Convert.ToInt32(RefType) == 9)
                    {
                        if (BussinessType == "EX")
                        {
                            addStrSea.Append(" AND UPPER(MBL.MBL_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                    }
                    else if (Convert.ToInt32(RefType) == 10)
                    {
                        addStrSea.Append(" AND ((UPPER(CIT.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(IASET.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'))");
                    }
                    else if (Convert.ToInt32(RefType) == 11)
                    {
                        addStrSea.Append(" AND UPPER(CT.COLLECTIONS_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 12)
                    {
                        addStrSea.Append(" AND UPPER(IST.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 13)
                    {
                        addStrSea.Append(" AND UPPER(PT.PAYMENT_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                }
                else
                {
                    if (Convert.ToInt32(RefType) == 1)
                    {
                        addStrSea.Append(" AND ((UPPER(POL.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(POD.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(JC.JOBCARD_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        if (BussinessType == "IM")
                        {
                            addStrSea.Append(" OR (UPPER(JC.Hbl_Hawb_Ref_No) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                            addStrSea.Append(" OR (UPPER(JC.Mbl_Mawb_Ref_No) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        }
                        addStrSea.Append(" OR (UPPER(CAN.CAN_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(DMT.DELIVERY_ORDER_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(CIT.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(CT.COLLECTIONS_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(IST.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(PT.PAYMENT_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(IASET.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'))");
                    }
                    else if (Convert.ToInt32(RefType) == 2)
                    {
                        addStrSea.Append(" AND UPPER(POL.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 3)
                    {
                        addStrSea.Append(" AND UPPER(POD.PORT_ID) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 4)
                    {
                        addStrSea.Append(" AND UPPER(JC.JOBCARD_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 5)
                    {
                        if (BussinessType == "IM")
                        {
                            addStrSea.Append(" AND UPPER(JC.HBL_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                    }
                    else if (Convert.ToInt32(RefType) == 6)
                    {
                        if (BussinessType == "IM")
                        {
                            addStrSea.Append(" AND UPPER(JC.Mbl_Mawb_Ref_No) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                        }
                    }
                    else if (Convert.ToInt32(RefType) == 7)
                    {
                        addStrSea.Append(" AND UPPER(CAN.CAN_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 8)
                    {
                        addStrSea.Append(" AND UPPER(DMT.DELIVERY_ORDER_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 9)
                    {
                        addStrSea.Append(" AND ((UPPER(CIT.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%')");
                        addStrSea.Append(" OR (UPPER(IASET.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'))");
                    }
                    else if (Convert.ToInt32(RefType) == 10)
                    {
                        addStrSea.Append(" AND UPPER(CT.COLLECTIONS_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 11)
                    {
                        addStrSea.Append(" AND UPPER(IST.INVOICE_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                    else if (Convert.ToInt32(RefType) == 12)
                    {
                        addStrSea.Append(" AND UPPER(PT.PAYMENT_REF_NO) LIKE '" + SrOP + RefNumber.ToUpper().Replace("'", "''") + "%'");
                    }
                }
            }

            if (ShipStatus == "HBL/HAWB")
            {
                addStrSea.Append(" AND HBL.HBL_DATE IS NULL ");
            }
            else if (ShipStatus == "MBL/MAWB")
            {
                addStrSea.Append(" AND MBL.MBL_DATE IS NULL ");
            }
            else if (ShipStatus == "Invoice Generated")
            {
                addStrSea.Append(" AND CIT.INVOICE_DATE IS NULL ");
            }
            else if (ShipStatus == "Collection")
            {
                addStrSea.Append(" AND CT.COLLECTIONS_DATE IS NULL ");
            }
            else if (ShipStatus == "Payment")
            {
                addStrSea.Append(" AND PT.PAYMENT_DATE IS  NULL ");
            }
            else if (ShipStatus == "Voucher")
            {
                addStrSea.Append(" AND IST.INVOICE_DATE IS NULL ");
            }
            else if (ShipStatus == "CRO Issued")
            {
                addStrSea.Append(" AND BCR.CRO_DATE IS NULL ");
            }
            else if (ShipStatus == "Delivery Order")
            {
                addStrSea.Append(" AND DMT.DELIVERY_ORDER_DATE IS NULL ");
            }
            else if (ShipStatus == "Cargo Arrival Notice")
            {
                addStrSea.Append(" AND CAN.CAN_DATE IS NULL  ");
            }
            if (Convert.ToInt32(Process) == 2)
            {
                if (ShipStatus == "Surrender BL/AWB")
                {
                    addStrSea.Append("  AND JC.MBLSURRDT IS NULL ");
                }
                else if (ShipStatus == "Customs Clearance")
                {
                    addStrSea.Append("  AND JC.CRQ_DATE IS NULL ");
                }
                else if (ShipStatus == "BRO Received")
                {
                    addStrSea.Append("  AND JC.BRO_DATE IS NULL ");
                }
            }
            if (!string.IsNullOrEmpty(Sector))
            {
                addStrSea.Append(Sector);
            }
            if (flag == 0)
            {
                addStrSea1.Append(" AND 1=2 ");
            }

            if (Group != "0")
            {
                if (BussinessType == "EX")
                {
                    addStrSea.Append(" AND JHDR.COMMODITY_GROUP_FK = " + Group);
                }
                else
                {
                    addStrSea.Append(" AND JC.COMMODITY_GROUP_FK = " + Group);
                }
            }

            if (!string.IsNullOrEmpty(FromDt) & !string.IsNullOrEmpty(ToDt))
            {
                if (BussinessType == "EX")
                {
                    addStrSea.Append("AND to_char(JHDR.JOBCARD_DATE) BETWEEN TO_DATE('" + FromDt + "',DATEFORMAT) and to_date('" + ToDt + "',DATEFORMAT)");
                }
                else
                {
                    addStrSea.Append("AND to_char(JC.JOBCARD_DATE) BETWEEN TO_DATE('" + FromDt + "',DATEFORMAT) and to_date('" + ToDt + "',DATEFORMAT)");
                }
            }

            DataSet DS = null;
            WorkFlow objWF = new WorkFlow();
            strMainQuery = "SELECT ROWNUM AS \"Sl. Nr.\", qry.* FROM ( SELECT M.* FROM ( ";
            strMainQuery = strMainQuery + MainQuery((BussinessType == "EX" ? "SE" : "SI"), LocFK, addStrSea.ToString(), addStrSea1.ToString(), start, last, ShipStatus, BizType);
            strMainQuery = strMainQuery + " UNION " + MainQuery((BussinessType == "EX" ? "AE" : "AI"), LocFK, addStrAir.ToString(), addStrAir1.ToString(), start, last, ShipStatus, BizType);
            if (BussinessType == "EX")
            {
                strMainQuery = strMainQuery + ")M  ORDER BY TO_DATE(\"ATD\", 'dd/MM/yyyy') DESC,TO_DATE(\"Job Card\", 'dd/MM/yyyy') DESC)qry";
            }
            else
            {
                strMainQuery = strMainQuery + ")M  ORDER BY TO_DATE(\"ATA\", '" + dateFormat + "') DESC,TO_DATE(\"Job Card\", '" + dateFormat + "') DESC)qry";
            }

            DS = objWF.GetDataSet(strMainQuery);

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

        #endregion "Fetch for BizType BOTH"
    }
}