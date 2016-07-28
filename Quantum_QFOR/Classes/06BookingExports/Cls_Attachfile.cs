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
    public class Cls_Attachfile : CommonFeatures
    {
        #region "file attachment"

        public bool CheckForAttachment(string filename, string form = "", Int64 PK = 0)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (form == "Quo")
                {
                    strsql.Append("select count(*) from attach_file_dtl_tbl attach where attach.file_name like '" + filename + "' and attach.quotation_mst_fk = " + PK + "");
                }
                else if (form == "JOB")
                {
                    strsql.Append("select count(*) from attach_file_dtl_tbl attach where attach.file_name like '" + filename + "' and attach.job_trn_fk = " + PK + "");
                }
                else
                {
                    strsql.Append("select count(*) from attach_file_dtl_tbl attach where attach.file_name like '" + filename + "' and attach.booking_trn_fk= " + PK + "");
                }

                if (Convert.ToInt32(objWF.ExecuteScaler(strsql.ToString())) > 0)
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "file attachment"

        #region "File Attached"

        public DataSet fetchattachfiles(Int32 FormPk = 0, string From = "", Int32 Biztype = 0, Int32 IntJcType = 2, int BCROFK = 0)
        {
            StringBuilder strquery = new StringBuilder(5000);
            StringBuilder strsubqry = new StringBuilder(5000);
            string str = null;
            string Enqpk = null;
            string Quopk = null;
            string Bkgpk = null;
            string jobpk = null;
            string HBLpk = null;
            string SurveyPk = null;
            string sRFQPK = null;
            string ShipContPk = null;
            string IMPORTJOBPK = null;
            string CAN_PK = null;
            string DELIVERY_ORDERPK = null;
            string Airlinepk = null;
            string Airrfqpk = null;
            string Announcementpk = null;
            string SalesCallPK = null;
            string CROPK = null;
            string TransportNotePK = null;
            string TransContFclPK = null;
            string TransContLclPK = null;
            string CBJCPK = null;
            StringBuilder strsql = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            DataSet dsmain = new DataSet();
            Int32 rowcnt = default(Int32);
            Enqpk = "";
            Quopk = "";
            Bkgpk = "";
            jobpk = "";
            HBLpk = "";
            sRFQPK = "";
            ShipContPk = "";
            IMPORTJOBPK = "";
            CAN_PK = "";
            DELIVERY_ORDERPK = "";
            Airlinepk = "";
            Airrfqpk = "";
            Announcementpk = "";
            SalesCallPK = "";
            CROPK = "";
            TransportNotePK = "";
            TransContFclPK = "";
            TransContLclPK = "";
            CBJCPK = "";

            ///-------------------Gangadhar Task QFOR-Apr-017-------------------------------------------------------------------

            StringBuilder sb = new StringBuilder(5000);
            DataTable dt = new DataTable();
            string strRefType = null;
            string ENQ_FLAG = null;
            string QUOT_FLAG = null;
            string BKG_FLAG = null;
            string CRO_FLAG = null;
            string EXPJOB_FLAG = null;
            string IMPJOB_FLAG = null;
            string HBL_FLAG = null;
            string CAN_FLAG = null;
            string DO_FLAG = null;
            string SRFQ_FLAG = null;
            string CONTRACT_FLAG = null;
            string WAREHOUSE_FLAG = null;
            string TRCNT_FLAG = null;
            string CBJC_FLAG = null;
            string TRANS_FLAG = null;
            string ANN_FLAG = null;
            string SALS_FLAG = null;
            StringBuilder sb1 = new StringBuilder(5000);

            var fromRefType = From;

            //Enquiry
            if (fromRefType == "Enq")
            {
                strRefType = "ENQUIRY";
                //Quotation
            }
            else if (fromRefType == "Quo")
            {
                strRefType = "QUOTATION/IMPORT QUOTATION";
                //Booking
            }
            else if (fromRefType == "Bkg")
            {
                strRefType = "BOOKING/NOMINATION";
                //CRO
            }
            else if (fromRefType == "CRO")
            {
                strRefType = "CRO";
                //Export JC
            }
            else if (fromRefType == "JOB")
            {
                strRefType = "EXPORT JOB CARD";
                //HBL/HAWB
            }
            else if (fromRefType == "HBL")
            {
                strRefType = "HBL/HAWB";
                //Import JC
            }
            else if (fromRefType == "IMPORTJOB")
            {
                strRefType = "IMPORT JOB CARD";
                //CAN
            }
            else if (fromRefType == "CAN")
            {
                strRefType = "CAN";
                //DELIVERY ORDER
            }
            else if (fromRefType == "DO")
            {
                strRefType = "DELIVERY ORDER";

                //Shipping Line/ Airline RFQ
            }
            else if (fromRefType == "SRFQ" | fromRefType == "AIRSRFQ")
            {
                strRefType = "SL/AIRLINE RFQ";
                //Shipping Line/ Airline Contract
            }
            else if (fromRefType == "SHIPCONT" | fromRefType == "Airline")
            {
                strRefType = "SL/AIRLINE CONTRACT";
                //Warehouse Contract
            }
            else if (fromRefType == "WHCONT")
            {
                strRefType = "WAREHOUSE CONTRACT";
                //Transport Contract
            }
            else if (fromRefType == "TRANSCONTFCL" | fromRefType == "TRANSCONTLCL")
            {
                strRefType = "TRANSPORT CONTRACT";

                //CBJC
            }
            else if (fromRefType == "CBJC")
            {
                strRefType = "CBJC";
                //TRANSPORT NOTE
            }
            else if (fromRefType == "TRASPORTNOTE")
            {
                strRefType = "TRANSPORT NOTE";

                //Announcement
            }
            else if (fromRefType == "SALES_CALL")
            {
                strRefType = "SALES CALL";
                //Announcement
            }
            else if (fromRefType == "Announcement")
            {
                strRefType = "ANNOUNCEMENT";
            }

            sb1.Append("         SELECT *");
            sb1.Append("         FROM DOCUMENT_FLOW_DTL DTL");
            sb1.Append("         WHERE DTL.DOCUMENT_FLOW_FK IN");
            sb1.Append("         (SELECT D.DOCUMENT_FLOW_PK");
            sb1.Append("         FROM DOCUMENT_FLOW_TBL D");
            sb1.Append("         WHERE UPPER(D.DESCRIPTION) like '" + strRefType.ToUpper() + "%')");
            sb1.Append("         AND DTL.lOGED_IN_LOC_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            dt = objWF.GetDataTable(sb1.ToString());
            if (dt.Rows.Count > 0)
            {
                int DOCUMENT_FLOW_FK = Convert.ToInt32(dt.Rows[0]["DOCUMENT_FLOW_FK"]);
                SRFQ_FLAG = Convert.ToString(dt.Rows[0]["FLAG3"]);
                CONTRACT_FLAG = Convert.ToString(dt.Rows[0]["FLAG4"]);
                WAREHOUSE_FLAG = Convert.ToString(dt.Rows[0]["FLAG5"]);
                TRCNT_FLAG = Convert.ToString(dt.Rows[0]["FLAG6"]);

                ENQ_FLAG = Convert.ToString(dt.Rows[0]["FLAG7"]);
                QUOT_FLAG = Convert.ToString(dt.Rows[0]["FLAG8"]);
                BKG_FLAG = Convert.ToString(dt.Rows[0]["FLAG9"]);
                CRO_FLAG = Convert.ToString(dt.Rows[0]["FLAG10"]);
                EXPJOB_FLAG = Convert.ToString(dt.Rows[0]["FLAG11"]);
                IMPJOB_FLAG = Convert.ToString(dt.Rows[0]["FLAG12"]);
                HBL_FLAG = Convert.ToString(dt.Rows[0]["FLAG13"]);
                CAN_FLAG = Convert.ToString(dt.Rows[0]["FLAG14"]);
                DO_FLAG = Convert.ToString(dt.Rows[0]["FLAG15"]);

                CBJC_FLAG = Convert.ToString(dt.Rows[0]["FLAG16"]);
                TRANS_FLAG = Convert.ToString(dt.Rows[0]["FLAG17"]);
            }
            //Enquiry
            if (strRefType == "ENQUIRY")
            {
                ENQ_FLAG = "1";
                //Quotation
            }
            else if (strRefType == "QUOTATION/IMPORT QUOTATION")
            {
                QUOT_FLAG = "1";
                //Booking
            }
            else if (strRefType == "BOOKING/NOMINATION")
            {
                BKG_FLAG = "1";
                //CRO
            }
            else if (strRefType == "CRO")
            {
                CRO_FLAG = "1";
                //Export JC
            }
            else if (strRefType == "EXPORT JOB CARD")
            {
                EXPJOB_FLAG = "1";
                //HBL/HAWB
            }
            else if (strRefType == "HBL/HAWB")
            {
                HBL_FLAG = "1";
                //Import JC
            }
            else if (strRefType == "IMPORT JOB CARD")
            {
                IMPJOB_FLAG = "1";
                //CAN
            }
            else if (strRefType == "CAN")
            {
                CAN_FLAG = "1";
                //DELIVERY ORDER
            }
            else if (strRefType == "DELIVERY ORDER")
            {
                DO_FLAG = "1";

                //Shipping Line/ Airline RFQ
            }
            else if (strRefType == "SL/AIRLINE RFQ")
            {
                SRFQ_FLAG = "1";
                //Shipping Line/ Airline Contract
            }
            else if (strRefType == "SL/AIRLINE CONTRACT")
            {
                CONTRACT_FLAG = "1";
                //Warehouse Contract
            }
            else if (strRefType == "WAREHOUSE CONTRACT")
            {
                WAREHOUSE_FLAG = "1";
                //Transport Contract
            }
            else if (strRefType == "TRANSPORT CONTRACT")
            {
                TRCNT_FLAG = "1";

                //CBJC
            }
            else if (strRefType == "CBJC")
            {
                CBJC_FLAG = "1";
                //TRANSPORT NOTE
            }
            else if (strRefType == "TRANSPORT NOTE")
            {
                TRANS_FLAG = "1";
                //Announcement
            }
            else if (strRefType == "ANNOUNCEMENT")
            {
                ANN_FLAG = "1";
                //Announcement
            }
            else if (strRefType == "SALES CALL")
            {
                SALS_FLAG = "1";
            }

            ///-------------------Gangadhar Task QFOR-Apr-017-------------------------------------------------------------------

            if (From == "Enq" | From == "Quo" | From == "Bkg" | From == "CRO" | From == "JOB" | From == "HBL")
            {
                strsql.Append(" select enq.enquiry_bkg_sea_pk  \"ENQPK\" ,qto.QUOTATION_MST_PK \"QTOPK\",bmas.BOOKING_mst_pk \"BKGPK\", bct.CRO_PK, ");
                strsql.Append(" job.JOB_CARD_TRN_PK \"JOBPK\", hbl.hbl_exp_tbl_pk \"HBLPK\", jit.JOB_CARD_TRN_PK IMPJOBPK, CAN.CAN_PK, DOM.delivery_order_pk  DOPK, 0 SALES_CALL_FK, ");
                strsql.Append("0 CRO_PK , 0 TRANSPORT_INST_SEA_PK,0 TRANSPK ,0 TRANSMAINPK ,0 CBJC_PK  from enquiry_bkg_sea_tbl enq,quotation_dtl_tbl qtn,");
                strsql.Append(" quotation_Mst_tbl qto,BOOKING_TRN bkg,BOOKING_MST_TBL bmas, BOOKING_CRO_TBL BCT, JOB_CARD_TRN job,hbl_exp_tbl hbl,JOB_CARD_TRN jit, can_mst_tbl  can, DELIVERY_ORDER_MST_TBL DOM ");
                if (From == "Enq")
                {
                    strsql.Append(" where qtn.trans_ref_no(+)=enq.enquiry_ref_no and qto.quotation_mst_pk(+)=qtn.QUOTATION_MST_FK ");
                    strsql.Append(" and bkg.trans_ref_no(+)=qto.quotation_ref_no and bmas.BOOKING_mst_pk(+)=bkg.BOOKING_MST_FK ");
                    strsql.Append(" and job.BOOKING_MST_FK(+)=bmas.BOOKING_mst_pk and HBL.JOB_CARD_sea_EXP_FK(+)=job.JOB_CARD_TRN_PK ");
                    strsql.Append("   and  bct.booking_mst_fk(+)=bmas.booking_mst_pk ");
                    strsql.Append("   AND  jit.jobcard_ref_no(+)=JOB.JOBCARD_REF_NO ");
                    strsql.Append("   AND DOM.job_card_mst_fk(+)=jit.JOB_CARD_TRN_PK ");
                    strsql.Append(" and can.job_card_fk(+)=jit.job_card_trn_pk ");
                    strsql.Append(" and enq.enquiry_bkg_sea_pk=" + FormPk);
                }
                else if (From == "Quo")
                {
                    strsql.Append(" where qtn.trans_ref_no = enq.enquiry_ref_no(+) and qto.QUOTATION_MST_PK = qtn.QUOTATION_MST_FK(+) ");
                    strsql.Append(" and bkg.trans_ref_no(+) = qto.quotation_ref_no and bmas.BOOKING_mst_pk(+) = bkg.BOOKING_MST_FK ");
                    strsql.Append(" and job.BOOKING_MST_FK(+) = bmas.BOOKING_mst_pk and HBL.JOB_CARD_sea_EXP_FK(+)=job.JOB_CARD_TRN_PK ");
                    strsql.Append(" and qto.QUOTATION_MST_PK=" + FormPk);
                    strsql.Append("   and  bct.booking_mst_fk(+)=bmas.booking_mst_pk ");
                    strsql.Append(" and can.job_card_fk(+)=jit.job_card_trn_pk ");
                    strsql.Append("   AND  jit.jobcard_ref_no(+)=JOB.JOBCARD_REF_NO ");
                    strsql.Append("   AND DOM.job_card_mst_fk(+)=jit.JOB_CARD_TRN_PK ");

                    //strsql.Append(" group by enq.enquiry_bkg_sea_pk,qto.quotation_sea_pk,bmas.BOOKING_mst_pk,job.JOB_CARD_TRN_PK,hbl.hbl_exp_tbl_pk")
                }
                else if (From == "Bkg")
                {
                    strsql.Append(" where qtn.trans_ref_no = enq.enquiry_ref_no(+) and qto.QUOTATION_MST_PK = qtn.QUOTATION_MST_FK(+) ");
                    strsql.Append(" and bkg.trans_ref_no = qto.quotation_ref_no(+) and bmas.BOOKING_mst_pk = bkg.BOOKING_MST_FK(+) ");
                    strsql.Append(" and job.BOOKING_MST_FK(+) = bmas.BOOKING_mst_pk and HBL.JOB_CARD_sea_EXP_FK(+)=job.JOB_CARD_TRN_PK ");
                    strsql.Append(" and bmas.BOOKING_mst_pk=" + FormPk);
                    strsql.Append("   and  bct.booking_mst_fk(+)=bmas.booking_mst_pk ");
                    strsql.Append(" and can.job_card_fk(+)=jit.job_card_trn_pk ");
                    strsql.Append("   AND  jit.jobcard_ref_no(+)=JOB.JOBCARD_REF_NO ");
                    strsql.Append("   AND DOM.job_card_mst_fk(+)=jit.JOB_CARD_TRN_PK ");
                }
                else if (From == "CRO")
                {
                    strsql.Append(" where qtn.trans_ref_no = enq.enquiry_ref_no(+) and qto.QUOTATION_MST_PK = qtn.QUOTATION_MST_FK(+) ");
                    strsql.Append(" and bkg.trans_ref_no = qto.quotation_ref_no(+) and bmas.BOOKING_mst_pk = bkg.BOOKING_MST_FK(+) ");
                    strsql.Append(" and job.BOOKING_MST_FK(+) = bmas.BOOKING_mst_pk and HBL.JOB_CARD_sea_EXP_FK(+)=job.JOB_CARD_TRN_PK ");
                    strsql.Append(" and BCT.CRO_PK =" + FormPk + "");
                    strsql.Append(" and  bct.booking_mst_fk(+)=bmas.booking_mst_pk ");
                    strsql.Append(" and can.job_card_fk(+)=jit.job_card_trn_pk ");
                    strsql.Append("   AND  jit.jobcard_ref_no(+)=JOB.JOBCARD_REF_NO ");
                    strsql.Append("   AND DOM.job_card_mst_fk(+)=jit.JOB_CARD_TRN_PK ");
                }
                else if (From == "JOB")
                {
                    strsql.Append(" where qtn.trans_ref_no = enq.enquiry_ref_no(+) and qto.QUOTATION_MST_PK = qtn.QUOTATION_MST_FK(+)  ");
                    strsql.Append(" and bkg.trans_ref_no = qto.quotation_ref_no(+) and bmas.BOOKING_mst_pk = bkg.BOOKING_MST_FK(+)  ");
                    strsql.Append(" and job.BOOKING_MST_FK = bmas.BOOKING_mst_pk(+) and HBL.JOB_CARD_sea_EXP_FK(+)=job.JOB_CARD_TRN_PK  ");
                    strsql.Append(" and job.JOB_CARD_TRN_PK=" + FormPk);
                    strsql.Append("   and  bct.booking_mst_fk(+)=bmas.booking_mst_pk ");
                    strsql.Append(" and can.job_card_fk(+)=jit.job_card_trn_pk ");
                    strsql.Append("   AND  jit.jobcard_ref_no(+)=JOB.JOBCARD_REF_NO ");
                    strsql.Append("   AND DOM.job_card_mst_fk(+)=jit.JOB_CARD_TRN_PK ");
                }
                else if (From == "HBL")
                {
                    strsql.Append(" where qtn.trans_ref_no = enq.enquiry_ref_no(+) and qto.QUOTATION_MST_PK = qtn.QUOTATION_MST_FK(+)  ");
                    strsql.Append(" and bkg.trans_ref_no = qto.quotation_ref_no(+) and bmas.BOOKING_mst_pk = bkg.BOOKING_MST_FK(+)  ");
                    strsql.Append(" and job.BOOKING_MST_FK = bmas.BOOKING_mst_pk(+) and hbl.job_card_sea_exp_fk=job.JOB_CARD_TRN_PK(+) ");
                    strsql.Append(" and hbl.hbl_exp_tbl_pk=" + FormPk);
                    strsql.Append("   and  bct.booking_mst_fk(+)=bmas.booking_mst_pk ");
                    strsql.Append(" and can.job_card_fk(+)=jit.job_card_trn_pk ");
                    strsql.Append("   AND  jit.jobcard_ref_no(+)=JOB.JOBCARD_REF_NO ");
                    strsql.Append("   AND DOM.job_card_mst_fk(+)=jit.JOB_CARD_TRN_PK ");
                }
                strsql.Append(" group by enq.enquiry_bkg_sea_pk,qto.QUOTATION_MST_PK,bmas.BOOKING_mst_pk,bct.CRO_PK,job.JOB_CARD_TRN_PK,hbl.hbl_exp_tbl_pk,jit.JOB_CARD_TRN_PK, CAN.CAN_PK, DOM.delivery_order_pk");
            }
            else if (From == "IMPORTJOB" | From == "CAN" | From == "DO")
            {
                if (From == "IMPORTJOB")
                {
                    //'added for the dts 6406
                    if (IntJcType == 0)
                    {
                        if ((Biztype == 2))
                        {
                            strsql.Remove(0, strsql.Length);
                            strsql.Append("     SELECT 0                  ENQPK,");
                            strsql.Append("       0                       QTOPK,");
                            strsql.Append("       0                       BKGPK,");
                            strsql.Append("       0                       JOBPK,");
                            strsql.Append("       0                       HBLPK,");
                            strsql.Append("       JIT.JOB_CARD_TRN_PK \"IMPJOBPK\", CAN.CAN_PK, ");
                            strsql.Append("       DOM.DELIVERY_ORDER_PK   \"DOPK\", 0 SALES_CALL_FK,0 CRO_PK,0 TRANSPORT_INST_SEA_PK,0 TRANSPK ,0 TRANSMAINPK ,0 CBJC_PK ");
                            strsql.Append("       FROM JOB_CARD_TRN JIT, can_mst_tbl  can, DELIVERY_ORDER_MST_TBL DOM");
                            strsql.Append("       WHERE JIT.JOB_CARD_TRN_PK = DOM.JOB_CARD_MST_FK(+)");
                            strsql.Append("     and can.job_card_fk(+)=jit.job_card_trn_pk ");
                            strsql.Append("       AND JIT.JOB_CARD_TRN_PK = " + FormPk);
                        }
                        else
                        {
                            strsql.Remove(0, strsql.Length);
                            strsql.Append("     SELECT 0                  ENQPK,");
                            strsql.Append("       0                       QTOPK,");
                            strsql.Append("       0                       BKGPK,");
                            strsql.Append("       0                       JOBPK,");
                            strsql.Append("       0                       HBLPK,");
                            strsql.Append("       JIT.JOB_CARD_TRN_PK \"IMPJOBPK\", CAN.CAN_PK, ");
                            strsql.Append("       DOM.DELIVERY_ORDER_PK   \"DOPK\", 0 SALES_CALL_FK,0 CRO_PK");
                            strsql.Append("       FROM JOB_CARD_TRN JIT, can_mst_tbl  can, DELIVERY_ORDER_MST_TBL DOM");
                            strsql.Append("       WHERE JIT.JOB_CARD_TRN_PK = DOM.JOB_CARD_MST_FK(+)");
                            strsql.Append(" and can.job_card_fk(+)=jit.job_card_trn_pk ");
                            strsql.Append("       AND JIT.JOB_CARD_TRN_PK = " + FormPk);
                        }
                        goto x;
                    }
                    else
                    {
                        strsql.Remove(0, strsql.Length);
                        strsql.Append(" select enq.enquiry_bkg_sea_pk  \"ENQPK\" ,qto.QUOTATION_MST_PK \"QTOPK\",bmas.BOOKING_mst_pk \"BKGPK\" ");
                        strsql.Append(" ,job.JOB_CARD_TRN_PK \"JOBPK\", hbl.hbl_exp_tbl_pk \"HBLPK\", jit.JOB_CARD_TRN_PK \"IMPJOBPK\", can.can_pk, DOM.DELIVERY_ORDER_PK \"DOPK\", 0 SALES_CALL_FK ,0 CRO_PK,0 TRANSPORT_INST_SEA_PK,0 TRANSPK ,0 TRANSMAINPK ,0 CBJC_PK   from enquiry_bkg_sea_tbl enq,QUOTATION_DTL_TBL qtn,");
                        strsql.Append(" QUOTATION_MST_TBL qto,BOOKING_TRN bkg,BOOKING_MST_TBL bmas,JOB_CARD_TRN job,hbl_exp_tbl hbl,JOB_CARD_TRN jit, can_mst_tbl  can, DELIVERY_ORDER_MST_TBL DOM ");

                        strsql.Append(" where qtn.trans_ref_no = enq.enquiry_ref_no(+) and qto.QUOTATION_MST_PK = qtn.QUOTATION_MST_FK(+)  ");
                        strsql.Append(" and bkg.trans_ref_no = qto.quotation_ref_no(+) and bmas.BOOKING_mst_pk = bkg.BOOKING_MST_FK(+)  ");
                        strsql.Append(" and job.BOOKING_MST_FK = bmas.BOOKING_mst_pk(+) and HBL.JOB_CARD_sea_EXP_FK(+)=job.JOB_CARD_TRN_PK ");
                        //'
                        strsql.Append(" and can.job_card_fk(+)=jit.job_card_trn_pk ");
                        strsql.Append("   AND  jit.jobcard_ref_no=JOB.JOBCARD_REF_NO(+)");
                        //'
                        strsql.Append("   AND DOM.job_card_mst_fk(+)=jit.JOB_CARD_TRN_PK ");
                        strsql.Append("  and jit.JOB_CARD_TRN_PK=" + FormPk);
                    }
                }
                else if (From == "CAN")
                {
                    strsql.Remove(0, strsql.Length);
                    strsql.Append(" select enq.enquiry_bkg_sea_pk  \"ENQPK\" ,qto.QUOTATION_MST_PK \"QTOPK\",bmas.BOOKING_mst_pk \"BKGPK\" ");
                    strsql.Append(" ,job.JOB_CARD_TRN_PK \"JOBPK\", hbl.hbl_exp_tbl_pk \"HBLPK\", jit.JOB_CARD_TRN_PK \"IMPJOBPK\", can.can_pk, DOM.DELIVERY_ORDER_PK \"DOPK\", 0 SALES_CALL_FK , 0 CRO_PK ,0 TRANSPORT_INST_SEA_PK,0 TRANSPK ,0 TRANSMAINPK ,0 CBJC_PK  from enquiry_bkg_sea_tbl enq,QUOTATION_DTL_TBL qtn,");
                    strsql.Append(" QUOTATION_MST_TBL qto,BOOKING_TRN bkg,BOOKING_MST_TBL bmas,JOB_CARD_TRN job,hbl_exp_tbl hbl,JOB_CARD_TRN jit,  can_mst_tbl  can, DELIVERY_ORDER_MST_TBL DOM ");

                    strsql.Append(" where qtn.trans_ref_no = enq.enquiry_ref_no(+) and qto.QUOTATION_MST_PK = qtn.QUOTATION_MST_FK(+)  ");
                    strsql.Append(" and bkg.trans_ref_no = qto.quotation_ref_no(+) and bmas.BOOKING_mst_pk = bkg.BOOKING_MST_FK(+)  ");
                    strsql.Append(" and job.BOOKING_MST_FK = bmas.BOOKING_mst_pk(+) and HBL.JOB_CARD_sea_EXP_FK(+)=job.JOB_CARD_TRN_PK ");
                    strsql.Append(" and can.job_card_fk(+)=jit.job_card_trn_pk ");
                    strsql.Append(" AND  jit.jobcard_ref_no(+)=JOB.JOBCARD_REF_NO ");
                    strsql.Append(" AND DOM.job_card_mst_fk(+)=jit.JOB_CARD_TRN_PK ");
                    strsql.Append("  and can.can_pk=" + FormPk);
                }
                else if (From == "DO")
                {
                    strsql.Remove(0, strsql.Length);
                    strsql.Append(" select enq.enquiry_bkg_sea_pk  \"ENQPK\" ,qto.QUOTATION_MST_PK \"QTOPK\",bmas.BOOKING_mst_pk \"BKGPK\" ");
                    strsql.Append(" ,job.JOB_CARD_TRN_PK \"JOBPK\", hbl.hbl_exp_tbl_pk \"HBLPK\", jit.JOB_CARD_TRN_PK \"IMPJOBPK\", CAN.CAN_PK, DOM.DELIVERY_ORDER_PK \"DOPK\", 0 SALES_CALL_FK , 0 CRO_PK ,0 TRANSPORT_INST_SEA_PK,0 TRANSPK ,0 TRANSMAINPK ,0 CBJC_PK  from enquiry_bkg_sea_tbl enq,QUOTATION_DTL_TBL qtn,");
                    strsql.Append(" QUOTATION_MST_TBL qto,BOOKING_TRN bkg,BOOKING_MST_TBL bmas,JOB_CARD_TRN job,hbl_exp_tbl hbl,JOB_CARD_TRN jit, can_mst_tbl  can, DELIVERY_ORDER_MST_TBL DOM ");

                    strsql.Append(" where qtn.trans_ref_no = enq.enquiry_ref_no(+) and qto.QUOTATION_MST_PK = qtn.QUOTATION_MST_FK(+)  ");
                    strsql.Append(" and bkg.trans_ref_no = qto.quotation_ref_no(+) and bmas.BOOKING_mst_pk = bkg.BOOKING_MST_FK(+)  ");
                    strsql.Append(" and job.BOOKING_MST_FK = bmas.BOOKING_mst_pk(+) and HBL.JOB_CARD_sea_EXP_FK(+)=job.JOB_CARD_TRN_PK ");
                    strsql.Append(" and can.job_card_fk(+)=jit.job_card_trn_pk ");
                    strsql.Append("   AND  jit.jobcard_ref_no(+)=JOB.JOBCARD_REF_NO ");
                    strsql.Append("   AND DOM.job_card_mst_fk(+)=jit.JOB_CARD_TRN_PK ");
                    strsql.Append("  and DOM.DELIVERY_ORDER_PK=" + FormPk);
                }
                strsql.Append(" group by enq.enquiry_bkg_sea_pk,qto.QUOTATION_MST_PK,bmas.BOOKING_mst_pk,job.JOB_CARD_TRN_PK,hbl.hbl_exp_tbl_pk,jit.JOB_CARD_TRN_PK, CAN.CAN_PK, DOM.delivery_order_pk");
            }
            else if (From == "SRFQ")
            {
                strsql.Append("select 0  ENQPK,");
                strsql.Append("0  QTOPK,");
                strsql.Append("0  BKGPK,");
                strsql.Append("0  JOBPK,");
                strsql.Append("0  HBLPK,");
                strsql.Append("RFQ.RFQ_MAIN_SEA_PK  SRFQPK,0 IMPJOBPK, 0 can_pk, 0 DOPK, 0 SALES_CALL_FK,0 CRO_PK,0 TRANSPORT_INST_SEA_PK,0 TRANSPK ,0 TRANSMAINPK ,0 CBJC_PK ");
                strsql.Append(" from RFQ_MAIN_SEA_TBL RFQ ");
                strsql.Append(" where RFQ.RFQ_MAIN_SEA_PK =" + FormPk + "");
            }
            else if (From == "AIRSRFQ")
            {
                strsql.Append("select 0  ENQPK,");
                strsql.Append("0  QTOPK,");
                strsql.Append("0  BKGPK,");
                strsql.Append("0  JOBPK,");
                strsql.Append("0  HBLPK,");
                strsql.Append("RFQ.RFQ_MAIN_Air_PK  AIRSRFQPK,0 IMPJOBPK,  0 can_pk, 0 DOPK, 0 SALES_CALL_FK,0 CRO_PK ,0 TRANSPORT_INST_SEA_PK,0 TRANSPK ,0 TRANSMAINPK,0 CBJC_PK ");
                strsql.Append(" from RFQ_MAIN_AIR_TBL RFQ ");
                strsql.Append(" where RFQ.RFQ_MAIN_AIR_PK =" + FormPk + "");
            }
            else if (From == "SHIPCONT")
            {
                strsql.Append("select 0  ENQPK,");
                strsql.Append("0  QTOPK,");
                strsql.Append("0  BKGPK,");
                strsql.Append("0  JOBPK,");
                strsql.Append("0  HBLPK,");
                strsql.Append("0 CONTPK,0 IMPJOBPK,  0 can_pk, 0 DOPK, R.rfq_main_sea_pk SRFQPK, CONT.CONT_MAIN_SEA_PK SHIPCONTPK, 0 SALES_CALL_FK,0 CRO_PK ,0 TRANSPORT_INST_SEA_PK , 0 TRANSPK,0 TRANSMAINPK,0 CBJC_PK ");
                strsql.Append(" from CONT_MAIN_SEA_TBL CONT, RFQ_MAIN_SEA_TBL R ");
                strsql.Append(" where CONT.CONT_MAIN_SEA_PK =" + FormPk + "");
                strsql.Append(" and CONT.rfq_main_tbl_fk=r.rfq_main_sea_pk(+)");
            }
            else if (From == "Airline")
            {
                strsql.Append("select 0  ENQPK,");
                strsql.Append("0  QTOPK,");
                strsql.Append("0  BKGPK,");
                strsql.Append("0  JOBPK,");
                strsql.Append("0  HBLPK,");
                strsql.Append("0 CONTPK,0 IMPJOBPK,  0 can_pk, 0 DOPK, r.rfq_main_air_pk AIRSRFQPK, CONT.CONT_MAIN_AIR_PK Airpk, 0 SALES_CALL_FK, 0 CRO_PK , 0 TRANSPORT_INST_SEA_PK, 0 TRANSPK,0 TRANSMAINPK, 0 CBJC_PK ");
                strsql.Append(" from CONT_MAIN_AIR_TBL CONT, RFQ_MAIN_air_TBL R ");
                strsql.Append(" where CONT.CONT_MAIN_AIR_PK =" + FormPk + "");
                strsql.Append(" and CONT.rfq_main_air_fk=r.rfq_main_air_pk(+)");
            }
            else if (From == "Announcement")
            {
                strsql.Append("select 0  ENQPK,");
                strsql.Append("0  QTOPK,");
                strsql.Append("0  BKGPK,");
                strsql.Append("0  JOBPK,");
                strsql.Append("0  HBLPK,");
                strsql.Append("0 CONTPK,0 IMPJOBPK,  0 can_pk, 0 DOPK,0 Airpk,Anon.announcement_pk Announcementpk, 0 SALES_CALL_FK, 0 CRO_PK ,0 TRANSPORT_INST_SEA_PK,0 TRANSPK ,0 TRANSMAINPK ,0 CBJC_PK ");
                strsql.Append(" from  announcement_tbl Anon ");
                strsql.Append(" where Anon.announcement_pk=" + FormPk + "");
            }
            else if (From == "SALES_CALL")
            {
                strsql.Append("select 0  ENQPK,");
                strsql.Append("0  QTOPK,");
                strsql.Append("0  BKGPK,");
                strsql.Append("0  JOBPK,");
                strsql.Append("0  HBLPK,");
                strsql.Append("0 CONTPK,0 IMPJOBPK,  0 can_pk, 0 DOPK,0 Airpk,SALES_CALL_PK SALES_CALL_FK,0 CRO_PK ,0 TRANSPORT_INST_SEA_PK, 0 TRANSPK,0 TRANSMAINPK,0 CBJC_PK ");
                strsql.Append(" from  SALES_CALL_TRN ");
                strsql.Append(" where SALES_CALL_PK =" + FormPk + "");
            }
            else if (From == "TRANSCONTFCL")
            {
                strsql.Append("select 0  ENQPK,");
                strsql.Append("0  QTOPK,");
                strsql.Append("0  BKGPK,");
                strsql.Append("0  JOBPK,");
                strsql.Append("0  HBLPK,");
                strsql.Append("0 CONTPK,0 IMPJOBPK,  0 can_pk, 0 DOPK,0 Airpk,0 SALES_CALL_FK, 0 CRO_PK, 0 TRANSPORT_INST_SEA_PK,CON.CONT_TRANS_FCL_PK TRANSPK,0 TRANSMAINPK, 0 CBJC_PK ");
                strsql.Append("  from  CONT_TRANS_FCL_TBL CON ");
                strsql.Append("  where CON.CONT_TRANS_FCL_PK=" + FormPk + "");
            }
            else if (From == "TRANSCONTLCL")
            {
                strsql.Append("select 0  ENQPK,");
                strsql.Append("0  QTOPK,");
                strsql.Append("0  BKGPK,");
                strsql.Append("0  JOBPK,");
                strsql.Append("0  HBLPK,");
                strsql.Append("0 CONTPK,0 IMPJOBPK,  0 can_pk, 0 DOPK,0 Airpk,0 SALES_CALL_FK, 0 CRO_PK, 0 TRANSPORT_INST_SEA_PK,CON.CONT_MAIN_TRANS_PK TRANSMAINPK,0 CBJC_PK ");
                strsql.Append("  from  CONT_MAIN_TRANS_TBL CON ");
                strsql.Append("  where CON.CONT_MAIN_TRANS_PK=" + FormPk + "");
            }
            else if (From == "CBJC" | From == "TRASPORTNOTE")
            {
                if (From == "CBJC")
                {
                    strsql.Append("select 0  ENQPK,");
                    strsql.Append("0  QTOPK,");
                    strsql.Append("0  BKGPK,");
                    strsql.Append("JC_FK JOBPK,");
                    strsql.Append("0  HBLPK,");
                    strsql.Append("0 CONTPK,JC_FK IMPJOBPK,  0 can_pk, 0 DOPK,0 Airpk,0 SALES_CALL_FK, 0 CRO_PK, 0 TRANSPORT_INST_SEA_PK,0 TRANSMAINPK, CBJC_PK ");
                    strsql.Append("  from  ( ");
                    strsql.Append("  SELECT NVL(T.JC_FK,0) JC_FK, T.CBJC_PK FROM CBJC_TBL t");
                    strsql.Append("  where T.CBJC_PK=" + FormPk + "");
                    strsql.Append("  ) ");
                }
                else if (From == "TRASPORTNOTE")
                {
                    strsql.Append("select 0  ENQPK,");
                    strsql.Append(" 0  QTOPK,");
                    strsql.Append(" 0  BKGPK,");
                    strsql.Append(" JOBPK  JOBPK,");
                    strsql.Append(" 0  HBLPK,");
                    strsql.Append(" 0 CONTPK, JOBPK IMPJOBPK,  0 can_pk, 0 DOPK,0 Airpk,0 SALES_CALL_FK, 0 CRO_PK, TRANSPORT_INST_SEA_PK, 0 TRANSPK,0 TRANSMAINPK, CBJC_PK ");
                    strsql.Append("  from ( ");
                    strsql.Append(" SELECT JCT.JOB_CARD_TRN_PK JOBPK, C.CBJC_PK, T.TRANSPORT_INST_SEA_PK");
                    strsql.Append("  FROM JOB_CARD_TRN JCT, CBJC_TBL C, TRANSPORT_INST_SEA_TBL T ");
                    strsql.Append(" WHERE 1 = 1");
                    strsql.Append("   AND C.JC_FK(+) = JCT.JOB_CARD_TRN_PK ");
                    strsql.Append("   AND INSTR(',' || T.JOB_CARD_FK || ',', ',' || C.CBJC_PK || ',') > 0");
                    strsql.Append("   AND T.JC_TYPE = 2");
                    strsql.Append("   AND T.TP_CBJC_JC = 1");
                    strsql.Append("   AND T.TRANSPORT_INST_SEA_PK =" + FormPk + "");
                    strsql.Append(" UNION");
                    strsql.Append(" SELECT JCT.JOB_CARD_TRN_PK JOBPK, 0 CBJC_PK, T.TRANSPORT_INST_SEA_PK");
                    strsql.Append("  FROM JOB_CARD_TRN JCT, TRANSPORT_INST_SEA_TBL T");
                    strsql.Append(" WHERE 1 = 1");
                    strsql.Append("   AND INSTR(',' || T.JOB_CARD_FK || ',', ',' || JCT.JOB_CARD_TRN_PK || ',') > 0");
                    strsql.Append("   AND T.JC_TYPE = 2");
                    strsql.Append("   AND T.TP_CBJC_JC = 2");
                    strsql.Append("   AND T.TRANSPORT_INST_SEA_PK =" + FormPk + "");
                    strsql.Append(" UNION");
                    strsql.Append(" SELECT 0 JOBPK, 0 CBJC_PK, T.TRANSPORT_INST_SEA_PK");
                    strsql.Append("  FROM  TRANSPORT_INST_SEA_TBL T");
                    strsql.Append(" WHERE 1 = 1");
                    strsql.Append(" AND T.JC_TYPE =1");
                    strsql.Append("   AND T.TRANSPORT_INST_SEA_PK =" + FormPk + "");
                    strsql.Append("   ) ");
                }
            }
            else
            {
                strsql.Append("select 0  ENQPK,");
                strsql.Append("0  QTOPK,");
                strsql.Append("0  BKGPK,");
                strsql.Append("0  JOBPK,");
                strsql.Append("0  HBLPK,");
                strsql.Append(" rsmt.survey_mst_pk  SURPK,0 IMPJOBPK,  0 can_pk, 0 DOPK, 0 SALES_CALL_FK,0 CRO_PK ,0 TRANSPORT_INST_SEA_PK,0 TRANSPK,0 TRANSMAINPK,0 CBJC_PK");
                strsql.Append(" from rem_m_survey_mst_tbl rsmt ");
                strsql.Append(" where rsmt.survey_mst_pk =" + FormPk + "");
            }
            //END
            if (Biztype == 1)
            {
                strsql.ToString().ToLower();
                strsql.Replace("QUOTATION_DTL_TBL", "QUOTATION_DTL_TBL");
                strsql.Replace("hbl_exp_tbl", "hawb_exp_tbl");
                strsql.Replace("hbl_exp_tbl_pk", "hawb_exp_tbl_pk");
                strsql.Replace("sea", "air");
            }
            x:
            dsmain = objWF.GetDataSet(strsql.ToString());
            if ((dsmain != null))
            {
                if (dsmain.Tables[0].Rows.Count > 0)
                {
                    for (rowcnt = 0; rowcnt <= dsmain.Tables[0].Rows.Count - 1; rowcnt++)
                    {
                        if (From == "Enq" | From == "Quo" | From == "Bkg" | From == "CRO" | From == "JOB" | From == "HBL")
                        {
                            if (string.IsNullOrEmpty(getDefault(Enqpk, "").ToString()))
                            {
                                Enqpk = getDefault(dsmain.Tables[0].Rows[0]["ENQPK"], "").ToString();
                            }
                            else
                            {
                                Enqpk += "," + getDefault(dsmain.Tables[0].Rows[0]["ENQPK"], "");
                            }

                            if (string.IsNullOrEmpty(getDefault(Quopk, "").ToString()))
                            {
                                Quopk = getDefault(dsmain.Tables[0].Rows[0]["QTOPK"], "").ToString();
                            }
                            else
                            {
                                Quopk += "," + getDefault(dsmain.Tables[0].Rows[0]["QTOPK"], "");
                            }

                            if (string.IsNullOrEmpty(getDefault(Bkgpk, "").ToString()))
                            {
                                Bkgpk = getDefault(dsmain.Tables[0].Rows[0]["BKGPK"], "").ToString();
                            }
                            else
                            {
                                Bkgpk += "," + getDefault(dsmain.Tables[0].Rows[0]["BKGPK"], "");
                            }
                            if (string.IsNullOrEmpty(getDefault(CROPK, "").ToString()))
                            {
                                CROPK = getDefault(dsmain.Tables[0].Rows[0]["CRO_PK"], "").ToString();
                            }
                            else
                            {
                                CROPK += "," + getDefault(dsmain.Tables[0].Rows[0]["CRO_PK"], "");
                            }
                            if (string.IsNullOrEmpty(getDefault(jobpk, "").ToString()))
                            {
                                jobpk = getDefault(dsmain.Tables[0].Rows[0]["JOBPK"], "").ToString();
                            }
                            else
                            {
                                jobpk += "," + getDefault(dsmain.Tables[0].Rows[0]["JOBPK"], "");
                            }

                            if (string.IsNullOrEmpty(getDefault(HBLpk, "").ToString()))
                            {
                                HBLpk = getDefault(dsmain.Tables[0].Rows[0]["HBLPK"], "").ToString();
                            }
                            else
                            {
                                HBLpk += "," + getDefault(dsmain.Tables[0].Rows[0]["HBLPK"], "");
                            }
                        }
                        else if (From == "IMPORTJOB" | From == "CAN" | From == "DO")
                        {
                            if (string.IsNullOrEmpty(getDefault(IMPORTJOBPK, "").ToString()))
                            {
                                IMPORTJOBPK = getDefault(dsmain.Tables[0].Rows[0]["IMPJOBPK"], "").ToString();
                            }
                            else
                            {
                                IMPORTJOBPK += "," + getDefault(dsmain.Tables[0].Rows[0]["IMPJOBPK"], "");
                            }
                            if (string.IsNullOrEmpty(getDefault(CAN_PK, "").ToString()))
                            {
                                CAN_PK = getDefault(dsmain.Tables[0].Rows[0]["CAN_PK"], "").ToString();
                            }
                            else
                            {
                                CAN_PK += "," + getDefault(dsmain.Tables[0].Rows[0]["CAN_PK"], "");
                            }
                            if (string.IsNullOrEmpty(getDefault(DELIVERY_ORDERPK, "").ToString()))
                            {
                                DELIVERY_ORDERPK = getDefault(dsmain.Tables[0].Rows[0]["DOPK"], "").ToString();
                            }
                            else
                            {
                                DELIVERY_ORDERPK += "," + getDefault(dsmain.Tables[0].Rows[0]["DOPK"], "");
                            }
                            //'added
                        }

                        if (From == "SRFQ" | From == "SHIPCONT")
                        {
                            if (string.IsNullOrEmpty(getDefault(sRFQPK, "").ToString()))
                            {
                                sRFQPK = getDefault(dsmain.Tables[0].Rows[0]["SRFQPK"], "").ToString();
                            }
                            else
                            {
                                sRFQPK += "," + getDefault(dsmain.Tables[0].Rows[0]["SRFQPK"], "");
                            }
                        }

                        if (From == "SHIPCONT")
                        {
                            if (string.IsNullOrEmpty(getDefault(ShipContPk, "").ToString()))
                            {
                                ShipContPk = getDefault(dsmain.Tables[0].Rows[0]["SHIPCONTPK"], "").ToString();
                            }
                            else
                            {
                                ShipContPk += "," + getDefault(dsmain.Tables[0].Rows[0]["SHIPCONTPK"], "");
                            }
                        }

                        if (From == "AIRSRFQ" | From == "Airline")
                        {
                            if (string.IsNullOrEmpty(getDefault(Airrfqpk, "").ToString()))
                            {
                                Airrfqpk = getDefault(dsmain.Tables[0].Rows[0]["AIRSRFQPK"], "").ToString();
                            }
                            else
                            {
                                Airrfqpk += "," + getDefault(dsmain.Tables[0].Rows[0]["AIRSRFQPK"], "");
                            }
                        }
                        if (From == "Airline")
                        {
                            if (string.IsNullOrEmpty(getDefault(ShipContPk, "").ToString()))
                            {
                                Airlinepk = getDefault(dsmain.Tables[0].Rows[0]["Airpk"], "").ToString();
                            }
                            else
                            {
                                Airlinepk += "," + getDefault(dsmain.Tables[0].Rows[0]["Airpk"], "");
                            }
                        }
                        //'
                        if (From == "CBJC" | From == "TRASPORTNOTE")
                        {
                            if (string.IsNullOrEmpty(getDefault(jobpk, "").ToString()))
                            {
                                jobpk = getDefault(dsmain.Tables[0].Rows[0]["JOBPK"], "").ToString();
                            }
                            else
                            {
                                jobpk += "," + getDefault(dsmain.Tables[0].Rows[0]["JOBPK"], "");
                            }
                            if (string.IsNullOrEmpty(getDefault(IMPORTJOBPK, "").ToString()))
                            {
                                IMPORTJOBPK = getDefault(dsmain.Tables[0].Rows[0]["IMPJOBPK"], "").ToString();
                            }
                            else
                            {
                                IMPORTJOBPK += "," + getDefault(dsmain.Tables[0].Rows[0]["IMPJOBPK"], "");
                            }
                            if (string.IsNullOrEmpty(getDefault(CBJCPK, "").ToString()))
                            {
                                CBJCPK = getDefault(dsmain.Tables[0].Rows[0]["CBJC_PK"], "").ToString();
                            }
                            else
                            {
                                CBJCPK += "," + getDefault(dsmain.Tables[0].Rows[0]["CBJC_PK"], "");
                            }
                        }
                        if (From == "TRASPORTNOTE")
                        {
                            if (string.IsNullOrEmpty(getDefault(TransportNotePK, "").ToString()))
                            {
                                TransportNotePK = getDefault(dsmain.Tables[0].Rows[0]["TRANSPORT_INST_SEA_PK"], "").ToString();
                            }
                            else
                            {
                                TransportNotePK += "," + getDefault(dsmain.Tables[0].Rows[0]["TRANSPORT_INST_SEA_PK"], "");
                            }
                        }

                        if (From == "SALES_CALL")
                        {
                            if (string.IsNullOrEmpty(getDefault(SalesCallPK, "").ToString()))
                            {
                                SalesCallPK = getDefault(dsmain.Tables[0].Rows[0]["SALES_CALL_FK"], "").ToString();
                            }
                            else
                            {
                                SalesCallPK += "," + getDefault(dsmain.Tables[0].Rows[0]["SALES_CALL_FK"], "");
                            }
                        }
                        //'
                        if (From == "TRANSCONTFCL")
                        {
                            if (string.IsNullOrEmpty(getDefault(TransContFclPK, "").ToString()))
                            {
                                TransContFclPK = getDefault(dsmain.Tables[0].Rows[0]["TRANSPK"], "").ToString();
                            }
                            else
                            {
                                TransContFclPK += "," + getDefault(dsmain.Tables[0].Rows[0]["TRANSPK"], "");
                            }
                        }
                        //'
                        if (From == "TRANSCONTLCL")
                        {
                            if (string.IsNullOrEmpty(getDefault(TransContFclPK, "").ToString()))
                            {
                                TransContLclPK = getDefault(dsmain.Tables[0].Rows[0]["TRANSMAINPK"], "").ToString();
                            }
                            else
                            {
                                TransContLclPK += "," + getDefault(dsmain.Tables[0].Rows[0]["TRANSMAINPK"], "");
                            }
                        }
                        if (From == "Removals")
                        {
                            if (string.IsNullOrEmpty(getDefault(SurveyPk, "").ToString()))
                            {
                                SurveyPk = getDefault(dsmain.Tables[0].Rows[0]["SURPK"], "").ToString();
                            }
                            else
                            {
                                SurveyPk += "," + getDefault(dsmain.Tables[0].Rows[0]["SURPK"], "");
                            }
                        }
                        if (From == "Announcement")
                        {
                            if (string.IsNullOrEmpty(getDefault(Announcementpk, "").ToString()))
                            {
                                Announcementpk = getDefault(dsmain.Tables[0].Rows[0]["Announcementpk"], "").ToString();
                            }
                            else
                            {
                                Announcementpk += "," + getDefault(dsmain.Tables[0].Rows[0]["Announcementpk"], "");
                            }
                        }
                    }
                }
            }

            str = str + ("SELECT ROWNUM SNO, Q.* FROM (");
            str = str + ("SELECT DISTINCT ");
            str = str + ("            QRY.*   FROM (");
            if (Enqpk.Length != 0 & Convert.ToInt32(ENQ_FLAG) == 1)
            {
                strquery.Append("            SELECT   ");
                strquery.Append("            '' Icon,");
                strquery.Append("            AFD.FILE_NAME,");
                strquery.Append("            AFD.FILE_TYPE,");
                strquery.Append("            AFD.FILE_SIZE,");
                strquery.Append("           USR.USER_NAME,");
                strquery.Append("            AFD.Created_Dt,");
                strquery.Append("           MU.USER_NAME Modified_by,");
                strquery.Append("           AFD.Modified_Dt,");
                strquery.Append("            'false' Chk,");
                strquery.Append("            AFD.file_path FILE_PATH,");
                strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                strquery.Append("            AFD.AIRLINE_FK Airpk,");
                strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK,AFD.TRANSPORT_NOTEPK ,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU ");
                strquery.Append("            WHERE");
                strquery.Append("            AFD.ENQUIRY_MST_FK  in ( " + Enqpk + ")");
                strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+)");
                strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
            }
            if (strquery.Length != 0 & (Quopk.Length != 0 | Bkgpk.Length != 0) & Convert.ToInt32(QUOT_FLAG) == 1)
            {
                strquery.Append("           UNION  ");
            }
            if (Quopk.Length != 0 & Convert.ToInt32(QUOT_FLAG) == 1)
            {
                strquery.Append("            SELECT   ");
                strquery.Append("            '' Icon,");
                strquery.Append("            AFD.FILE_NAME,");
                strquery.Append("            AFD.FILE_TYPE,");
                strquery.Append("            AFD.FILE_SIZE,");
                strquery.Append("           USR.USER_NAME,");
                strquery.Append("            AFD.Created_Dt,");
                strquery.Append("           MU.USER_NAME Modified_by,");
                strquery.Append("           AFD.Modified_Dt,");
                strquery.Append("            'false' Chk,");
                strquery.Append("            AFD.file_path FILE_PATH,");
                strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                strquery.Append("            AFD.AIRLINE_FK Airpk,");
                strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK,AFD.TRANSPORT_NOTEPK ,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                strquery.Append("            WHERE");
                strquery.Append("            AFD.QUOTATION_MST_FK  in ( " + Quopk + ")");
                strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
            }
            if ((strquery.Length != 0 & Bkgpk.Length != 0 & Convert.ToInt32(BKG_FLAG) == 1))
            {
                strquery.Append("           UNION  ");
            }
            if (Bkgpk.Length != 0 & Convert.ToInt32(BKG_FLAG) == 1)
            {
                strquery.Append("            SELECT   ");
                strquery.Append("            '' Icon,");
                strquery.Append("            AFD.FILE_NAME,");
                strquery.Append("            AFD.FILE_TYPE,");
                strquery.Append("            AFD.FILE_SIZE,");
                strquery.Append("           USR.USER_NAME,");
                strquery.Append("            AFD.Created_Dt,");
                strquery.Append("           MU.USER_NAME Modified_by,");
                strquery.Append("           AFD.Modified_Dt,");
                strquery.Append("            'false' Chk,");
                strquery.Append("            AFD.file_path FILE_PATH,");
                strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                strquery.Append("            AFD.AIRLINE_FK Airpk,");
                strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK ,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD, User_Mst_Tbl USR, USER_MST_TBL MU");
                strquery.Append("            WHERE");
                strquery.Append("            AFD.BOOKING_TRN_FK   in ( " + Bkgpk + ")");
                strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
            }
            if ((strquery.Length != 0 & jobpk.Length != 0 & Convert.ToInt32(EXPJOB_FLAG) == 1))
            {
                strquery.Append("           UNION  ");
            }
            if (jobpk.Length != 0 & Convert.ToInt32(EXPJOB_FLAG) == 1)
            {
                strquery.Append("            SELECT   ");
                strquery.Append("            '' Icon,");
                strquery.Append("            AFD.FILE_NAME,");
                strquery.Append("            AFD.FILE_TYPE,");
                strquery.Append("            AFD.FILE_SIZE,");
                strquery.Append("           USR.USER_NAME,");
                strquery.Append("            AFD.Created_Dt,");
                strquery.Append("           MU.USER_NAME Modified_by,");
                strquery.Append("           AFD.Modified_Dt,");
                strquery.Append("            'false' Chk,");
                strquery.Append("            AFD.file_path FILE_PATH,");
                strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                strquery.Append("            AFD.AIRLINE_FK Airpk,");
                strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK ,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                strquery.Append("            WHERE");
                strquery.Append("            AFD.JOB_CARD_TRN_FK in ( " + jobpk + ")");
                strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
            }

            if ((strquery.Length != 0 & SalesCallPK.Length != 0 & Convert.ToInt32(SALS_FLAG) == 1))
            {
                strquery.Append("           UNION  ");
            }
            if (SalesCallPK.Length != 0 & Convert.ToInt32(SALS_FLAG) == 1)
            {
                strquery.Append("            SELECT   ");
                strquery.Append("            '' Icon,");
                strquery.Append("            AFD.FILE_NAME,");
                strquery.Append("            AFD.FILE_TYPE,");
                strquery.Append("            AFD.FILE_SIZE,");
                strquery.Append("           USR.USER_NAME,");
                strquery.Append("            AFD.Created_Dt,");
                strquery.Append("           MU.USER_NAME Modified_by,");
                strquery.Append("           AFD.Modified_Dt,");

                strquery.Append("            'false' Chk,");
                strquery.Append("            AFD.file_path FILE_PATH,");
                strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                strquery.Append("            AFD.AIRLINE_FK Airpk,");
                strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK,AFD.TRANSPORT_NOTEPK ,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD, User_Mst_Tbl USR, USER_MST_TBL MU");
                strquery.Append("            WHERE");
                strquery.Append("            AFD.SALES_CALL_FK in ( " + SalesCallPK + ")");
                strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
            }

            if ((strquery.Length != 0 & HBLpk.Length != 0 & Convert.ToInt32(HBL_FLAG) == 1))
            {
                strquery.Append("           UNION  ");
            }
            if (HBLpk.Length != 0 & Convert.ToInt32(HBL_FLAG) == 1)
            {
                strquery.Append("            SELECT   ");
                strquery.Append("            '' Icon,");
                strquery.Append("            AFD.FILE_NAME,");
                strquery.Append("            AFD.FILE_TYPE,");
                strquery.Append("            AFD.FILE_SIZE,");
                strquery.Append("           USR.USER_NAME,");
                strquery.Append("            AFD.Created_Dt,");
                strquery.Append("           MU.USER_NAME Modified_by,");
                strquery.Append("           AFD.Modified_Dt,");

                strquery.Append("            'false' Chk,");
                strquery.Append("            AFD.file_path FILE_PATH,");
                strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                strquery.Append("            AFD.AIRLINE_FK Airpk,");
                strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK ,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK ,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                strquery.Append("            WHERE");
                strquery.Append("            AFD.HBL_TRN_FK in ( " + HBLpk + ")");
                strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
            }
            if ((strquery.Length != 0 & IMPORTJOBPK.Length != 0 & Convert.ToInt32(IMPJOB_FLAG) == 1))
            {
                strquery.Append("           UNION  ");
            }
            if (IMPORTJOBPK.Length != 0 & Convert.ToInt32(IMPJOB_FLAG) == 1)
            {
                strquery.Append("            SELECT   ");
                strquery.Append("            '' Icon,");
                strquery.Append("            AFD.FILE_NAME,");
                strquery.Append("            AFD.FILE_TYPE,");
                strquery.Append("            AFD.FILE_SIZE,");
                strquery.Append("           USR.USER_NAME,");
                strquery.Append("            AFD.Created_Dt,");
                strquery.Append("           MU.USER_NAME Modified_by,");
                strquery.Append("           AFD.Modified_Dt,");
                strquery.Append("            'false' Chk,");
                strquery.Append("            AFD.file_path FILE_PATH,");
                strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                strquery.Append("            AFD.AIRLINE_FK Airpk,");
                strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK,AFD.TRANSPORT_NOTEPK ,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                strquery.Append("            WHERE");
                strquery.Append("            AFD.JOB_CARD_TRN_FK in ( " + IMPORTJOBPK + ")");
                strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
            }
            if ((strquery.Length != 0 & CAN_PK.Length != 0 & Convert.ToInt32(CAN_FLAG) == 1))
            {
                strquery.Append("           UNION  ");
            }
            if (CAN_PK.Length != 0 & Convert.ToInt32(CAN_FLAG) == 1)
            {
                strquery.Append("            SELECT   ");
                strquery.Append("            '' Icon,");
                strquery.Append("            AFD.FILE_NAME,");
                strquery.Append("            AFD.FILE_TYPE,");
                strquery.Append("            AFD.FILE_SIZE,");
                strquery.Append("           USR.USER_NAME,");
                strquery.Append("            AFD.Created_Dt,");
                strquery.Append("           MU.USER_NAME Modified_by,");
                strquery.Append("           AFD.Modified_Dt,");
                strquery.Append("            'false' Chk,");
                strquery.Append("            AFD.file_path FILE_PATH,");
                strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                strquery.Append("            AFD.AIRLINE_FK Airpk,");
                strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK ,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                strquery.Append("            WHERE");
                strquery.Append("            AFD.CAN_FK in ( " + CAN_PK + ")");
                strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
            }
            if ((strquery.Length != 0 & DELIVERY_ORDERPK.Length != 0 & Convert.ToInt32(DO_FLAG) == 1))
            {
                strquery.Append("           UNION  ");
            }
            if (DELIVERY_ORDERPK.Length != 0 & Convert.ToInt32(DO_FLAG) == 1)
            {
                strquery.Append("            SELECT   ");
                strquery.Append("            '' Icon,");
                strquery.Append("            AFD.FILE_NAME,");
                strquery.Append("            AFD.FILE_TYPE,");
                strquery.Append("            AFD.FILE_SIZE,");
                strquery.Append("           USR.USER_NAME,");
                strquery.Append("            AFD.Created_Dt,");
                strquery.Append("           MU.USER_NAME Modified_by,");
                strquery.Append("           AFD.Modified_Dt,");
                strquery.Append("            'false' Chk,");
                strquery.Append("            AFD.file_path FILE_PATH,");
                strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                strquery.Append("            AFD.AIRLINE_FK Airpk,");
                strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK ,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                strquery.Append("            WHERE");
                strquery.Append("            AFD.DELIVERY_ORDER_FK in ( " + DELIVERY_ORDERPK + ")");
                strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
            }
            //'added for the import job card and do
            if ((SurveyPk != null))
            {
                if ((strquery.Length != 0 & SurveyPk.Length != 0))
                {
                    strquery.Append("           UNION  ");
                }
            }
            if ((SurveyPk != null))
            {
                if ((SurveyPk.Length != 0))
                {
                    strquery.Append("            SELECT   ");
                    strquery.Append("            '' Icon,");
                    strquery.Append("            AFD.FILE_NAME,");
                    strquery.Append("            AFD.FILE_TYPE,");
                    strquery.Append("            AFD.FILE_SIZE,");
                    strquery.Append("           USR.USER_NAME,");
                    strquery.Append("            AFD.Created_Dt,");
                    strquery.Append("           MU.USER_NAME Modified_by,");
                    strquery.Append("           AFD.Modified_Dt,");
                    strquery.Append("            'false' Chk,");
                    strquery.Append("            AFD.file_path FILE_PATH,");
                    strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                    strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                    strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                    strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                    strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                    strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                    strquery.Append("            AFD.AIRLINE_FK Airpk,");
                    strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                    strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                    strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                    strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                    strquery.Append("            WHERE");
                    strquery.Append("            AFD.SURVEY_MST_FK in ( " + SurveyPk + ")");
                    strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                    strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
                }
            }
            //'
            //'TransContLclPK
            if ((TransContFclPK != null))
            {
                if ((strquery.Length != 0 & TransContFclPK.Length != 0 & Convert.ToInt32(TRCNT_FLAG) == 1))
                {
                    strquery.Append("           UNION  ");
                }
            }
            if ((TransContFclPK != null))
            {
                if ((TransContFclPK.Length != 0 & Convert.ToInt32(TRCNT_FLAG) == 1))
                {
                    strquery.Append("            SELECT   ");
                    strquery.Append("            '' Icon,");
                    strquery.Append("            AFD.FILE_NAME,");
                    strquery.Append("            AFD.FILE_TYPE,");
                    strquery.Append("            AFD.FILE_SIZE,");
                    strquery.Append("           USR.USER_NAME,");
                    strquery.Append("            AFD.Created_Dt,");
                    strquery.Append("           MU.USER_NAME Modified_by,");
                    strquery.Append("           AFD.Modified_Dt,");
                    strquery.Append("            'false' Chk,");
                    strquery.Append("            AFD.file_path FILE_PATH,");
                    strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                    strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                    strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                    strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                    strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                    strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                    strquery.Append("            AFD.AIRLINE_FK Airpk,");
                    strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                    strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                    strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK ,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                    strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                    strquery.Append("            WHERE");
                    strquery.Append("            AFD.TRANS_CONT_FCL_FK in ( " + TransContFclPK + ")");
                    strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                    strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
                }
            }
            //'
            if ((TransContLclPK != null))
            {
                if ((strquery.Length != 0 & TransContLclPK.Length != 0 & Convert.ToInt32(TRCNT_FLAG) == 1))
                {
                    strquery.Append("           UNION  ");
                }
            }
            if ((TransContLclPK != null))
            {
                if ((TransContLclPK.Length != 0 & Convert.ToInt32(TRCNT_FLAG) == 1))
                {
                    strquery.Append("            SELECT   ");
                    strquery.Append("            '' Icon,");
                    strquery.Append("            AFD.FILE_NAME,");
                    strquery.Append("            AFD.FILE_TYPE,");
                    strquery.Append("            AFD.FILE_SIZE,");
                    strquery.Append("           USR.USER_NAME,");
                    strquery.Append("            AFD.Created_Dt,");
                    strquery.Append("           MU.USER_NAME Modified_by,");
                    strquery.Append("           AFD.Modified_Dt,");
                    strquery.Append("            'false' Chk,");
                    strquery.Append("            AFD.file_path FILE_PATH,");
                    strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                    strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                    strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                    strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                    strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                    strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                    strquery.Append("            AFD.AIRLINE_FK Airpk,");
                    strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                    strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                    strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK ,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                    strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                    strquery.Append("            WHERE");
                    strquery.Append("            AFD.TRANS_CONT_LCL_FK in ( " + TransContLclPK + ")");
                    strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                    strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
                }
            }
            //'
            if ((sRFQPK != null))
            {
                if ((strquery.Length != 0 & sRFQPK.Length != 0 & Convert.ToInt32(SRFQ_FLAG) == 1))
                {
                    strquery.Append("           UNION  ");
                }
            }
            if ((sRFQPK != null))
            {
                if ((sRFQPK.Length != 0 & Convert.ToInt32(SRFQ_FLAG) == 1))
                {
                    strquery.Append("            SELECT   ");
                    strquery.Append("            '' Icon,");
                    strquery.Append("            AFD.FILE_NAME,");
                    strquery.Append("            AFD.FILE_TYPE,");
                    strquery.Append("            AFD.FILE_SIZE,");
                    strquery.Append("           USR.USER_NAME,");
                    strquery.Append("            AFD.Created_Dt,");
                    strquery.Append("           MU.USER_NAME Modified_by,");
                    strquery.Append("           AFD.Modified_Dt,");
                    strquery.Append("            'false' Chk,");
                    strquery.Append("            AFD.file_path FILE_PATH,");
                    strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                    strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                    strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                    strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                    strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                    strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                    strquery.Append("            AFD.AIRLINE_FK Airpk,");
                    strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                    strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                    strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK ,AFD.TRANSPORT_NOTEPK ,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                    strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                    strquery.Append("            WHERE");
                    strquery.Append("            AFD.RFQ_SEA_FK in ( " + sRFQPK + ")");
                    strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                    strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
                }
            }
            //Airrfqpk
            if ((Airrfqpk != null))
            {
                if ((strquery.Length != 0 & Airrfqpk.Length != 0 & Convert.ToInt32(SRFQ_FLAG) == 1))
                {
                    strquery.Append("           UNION  ");
                }
            }
            if ((Airrfqpk != null))
            {
                if ((Airrfqpk.Length != 0 & Convert.ToInt32(SRFQ_FLAG) == 1))
                {
                    strquery.Append("            SELECT   ");
                    strquery.Append("            '' Icon,");
                    strquery.Append("            AFD.FILE_NAME,");
                    strquery.Append("            AFD.FILE_TYPE,");
                    strquery.Append("            AFD.FILE_SIZE,");
                    strquery.Append("           USR.USER_NAME,");
                    strquery.Append("            AFD.Created_Dt,");
                    strquery.Append("           MU.USER_NAME Modified_by,");
                    strquery.Append("           AFD.Modified_Dt,");
                    strquery.Append("            'false' Chk,");
                    strquery.Append("            AFD.file_path FILE_PATH,");
                    strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                    strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                    strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                    strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                    strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                    strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                    strquery.Append("            AFD.AIRLINE_FK Airpk,");
                    strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                    strquery.Append("            AFD.Announcement_Fk Announcementpk, ");
                    strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK ,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                    strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                    strquery.Append("            WHERE");
                    strquery.Append("            AFD.RFQ_AIR_FK in ( " + Airrfqpk + ")");
                    strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                    strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
                }
            }
            if ((ShipContPk != null))
            {
                if ((strquery.Length != 0 & ShipContPk.Length != 0 & Convert.ToInt32(CONTRACT_FLAG) == 1))
                {
                    strquery.Append("           UNION  ");
                }
            }
            if ((ShipContPk != null))
            {
                if ((ShipContPk.Length != 0 & Convert.ToInt32(CONTRACT_FLAG) == 1))
                {
                    strquery.Append("            SELECT   ");
                    strquery.Append("            '' Icon,");
                    strquery.Append("            AFD.FILE_NAME,");
                    strquery.Append("            AFD.FILE_TYPE,");
                    strquery.Append("            AFD.FILE_SIZE,");
                    strquery.Append("           USR.USER_NAME,");
                    strquery.Append("            AFD.Created_Dt,");
                    strquery.Append("           MU.USER_NAME Modified_by,");
                    strquery.Append("           AFD.Modified_Dt,");
                    strquery.Append("            'false' Chk,");
                    strquery.Append("            AFD.file_path FILE_PATH,");
                    strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                    strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                    strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                    strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                    strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                    strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                    strquery.Append("            AFD.AIRLINE_FK Airpk,");
                    strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                    strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                    strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                    strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                    strquery.Append("            WHERE");
                    strquery.Append("            AFD.CONT_MAIN_SEA_FK in ( " + ShipContPk + ")");
                    strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                    strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
                }
            }
            if ((Announcementpk != null))
            {
                if ((strquery.Length != 0 & Announcementpk.Length != 0 & Convert.ToInt32(ANN_FLAG) == 1))
                {
                    strquery.Append("           UNION  ");
                }
            }
            if ((Announcementpk != null))
            {
                if ((Announcementpk.Length != 0 & Convert.ToInt32(ANN_FLAG) == 1))
                {
                    strquery.Append("            SELECT   ");
                    strquery.Append("            '' Icon,");
                    strquery.Append("            AFD.FILE_NAME,");
                    strquery.Append("            AFD.FILE_TYPE,");
                    strquery.Append("            AFD.FILE_SIZE,");
                    strquery.Append("           USR.USER_NAME,");
                    strquery.Append("            AFD.Created_Dt,");
                    strquery.Append("           MU.USER_NAME Modified_by,");
                    strquery.Append("           AFD.Modified_Dt,");
                    strquery.Append("            'false' Chk,");
                    strquery.Append("            AFD.file_path FILE_PATH,");
                    strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                    strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                    strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                    strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                    strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                    strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                    strquery.Append("            AFD.AIRLINE_FK Airpk,");
                    strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                    strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                    strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                    //end
                    strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                    strquery.Append("            WHERE");
                    strquery.Append("            AFD.Announcement_Fk in ( " + Announcementpk + ")");
                    strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                    strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
                }
            }
            if ((Airlinepk != null))
            {
                if ((strquery.Length != 0 & Airlinepk.Length != 0 & Convert.ToInt32(CONTRACT_FLAG) == 1))
                {
                    strquery.Append("           UNION  ");
                }
            }
            if ((Airlinepk != null))
            {
                if ((Airlinepk.Length != 0 & Convert.ToInt32(CONTRACT_FLAG) == 1))
                {
                    strquery.Append("            SELECT   ");
                    strquery.Append("            '' Icon,");
                    strquery.Append("            AFD.FILE_NAME,");
                    strquery.Append("            AFD.FILE_TYPE,");
                    strquery.Append("            AFD.FILE_SIZE,");
                    strquery.Append("           USR.USER_NAME,");
                    strquery.Append("            AFD.Created_Dt,");
                    strquery.Append("           MU.USER_NAME Modified_by,");
                    strquery.Append("           AFD.Modified_Dt,");
                    strquery.Append("            'false' Chk,");
                    strquery.Append("            AFD.file_path FILE_PATH,");
                    strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                    strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                    strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                    strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                    strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                    strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                    strquery.Append("            AFD.AIRLINE_FK Airpk,");
                    strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                    strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                    strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK,AFD.TRANSPORT_NOTEPK ,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                    strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                    strquery.Append("            WHERE");
                    strquery.Append("            AFD.Airline_Fk in ( " + Airlinepk + ")");
                    strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                    strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
                }
            }
            //'CRO
            if ((CROPK != null))
            {
                if ((strquery.Length != 0 & CROPK.Length != 0 & Convert.ToInt32(CRO_FLAG) == 1))
                {
                    strquery.Append("           UNION  ");
                }
            }
            if ((CROPK != null))
            {
                if ((CROPK.Length != 0 & Convert.ToInt32(CRO_FLAG) == 1))
                {
                    strquery.Append("            SELECT   ");
                    strquery.Append("            '' Icon,");
                    strquery.Append("            AFD.FILE_NAME,");
                    strquery.Append("            AFD.FILE_TYPE,");
                    strquery.Append("            AFD.FILE_SIZE,");
                    strquery.Append("           USR.USER_NAME,");
                    strquery.Append("            AFD.Created_Dt,");
                    strquery.Append("           MU.USER_NAME Modified_by,");
                    strquery.Append("           AFD.Modified_Dt,");
                    strquery.Append("            'false' Chk,");
                    strquery.Append("            AFD.file_path FILE_PATH,");
                    strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                    strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                    strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                    strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                    strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                    strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                    strquery.Append("            AFD.AIRLINE_FK Airpk,");
                    strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                    strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                    strquery.Append("            AFD.SALES_CALL_FK,");
                    strquery.Append("            AFD.CRO_FK,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                    strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                    strquery.Append("            WHERE");
                    strquery.Append("            AFD.CRO_FK in ( " + CROPK + ")");
                    strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                    strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
                }
            }
            //'
            if ((CBJCPK != null))
            {
                if ((strquery.Length != 0 & CBJCPK.Length != 0 & Convert.ToInt32(CBJC_FLAG) == 1))
                {
                    strquery.Append("           UNION  ");
                }
            }
            if ((CBJCPK != null))
            {
                if ((CBJCPK.Length != 0 & Convert.ToInt32(CBJC_FLAG) == 1))
                {
                    strquery.Append("            SELECT   ");
                    strquery.Append("            '' Icon,");
                    strquery.Append("            AFD.FILE_NAME,");
                    strquery.Append("            AFD.FILE_TYPE,");
                    strquery.Append("            AFD.FILE_SIZE,");
                    strquery.Append("           USR.USER_NAME,");
                    strquery.Append("            AFD.Created_Dt,");
                    strquery.Append("           MU.USER_NAME Modified_by,");
                    strquery.Append("           AFD.Modified_Dt,");
                    strquery.Append("            'false' Chk,");
                    strquery.Append("            AFD.file_path FILE_PATH,");
                    strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                    strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                    strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                    strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                    strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                    strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                    strquery.Append("            AFD.AIRLINE_FK Airpk,");
                    strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                    strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                    strquery.Append("            AFD.SALES_CALL_FK,");
                    strquery.Append("            AFD.CRO_FK,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK, AFD.CBJC_PK ");
                    strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                    strquery.Append("            WHERE");
                    strquery.Append("            AFD.CBJC_PK in ( " + CBJCPK + ")");
                    strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                    strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
                }
            }
            //'
            //'TRASPORTNOTE Koteswari - PTS ID-JAN-033
            if ((TransportNotePK != null))
            {
                if ((strquery.Length != 0 & TransportNotePK.Length != 0 & Convert.ToInt32(TRANS_FLAG) == 1))
                {
                    strquery.Append("           UNION  ");
                }
            }
            if ((TransportNotePK != null) & Convert.ToInt32(TRANS_FLAG) == 1)
            {
                if ((TransportNotePK.Length != 0))
                {
                    strquery.Append("            SELECT   ");
                    strquery.Append("            '' Icon,");
                    strquery.Append("            AFD.FILE_NAME,");
                    strquery.Append("            AFD.FILE_TYPE,");
                    strquery.Append("            AFD.FILE_SIZE,");
                    strquery.Append("           USR.USER_NAME,");
                    strquery.Append("            AFD.Created_Dt,");
                    strquery.Append("           MU.USER_NAME Modified_by,");
                    strquery.Append("           AFD.Modified_Dt,");
                    strquery.Append("            'false' Chk,");
                    strquery.Append("            AFD.file_path FILE_PATH,");
                    strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                    strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                    strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                    strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                    strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                    strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                    strquery.Append("            AFD.AIRLINE_FK Airpk,");
                    strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                    strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                    strquery.Append("            AFD.SALES_CALL_FK,");
                    strquery.Append("            AFD.CRO_FK,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                    strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                    strquery.Append("            WHERE");
                    strquery.Append("            AFD.TRANSPORT_NOTEPK in ( " + TransportNotePK + ")");
                    strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                    strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);

                    if ((BCROFK != 0))
                    {
                        strquery.Append("           UNION  ");
                        strquery.Append("            SELECT   ");
                        strquery.Append("            '' Icon,");
                        strquery.Append("            AFD.FILE_NAME,");
                        strquery.Append("            AFD.FILE_TYPE,");
                        strquery.Append("            AFD.FILE_SIZE,");
                        strquery.Append("           USR.USER_NAME,");
                        strquery.Append("            AFD.Created_Dt,");
                        strquery.Append("           MU.USER_NAME Modified_by,");
                        strquery.Append("           AFD.Modified_Dt,");
                        strquery.Append("            'false' Chk,");
                        strquery.Append("            AFD.file_path FILE_PATH,");
                        strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                        strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                        strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                        strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                        strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                        strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                        strquery.Append("            AFD.AIRLINE_FK Airpk,");
                        strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                        strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                        strquery.Append("            AFD.SALES_CALL_FK,");
                        strquery.Append("            AFD.CRO_FK,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                        strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                        strquery.Append("            WHERE");
                        strquery.Append("            AFD.CRO_FK in ( " + BCROFK + ")");
                        strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                        strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
                    }
                }
                else if ((BCROFK != 0))
                {
                    strquery.Append("            SELECT   ");
                    strquery.Append("            '' Icon,");
                    strquery.Append("            AFD.FILE_NAME,");
                    strquery.Append("            AFD.FILE_TYPE,");
                    strquery.Append("            AFD.FILE_SIZE,");
                    strquery.Append("           USR.USER_NAME,");
                    strquery.Append("            AFD.Created_Dt,");
                    strquery.Append("           MU.USER_NAME Modified_by,");
                    strquery.Append("           AFD.Modified_Dt,");
                    strquery.Append("            'false' Chk,");
                    strquery.Append("            AFD.file_path FILE_PATH,");
                    strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                    strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                    strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                    strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                    strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                    strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                    strquery.Append("            AFD.AIRLINE_FK Airpk,");
                    strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                    strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                    strquery.Append("            AFD.SALES_CALL_FK,");
                    strquery.Append("            AFD.CRO_FK,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                    strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                    strquery.Append("            WHERE");
                    strquery.Append("            AFD.CRO_FK in ( " + BCROFK + ")");
                    strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                    strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
                }
            }
            //'End
            if (strquery.Length == 0)
            {
                strquery.Append("            SELECT   ");
                strquery.Append("            '' Icon,");
                strquery.Append("            AFD.FILE_NAME,");
                strquery.Append("            AFD.FILE_TYPE,");
                strquery.Append("            AFD.FILE_SIZE,");
                strquery.Append("           USR.USER_NAME,");
                strquery.Append("            AFD.Created_Dt,");
                strquery.Append("           MU.USER_NAME Modified_by,");
                strquery.Append("           AFD.Modified_Dt,");
                strquery.Append("            'false' Chk,");
                strquery.Append("            AFD.file_path FILE_PATH,");
                strquery.Append("            AFD.ENQUIRY_MST_FK EN_FK,");
                strquery.Append("            AFD.QUOTATION_MST_FK QU_FK,");
                strquery.Append("            AFD.BOOKING_TRN_FK BO_FK,");
                strquery.Append("            AFD.SURVEY_MST_FK SURPK,");
                strquery.Append("            AFD.RFQ_SEA_FK SRFQPK,");
                strquery.Append("            AFD.CONT_MAIN_SEA_FK CONTPK,");
                strquery.Append("            AFD.AIRLINE_FK Airpk,");
                strquery.Append("            AFD.RFQ_AIR_FK Airrfqpk,");
                strquery.Append("            AFD.Announcement_Fk Announcementpk,");
                strquery.Append("            AFD.SALES_CALL_FK,AFD.CRO_FK,AFD.TRANSPORT_NOTEPK,AFD.TRANS_CONT_FCL_FK,AFD.TRANS_CONT_LCL_FK,AFD.CBJC_PK ");
                strquery.Append("            FROM ATTACH_FILE_DTL_TBL AFD,User_Mst_Tbl USR, USER_MST_TBL MU");
                strquery.Append("            WHERE 1=2");
                strquery.Append("            AND USR.USER_MST_PK(+)=AFD.CREATED_BY_FK  AND AFD.MODIFIED_BY_FK = MU.USER_MST_PK(+) ");
                strquery.Append("            AND AFD.BIZ_TYPE = " + Biztype);
            }

            strquery.Append("            ) QRY ) Q");
            strsubqry.Append(str);
            strsubqry.Append(strquery);
            try
            {
                return objWF.GetDataSet(strsubqry.ToString());
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

        public Int32 CntAttachments(Int32 FormPk = 0, string From = "", Int32 Biztype = 2, Int32 IntJcType = 2, int CROFK = 0)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                return (((DataSet)fetchattachfiles(FormPk, From, Biztype, IntJcType, CROFK)).Tables[0].Rows.Count);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public string RemoveFile(string file_name)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand delCommand = new OracleCommand();
            OracleTransaction insertTrans = null;
            objWK.OpenConnection();
            insertTrans = objWK.MyConnection.BeginTransaction();
            try
            {
                var _with1 = delCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".ATTACH_FILE_DTL_PKG.ATTACH_FILE_DTL_DEL";
                _with1.Parameters.Clear();
                var _with2 = _with1.Parameters;
                //.Add("EN_FK_IN", EN_FK).Direction = ParameterDirection.Input
                //.Add("QU_FK_IN", QU_FK).Direction = ParameterDirection.Input
                //.Add("BO_FK_IN", BO_FK).Direction = ParameterDirection.Input
                _with2.Add("FILE_NAME_IN", file_name).Direction = ParameterDirection.Input;
                var _with3 = objWK.MyDataAdapter;
                _with3.DeleteCommand = delCommand;
                _with3.DeleteCommand.Transaction = insertTrans;
                _with3.DeleteCommand.ExecuteNonQuery();
                insertTrans.Commit();
                //Return "All Data Saved Successfully"
                return "Record Deleted Successfully";
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                insertTrans.Rollback();
                return "No Records to save";
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        //Function Pair
        //_____________________________________________________________________________________________________________________________________________________________________________________________________________
        public object SaveFile(Int32 EN_FK, Int32 QU_FK, Int32 BO_FK, Int32 Jobpk, Int32 HBLpk, Int32 Biz_type, string[] filename, string[] filepath, Int32 SurPk = 0, Int32 SRfqPK = 0,
        Int32 ShipContPk = 0, Int32 Import_Jobpk = 0, Int32 Canpk = 0, Int32 DeliveryOrderpk = 0, Int32 Airlinepk = 0, Int32 AirSRfqPk = 0, Int32 Announcementpk = 0, Int32 SalesCallPK = 0, Int32 CROPk = 0, Int32 TrasportNotePK = 0,
        Int32 TransContFclPK = 0, Int32 TransContLclPK = 0, Int32 CBJC_PK = 0, Int32 MenuFK = 0, string FileLength = "", string FileType = "")
        {
            Int16 i = default(Int16);
            //The code in this function was previously here itself, now added in function: SaveIndividualFile(),
            //and call each time this function instead.
            //Code changed by Ashish Arya on 27th Sept 2011
            try
            {
                for (i = 0; i <= filename.Length - 1; i++)
                {
                    if (!string.IsNullOrEmpty(filename[i].Trim()))
                    {
                        if (SaveIndividualFile(EN_FK, QU_FK, BO_FK, Jobpk, HBLpk, Biz_type, filename[i], filepath[i], FileLength, FileType,
                        SurPk, SRfqPK, ShipContPk, Import_Jobpk, Canpk, DeliveryOrderpk, Airlinepk, AirSRfqPk, Announcementpk, SalesCallPK,
                        CROPk, TrasportNotePK, TransContFclPK, TransContLclPK, CBJC_PK, MenuFK))
                        {
                            arrMessage.Add(filename[i] + " Saved!");
                        }
                        else
                        {
                            arrMessage.Add(filename[i] + " already exist!");
                        }
                    }
                }
                //arrMessage.Add("All Data Saved !")
                return arrMessage;
                //End If
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
            }
        }

        //Function Added by Ashish Arya on 27th Sept 2011 for single file save in database
        public bool SaveIndividualFile(Int32 EN_FK, Int32 QU_FK, Int32 BO_FK, Int32 Jobpk, Int32 HBLpk, Int32 Biz_type, string filename, string filepath, string FileLength, string FileType,
        Int32 SurPk = 0, Int32 SRfqPK = 0, Int32 ShipContPk = 0, Int32 Import_Jobpk = 0, Int32 Canpk = 0, Int32 DeliveryOrderpk = 0, Int32 Airlinepk = 0, Int32 AirSRfqPk = 0, Int32 Announcementpk = 0, Int32 SalesCallPK = 0,
        Int32 CROPk = 0, Int32 TrasportNotePK = 0, Int32 TransContFclPK = 0, Int32 TransContLclPK = 0, Int32 CBJC_PK = 0, Int32 MenuFK = 0)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand insCommand = new OracleCommand();
            OracleTransaction insertTrans = null;
            string path = "..\\";

            var strNull = DBNull.Value;
            objWK.OpenConnection();
            insertTrans = objWK.MyConnection.BeginTransaction();
            try
            {
                if (!string.IsNullOrEmpty(filename.Trim()))
                {
                    var _with4 = insCommand;
                    _with4.Connection = objWK.MyConnection;
                    _with4.CommandType = CommandType.StoredProcedure;
                    _with4.CommandText = objWK.MyUserName + ".ATTACH_FILE_DTL_PKG.ATTACH_FILE_DTL_INS";
                    _with4.Parameters.Clear();
                    var _with5 = _with4.Parameters;
                    _with5.Add("EN_FK_IN", getDefault(EN_FK, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("QU_FK_IN", getDefault(QU_FK, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("BO_FK_IN", getDefault(BO_FK, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("JOB_FK_IN", getDefault(Jobpk, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("HBL_FK_IN", getDefault(HBLpk, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("SUR_FK_IN", getDefault(SurPk, 0)).Direction = ParameterDirection.Input;
                    //added by surya prasad on -jan-2009
                    _with5.Add("SRFQ_FK_IN", getDefault(SRfqPK, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("AIRSRFQ_FK_IN", getDefault(AirSRfqPk, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("MENU_MST_FK_IN", getDefault(MenuFK, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("ANNOUNCEMENT_FK_IN", getDefault(Announcementpk, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("CRO_FK_IN", getDefault(CROPk, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("TRANSPORTNOTE_FK_IN", getDefault(TrasportNotePK, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("TRANS_CONT_FCL_FK_IN", getDefault(TransContFclPK, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("TRANS_CONT_LCL_FK_IN", getDefault(TransContLclPK, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("CBJC_FK_IN", getDefault(CBJC_PK, 0)).Direction = ParameterDirection.Input;

                    //added by surya prasad on 17-jun-2009
                    _with5.Add("SHIPCONT_FK_IN", getDefault(ShipContPk, 0)).Direction = ParameterDirection.Input;
                    //added by Sivachandran on 17-jun-2009
                    _with5.Add("FILE_NAME_IN", filename).Direction = ParameterDirection.Input;
                    _with5.Add("FILE_Type_IN", FileType).Direction = ParameterDirection.Input;
                    _with5.Add("FILE_SIZE_IN", FileLength).Direction = ParameterDirection.Input;
                    _with5.Add("DELIVERY_ORDER_FK_IN", getDefault(DeliveryOrderpk, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("AIRLINE_FK_IN", getDefault(Airlinepk, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("JOB_CARD_SEA_IMP_FK_IN", getDefault(Import_Jobpk, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("SALES_CALL_FK_IN", getDefault(SalesCallPK, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("CAN_FK_IN", getDefault(Canpk, 0)).Direction = ParameterDirection.Input;
                    _with5.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with5.Add("FILE_PATH_IN", filepath).Direction = ParameterDirection.Input;
                    _with5.Add("BIZ_TYPES", Biz_type).Direction = ParameterDirection.Input;
                    var _with6 = objWK.MyDataAdapter;
                    _with6.InsertCommand = insCommand;
                    _with6.InsertCommand.Transaction = insertTrans;
                    _with6.InsertCommand.ExecuteNonQuery();
                }
                //If arrMessage.Count > 0 Then
                //If InStr(arrMessage(0), "Successfully") Then
                //arrMessage.Add(filename & " Saved Successfully!")
                insertTrans.Commit();
                return true;
            }
            catch (Exception ex)
            {
                insertTrans.Rollback();
                arrMessage.Add(ex.Message);
                //Return arrMessage
            }
            finally
            {
                objWK.CloseConnection();
            }
            return false;
        }

        public object SaveFile(int FormPK, string FormValue, Int32 EN_FK, Int32 QU_FK, Int32 BO_FK, Int32 Jobpk, Int32 HBLpk, Int32 Biz_type, string[] filename, string[] filepath,
        string[] FileLength, string[] FileType, Int32 SurPk = 0, Int32 SRfqPK = 0, Int32 ShipContPk = 0, Int32 Import_Jobpk = 0, Int32 Canpk = 0, Int32 DeliveryOrderpk = 0, Int32 Airlinepk = 0, Int32 AirSRfqPk = 0,
        Int32 Announcementpk = 0, Int32 SalesCallPK = 0, Int32 CROPk = 0, Int32 TrasportNotePK = 0, Int32 TransContFclPK = 0, Int32 TransContLclPK = 0, Int32 CBJC_PK = 0, Int32 MenuFK = 0)
        {
            Int16 i = default(Int16);
            //The code in this function was previously here itself, now added in function: SaveIndividualFile(),
            //and call each time this function instead.
            //Code changed by Ashish Arya on 27th Sept 2011
            try
            {
                for (i = 0; i <= filename.Length - 1; i++)
                {
                    if (!string.IsNullOrEmpty(filename[i].Trim()))
                    {
                        if (SaveIndividualFile(FormPK, FormValue, EN_FK, QU_FK, BO_FK, Jobpk, HBLpk, Biz_type, filename[i], filepath[i],
                        FileLength[i], FileType[i], SurPk, SRfqPK, ShipContPk, Import_Jobpk, Canpk, DeliveryOrderpk, Airlinepk, AirSRfqPk,
                        Announcementpk, SalesCallPK, CROPk, TrasportNotePK, TransContFclPK, TransContLclPK, CBJC_PK, MenuFK))
                        {
                            arrMessage.Add(filename[i] + " Saved!");
                        }
                        else
                        {
                            arrMessage.Add(filename[i] + " overwrite Sucessfully!");
                        }
                    }
                }
                //arrMessage.Add("All Data Saved")
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
            }
        }

        public bool SaveIndividualFile(int FormPK, string FormValue, Int32 EN_FK, Int32 QU_FK, Int32 BO_FK, Int32 Jobpk, Int32 HBLpk, Int32 Biz_type, string filename, string filepath,
        string FileLength, string FileType, Int32 SurPk = 0, Int32 SRfqPK = 0, Int32 ShipContPk = 0, Int32 Import_Jobpk = 0, Int32 Canpk = 0, Int32 DeliveryOrderpk = 0, Int32 Airlinepk = 0, Int32 AirSRfqPk = 0,
        Int32 Announcementpk = 0, Int32 SalesCallPK = 0, Int32 CROPk = 0, Int32 TrasportNotePK = 0, Int32 TransContFclPK = 0, Int32 TransContLclPK = 0, Int32 CBJC_PK = 0, Int32 MenuFk = 0)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            if (IfFileExistInDB(filename, FormValue, FormPK))
            {
                int PK = FormPK;

                sb.Append("UPDATE ATTACH_FILE_DTL_TBL ATTACH SET ATTACH.FILE_SIZE='" + FileLength + "', ATTACH.File_Path='" + filepath + "', ATTACH.MENU_MST_FK='" + MenuFk + "', ATTACH.File_Type='" + FileType + "', ATTACH.MODIFIED_BY_FK = '" + HttpContext.Current.Session["USER_PK"] + "', ATTACH.MODIFIED_DT= SYSDATE WHERE UPPER(ATTACH.FILE_NAME) = '" + filename.ToUpper() + "' ");

                switch (FormValue)
                {
                    case "Quo":
                        sb.Append(" and attach.QUOTATION_MST_FK = " + PK + "");
                        break;

                    case "JOB":
                        sb.Append(" and attach.JOB_TRN_FK = " + PK + "");
                        break;

                    case "SALES_CALL":
                        sb.Append(" and attach.SALES_CALL_FK = " + PK + "");
                        break;

                    case "Bkg":
                        sb.Append(" and attach.BOOKING_TRN_FK = " + PK + "");
                        break;

                    case "HBL":
                        sb.Append(" and attach.HBL_TRN_FK = " + PK + "");
                        break;

                    case "Removals":
                        sb.Append(" and attach.SURVEY_MST_FK = " + PK + "");
                        break;

                    case "SRFQ":
                        sb.Append(" and attach.RFQ_SEA_FK = " + PK + "");
                        break;

                    case "SHIPCONT":
                        sb.Append(" and attach.CONT_MAIN_SEA_FK = " + PK + "");
                        break;

                    case "Airline":
                        sb.Append(" and attach.AIRLINE_FK = " + PK + "");
                        break;

                    case "AIRSRFQ":
                        sb.Append(" and attach.RFQ_AIR_FK = " + PK + "");
                        break;

                    case "IMPORTJOB":
                        sb.Append(" and attach.JOB_CARD_TRN_FK = " + PK + "");
                        break;

                    case "CAN":
                        sb.Append(" and attach.CAN_FK = " + PK + "");
                        break;

                    case "DO":
                        sb.Append(" and attach.DELIVERY_ORDER_FK = " + PK + "");
                        break;

                    case "Announcement":
                        sb.Append(" and attach.Announcement_Fk = " + PK + "");
                        break;

                    case "CRO":
                        sb.Append(" and attach.CRO_FK = " + PK + "");
                        break;

                    case "TRASPORTNOTE":
                        sb.Append(" and attach.TRANSPORT_NOTEPK = " + PK + "");
                        break;

                    case "TRANSCONTFCL":
                        sb.Append(" and attach.TRANS_CONT_FCL_FK = " + PK + "");
                        break;

                    case "TRANSCONTLCL":
                        sb.Append(" and attach.TRANS_CONT_LCL_FK = " + PK + "");
                        break;

                    case "CBJC":
                        sb.Append(" and attach.CBJC_PK = " + PK + "");
                        break;
                }
                if (Convert.ToInt32(objWF.ExecuteScaler(sb.ToString())) > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                SaveIndividualFile(EN_FK, QU_FK, BO_FK, Jobpk, HBLpk, Biz_type, filename, filepath, FileLength, FileType,
                SurPk, SRfqPK, ShipContPk, Import_Jobpk, Canpk, DeliveryOrderpk, Airlinepk, AirSRfqPk, Announcementpk, SalesCallPK,
                CROPk, TrasportNotePK, TransContFclPK, TransContLclPK, CBJC_PK, MenuFk);
                return true;
            }
        }

        //_____________________________________________________________________________________________________________________________________________________________________________________________________________
        //This function is added by Ashish Arya on 27th Sept 2011 to check the availability of a file in database
        public bool IfFileExistInDB(string FileName, string FormValue, Int64 FormPK)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            int PK = Convert.ToInt32(FormPK);
            strsql.Append("select count(*) from attach_file_dtl_tbl attach where Upper(attach.file_name) = '" + FileName.ToUpper() + "' ");
            try
            {
                switch (FormValue)
                {
                    case "Quo":
                        strsql.Append(" and attach.QUOTATION_MST_FK = " + PK + "");
                        break;

                    case "JOB":
                        strsql.Append(" and attach.JOB_TRN_FK = " + PK + "");
                        break;

                    case "Bkg":
                        strsql.Append(" and attach.BOOKING_TRN_FK = " + PK + "");
                        break;

                    case "Enq":
                        strsql.Append(" and attach.enquiry_mst_fk = " + PK + "");
                        break;

                    case "HBL":
                        strsql.Append(" and attach.HBL_TRN_FK = " + PK + "");
                        break;

                    case "Removals":
                        strsql.Append(" and attach.SURVEY_MST_FK = " + PK + "");
                        break;

                    case "SRFQ":
                        strsql.Append(" and attach.RFQ_SEA_FK = " + PK + "");
                        break;

                    case "AIRSRFQ":
                        strsql.Append(" and attach.RFQ_AIR_FK = " + PK + "");
                        break;

                    case "SHIPCONT":
                        strsql.Append(" and attach.CONT_MAIN_SEA_FK = " + PK + "");
                        break;

                    case "Airline":
                        strsql.Append(" and attach.AIRLINE_FK = " + PK + "");
                        break;

                    case "IMPORTJOB":
                        strsql.Append(" and attach.JOB_CARD_TRN_FK = " + PK + "");
                        break;

                    case "CAN":
                        strsql.Append(" and attach.CAN_FK = " + PK + "");
                        break;

                    case "DO":
                        strsql.Append(" and attach.DELIVERY_ORDER_FK = " + PK + "");
                        break;

                    case "Announcement":
                        strsql.Append(" and attach.Announcement_Fk = " + PK + "");
                        break;

                    case "SALES_CALL":
                        strsql.Append(" and attach.SALES_CALL_FK = " + PK + "");
                        break;

                    case "CRO":
                        strsql.Append(" and attach.CRO_FK = " + PK + "");
                        break;

                    case "TRASPORTNOTE":
                        strsql.Append(" and attach.TRANSPORT_NOTEPK = " + PK + "");
                        break;

                    case "TRANSCONTFCL":
                        strsql.Append(" and attach.TRANS_CONT_FCL_FK = " + PK + "");
                        break;

                    case "TRANSCONTLCL":
                        strsql.Append(" and attach.TRANS_CONT_LCL_FK = " + PK + "");
                        break;

                    case "CBJC":
                        strsql.Append(" and attach.CBJC_PK = " + PK + "");
                        break;
                }

                if (Convert.ToInt32(objWF.ExecuteScaler(strsql.ToString())) > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (OracleException Oraexp)
            {
                //Throw Oraexp        ''Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
            }
            return false;
        }

        #endregion "File Attached"

        #region "Getting File Extension"

        public object FetchfileExtnDetails()
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append(" SELECT AIMT.FILE_TYPE ,");
            sb.Append(" AIMT.FILE_ICON_PATH ,");
            sb.Append(" AIMT.EXTENSION ");
            sb.Append(" FROM ATTACHMENT_ICON_MST_TBL AIMT");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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

        #endregion "Getting File Extension"
    }
}