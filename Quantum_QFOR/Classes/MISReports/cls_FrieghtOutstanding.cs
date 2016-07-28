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
using Oracle.ManagedDataAccess.Types;
using System;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    public class cls_FrieghtOutstanding : CommonFeatures
    {

        string type;
        #region "Getr DataSet"

        public DataSet GetDataset_CusPrint(string CustomerPK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            try
            {
                strSQL = "SELECT CMT.CUSTOMER_ID,";
                strSQL = strSQL + " CMT.CUSTOMER_MST_PK,";
                strSQL = strSQL + "CMT.CUSTOMER_NAME,";
                strSQL = strSQL + "CMT.ACTIVE_FLAG,";
                strSQL = strSQL + "CMT.CREDIT_LIMIT,";
                strSQL = strSQL + "CMT.CREDIT_DAYS,";
                strSQL = strSQL + "CMT.SECURITY_CHK_REQD,";
                strSQL = strSQL + "CMT.VAT_NO,";
                strSQL = strSQL + "CMT.ACCOUNT_NO,";
                strSQL = strSQL + "CUSTOMER_TYPE_FK,";
                strSQL = strSQL + "CCD.ADM_ADDRESS_1,";
                strSQL = strSQL + "CCD.ADM_ADDRESS_2,";
                strSQL = strSQL + "CCD.ADM_ADDRESS_3,";
                strSQL = strSQL + "CCD.ADM_CITY,";
                strSQL = strSQL + "CCD.ADM_ZIP_CODE,";
                strSQL = strSQL + "CCD.ADM_LOCATION_MST_FK,";
                strSQL = strSQL + "CCD.ADM_CONTACT_PERSON,";
                strSQL = strSQL + "CCD.ADM_PHONE_NO_1,";
                strSQL = strSQL + "CCD.ADM_PHONE_NO_2,";
                strSQL = strSQL + "CCD.ADM_FAX_NO,";
                strSQL = strSQL + "CCD.ADM_EMAIL_ID,";
                strSQL = strSQL + "CCD.ADM_URL,";
                strSQL = strSQL + "CCD.ADM_COUNTRY_MST_FK,";
                strSQL = strSQL + "CCD.COR_ADDRESS_1,";
                strSQL = strSQL + "CCD.COR_ADDRESS_2,";
                strSQL = strSQL + "CCD.COR_ADDRESS_3,";
                strSQL = strSQL + "CCD.COR_CITY,";
                strSQL = strSQL + "CCD.COR_ZIP_CODE,";
                strSQL = strSQL + "CCD.COR_LOCATION_MST_FK,";
                strSQL = strSQL + "CCD.COR_CONTACT_PERSON,";
                strSQL = strSQL + "CCD.COR_PHONE_NO_1,";
                strSQL = strSQL + "CCD.COR_PHONE_NO_2,";
                strSQL = strSQL + "CCD.COR_FAX_NO,";
                strSQL = strSQL + "CCD.COR_EMAIL_ID,";
                strSQL = strSQL + "CCD.COR_URL,";
                strSQL = strSQL + "CCD.COR_COUNTRY_MST_FK,";
                strSQL = strSQL + "CCD.BILL_ADDRESS_1,";
                strSQL = strSQL + "CCD.BILL_ADDRESS_2,";
                strSQL = strSQL + "CCD.BILL_ADDRESS_3,";
                strSQL = strSQL + "CCD.BILL_CITY,";
                strSQL = strSQL + "CCD.BILL_ZIP_CODE,";
                strSQL = strSQL + "CCD.BILL_LOCATION_MST_FK,";
                strSQL = strSQL + "CCD.BILL_CONTACT_PERSON,";
                strSQL = strSQL + "CCD.BILL_PHONE_NO_1,";
                strSQL = strSQL + "CCD.BILL_PHONE_NO_2,";
                strSQL = strSQL + "CCD.BILL_FAX_NO,";
                strSQL = strSQL + "CCD.BILL_EMAIL_ID,";
                strSQL = strSQL + "CCD.BILL_URL,";
                strSQL = strSQL + "CCD.ADM_SHORT_NAME,";
                strSQL = strSQL + "CCD.COR_SHORT_NAME,";
                strSQL = strSQL + "CCD.BILL_SHORT_NAME,";
                strSQL = strSQL + "CCD.BILL_COUNTRY_MST_FK,";
                strSQL = strSQL + "CMT.VERSION_NO,";
                strSQL = strSQL + "CCD.ADM_SALUTATION,";
                strSQL = strSQL + "CCD.COR_SALUTATION,";
                strSQL = strSQL + "CCD.BILL_SALUTATION,";
                strSQL = strSQL + "CMT.REP_EMP_MST_FK,";
                strSQL = strSQL + "EMP.EMPLOYEE_NAME,";
                strSQL = strSQL + "CMT.DEFERMENT_NO,";
                strSQL = strSQL + "CMT.TURN_NO,";
                strSQL = strSQL + "CMT.DP_AGENT_MST_FK,";
                strSQL = strSQL + "AG.AGENT_ID,";
                strSQL = strSQL + "AG.AGENT_NAME,";
                strSQL = strSQL + "CONA.COUNTRY_ID COUNTRYAID,";
                strSQL = strSQL + "CONA.COUNTRY_NAME COUNTRYANAME,";
                strSQL = strSQL + "CONB.COUNTRY_ID COUNTRYBID,";
                strSQL = strSQL + "CONB.COUNTRY_NAME COUNTRYBNAME,";
                strSQL = strSQL + "CONC.COUNTRY_ID COUNTRYCID,";
                strSQL = strSQL + "CONC.COUNTRY_NAME COUNTRYCNAME,";
                strSQL = strSQL + "LOCA.LOCATION_ID LOCAID,";
                strSQL = strSQL + "LOCA.LOCATION_NAME LOCANAME,";
                strSQL = strSQL + "LOCB.LOCATION_ID LOCBID,";
                strSQL = strSQL + "LOCB.LOCATION_NAME LOCBNAME,";
                strSQL = strSQL + "LOCC.LOCATION_ID LOCCID,";
                strSQL = strSQL + "LOCC.LOCATION_NAME LOCNAME,";
                strSQL = strSQL + "CMT.COL_ADDRESS,";
                strSQL = strSQL + "CMT.DEL_ADDRESS,";
                strSQL = strSQL + "CMT.REMARKS,";
                strSQL = strSQL + "CMT.Cust_Reg_No,";
                strSQL = strSQL + "cmt.temp_party,";
                strSQL = strSQL + "cmt.credit_limit_used";
                strSQL = strSQL + "FROM CUSTOMER_MST_TBL      CMT,";
                strSQL = strSQL + "CUSTOMER_CONTACT_DTLS CCD,";
                strSQL = strSQL + "EMPLOYEE_MST_TBL      EMP,";
                strSQL = strSQL + "AGENT_MST_TBL         AG,";
                strSQL = strSQL + "COUNTRY_MST_TBL       CONA,";
                strSQL = strSQL + "COUNTRY_MST_TBL       CONB,";
                strSQL = strSQL + "COUNTRY_MST_TBL       CONC,";
                strSQL = strSQL + "LOCATION_MST_TBL      LOCA,";
                strSQL = strSQL + "LOCATION_MST_TBL      LOCB,";
                strSQL = strSQL + "LOCATION_MST_TBL LOCC";
                strSQL = strSQL + "WHERE(CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK)";
                strSQL = strSQL + "AND CMT.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                strSQL = strSQL + "AND CMT.DP_AGENT_MST_FK = AG.AGENT_MST_PK(+)";
                strSQL = strSQL + "AND CCD.ADM_COUNTRY_MST_FK = CONA.COUNTRY_MST_PK";
                strSQL = strSQL + "AND CCD.BILL_COUNTRY_MST_FK = CONB.COUNTRY_MST_PK(+)";
                strSQL = strSQL + "AND CCD.COR_COUNTRY_MST_FK = CONC.COUNTRY_MST_PK(+)";
                strSQL = strSQL + "AND CCD.ADM_LOCATION_MST_FK = LOCA.LOCATION_MST_PK(+)";
                strSQL = strSQL + "AND CCD.BILL_LOCATION_MST_FK = LOCB.LOCATION_MST_PK(+)";
                strSQL = strSQL + "AND CCD.COR_LOCATION_MST_FK = LOCC.LOCATION_MST_PK(+)";

                if ((!string.IsNullOrEmpty(CustomerPK)))
                {
                    strSQL = strSQL + "AND CMT.CUSTOMER_MST_PK IN (" + CustomerPK + ")";
                }

                dsAll = objWF.GetDataSet(strSQL);
                return dsAll;
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

        #region "Get Invoice"

        public DataSet getInvData()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            try
            {
                strSQL = "select distinct(CUSTOMER_NAME) CUSTOMER_NAME,INVOICE,COLLECTION,OUTSTANDING from INVOICE_TEMP_TBL";
                dsAll = objWF.GetDataSet(strSQL);
                return dsAll;
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

        #region "Fetch List"

        public DataSet FetchForCustProfile(string FROM_DATE = "", string TO_DATE = "", string strCustomer = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long usrLocFK = 0, short BizType = 1, short process = 1, long LocValue = 0)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            DataSet dsTotalRecords = new DataSet();
            StringBuilder strBuilder = new StringBuilder();
            StringBuilder strCondition = new StringBuilder();
            StringBuilder strCondition1 = new StringBuilder();
            StringBuilder strCondition2 = new StringBuilder();
            StringBuilder strCondition3 = new StringBuilder();
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (string.IsNullOrEmpty(FROM_DATE.Trim()))
            {
                FROM_DATE = "01/01/1900";
            }
            if (string.IsNullOrEmpty(TO_DATE.Trim()))
            {
                TO_DATE = "01/01/2100";
            }
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);

            strCustomer = objWF.ExecuteScaler("select customer_mst_pk from CUSTOMER_MST_TBL where customer_name='" + strCustomer + "'");

            strCondition.Append(" select distinct(CMT.CUSTOMER_NAME) Customer,");
            strCondition.Append(" INV.NET_RECEIVABLE*get_ex_rate(inv.currency_mst_fk," + BaseCurrFk + ",inv.invoice_date) Invoice,");
            strCondition.Append("NVL(CTRN.recd_amount_hdr_curr,0)*GET_EX_RATE(COL.CURRENCY_MST_FK," + BaseCurrFk + ",COL.COLLECTIONS_DATE) Collection,");
            strCondition.Append("NVL((INV.NET_RECEIVABLE*get_ex_rate(inv.currency_mst_fk," + BaseCurrFk + ",inv.invoice_date) - NVL(CTRN.recd_amount_hdr_curr,0.00)*GET_EX_RATE(COL.CURRENCY_MST_FK," + BaseCurrFk + ",COL.COLLECTIONS_DATE)), 0) Balance,");

            if (BizType == 3 | process == 0)
            {
                strCondition1.Append(" select distinct(CMT.CUSTOMER_NAME) Customer,");
                strCondition1.Append(" INV.NET_RECEIVABLE*get_ex_rate(inv.currency_mst_fk," + BaseCurrFk + ",inv.invoice_date) Invoice,");
                strCondition1.Append("NVL(CTRN.recd_amount_hdr_curr,0)*GET_EX_RATE(COL.CURRENCY_MST_FK," + BaseCurrFk + ",COL.COLLECTIONS_DATE) Collection,");
                strCondition1.Append("NVL((INV.NET_RECEIVABLE*get_ex_rate(inv.currency_mst_fk," + BaseCurrFk + ",inv.invoice_date) - NVL(CTRN.recd_amount_hdr_curr,0.00)*GET_EX_RATE(COL.CURRENCY_MST_FK," + BaseCurrFk + ",COL.COLLECTIONS_DATE)), 0) Balance,");
                strCondition2.Append(" select distinct(CMT.CUSTOMER_NAME) Customer,");
                strCondition2.Append(" INV.NET_RECEIVABLE*get_ex_rate(inv.currency_mst_fk," + BaseCurrFk + ",inv.invoice_date) Invoice,");
                strCondition2.Append("NVL(CTRN.recd_amount_hdr_curr,0)*GET_EX_RATE(COL.CURRENCY_MST_FK," + BaseCurrFk + ",COL.COLLECTIONS_DATE) Collection,");
                strCondition2.Append("NVL((INV.NET_RECEIVABLE*get_ex_rate(inv.currency_mst_fk," + BaseCurrFk + ",inv.invoice_date) - NVL(CTRN.recd_amount_hdr_curr,0.00)*GET_EX_RATE(COL.CURRENCY_MST_FK," + BaseCurrFk + ",COL.COLLECTIONS_DATE)), 0) Balance,");
                strCondition3.Append(" select distinct(CMT.CUSTOMER_NAME) Customer,");
                strCondition3.Append(" INV.NET_RECEIVABLE*get_ex_rate(inv.currency_mst_fk," + BaseCurrFk + ",inv.invoice_date) Invoice,");
                strCondition3.Append("NVL(CTRN.recd_amount_hdr_curr,0)*GET_EX_RATE(COL.CURRENCY_MST_FK," + BaseCurrFk + ",COL.COLLECTIONS_DATE) Collection,");
                strCondition3.Append("NVL((INV.NET_RECEIVABLE*get_ex_rate(inv.currency_mst_fk," + BaseCurrFk + ",inv.invoice_date) - NVL(CTRN.recd_amount_hdr_curr,0.00)*GET_EX_RATE(COL.CURRENCY_MST_FK," + BaseCurrFk + ",COL.COLLECTIONS_DATE)), 0) Balance,");
            }

            if (BizType == 2 & process == 1)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_EXP_ACTREV(job.job_card_trn_pk ," + BaseCurrFk + ") REV,");
            }

            if (BizType == 2 & process == 2)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_IMP_ACTREV(JOB.job_card_trn_pk ," + BaseCurrFk + ") REV,");
            }

            if (BizType == 1 & process == 1)
            {
                strCondition.Append("FETCH_JOB_CARD_AIR_EXP_ACTREV(JOB.job_card_trn_pk ," + BaseCurrFk + ") REV,");
            }

            if (BizType == 1 & process == 2)
            {
                strCondition.Append("FETCH_JOB_CARD_AIR_IMP_ACTREV(JOB.job_card_trn_pk ," + BaseCurrFk + ") REV,");
            }

            if (BizType == 3 & process == 1)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_EXP_ACTREV(job.job_card_trn_pk ," + BaseCurrFk + ") REV,");
                strCondition1.Append("FETCH_JOB_CARD_AIR_EXP_ACTREV(JOB.job_card_trn_pk ," + BaseCurrFk + ") REV,");
            }
            if (BizType == 3 & process == 2)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_IMP_ACTREV(JOB.job_card_trn_pk ," + BaseCurrFk + ") REV,");
                strCondition1.Append("FETCH_JOB_CARD_AIR_IMP_ACTREV(JOB.job_card_trn_pk ," + BaseCurrFk + ") REV,");
            }
            if (BizType == 2 & process == 0)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_EXP_ACTREV(job.job_card_trn_pk ," + BaseCurrFk + ") REV,");
                strCondition1.Append("FETCH_JOB_CARD_SEA_IMP_ACTREV(JOB.job_card_trn_pk ," + BaseCurrFk + ") REV,");
            }
            if (BizType == 1 & process == 0)
            {
                strCondition.Append("FETCH_JOB_CARD_AIR_EXP_ACTREV(JOB.job_card_trn_pk ," + BaseCurrFk + ") REV,");
                strCondition1.Append("FETCH_JOB_CARD_AIR_IMP_ACTREV(JOB.job_card_trn_pk ," + BaseCurrFk + ") REV,");
            }
            if (BizType == 3 & process == 0)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_EXP_ACTREV(job.job_card_trn_pk ," + BaseCurrFk + ") REV,");
                strCondition1.Append("FETCH_JOB_CARD_SEA_IMP_ACTREV(JOB.job_card_trn_pk ," + BaseCurrFk + ") REV,");
                strCondition2.Append("FETCH_JOB_CARD_AIR_EXP_ACTREV(JOB.job_card_trn_pk ," + BaseCurrFk + ") REV,");
                strCondition3.Append("FETCH_JOB_CARD_AIR_IMP_ACTREV(JOB.job_card_trn_pk ," + BaseCurrFk + ") REV,");
            }

            strCondition.Append(" CMT.CUSTOMER_MST_PK CUST_PK, 'false' SEL");
            strCondition.Append(" FROM");
            strCondition.Append(" COLLECTIONS_TBL COL, ");
            strCondition.Append(" COLLECTIONS_TRN_TBL CTRN, ");
            strCondition.Append(" CONSOL_INVOICE_TBL INV, ");
            strCondition.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");

            if (BizType == 3 | process == 0)
            {
                strCondition1.Append(" CMT.CUSTOMER_MST_PK CUST_PK, 'false' SEL");
                strCondition1.Append(" FROM");
                strCondition1.Append(" COLLECTIONS_TBL COL, ");
                strCondition1.Append(" COLLECTIONS_TRN_TBL CTRN, ");
                strCondition1.Append(" CONSOL_INVOICE_TBL INV, ");
                strCondition1.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");

                strCondition2.Append(" CMT.CUSTOMER_MST_PK CUST_PK, 'false' SEL");
                strCondition2.Append(" FROM");
                strCondition2.Append(" COLLECTIONS_TBL COL, ");
                strCondition2.Append(" COLLECTIONS_TRN_TBL CTRN, ");
                strCondition2.Append(" CONSOL_INVOICE_TBL INV, ");
                strCondition2.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");

                strCondition3.Append(" CMT.CUSTOMER_MST_PK CUST_PK, 'false' SEL");
                strCondition3.Append(" FROM");
                strCondition3.Append(" COLLECTIONS_TBL COL, ");
                strCondition3.Append(" COLLECTIONS_TRN_TBL CTRN, ");
                strCondition3.Append(" CONSOL_INVOICE_TBL INV, ");
                strCondition3.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");
            }

            if (BizType == 2 & process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 1 & process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HAWB_EXP_TBL            HAWB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 2 & process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                //(+) Snigdharani 18/10/2008
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 1 & process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 3 & process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append(" AND JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1 ");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
                strCondition1.Append(" JOB_CARD_TRN   JOB,");
                strCondition1.Append(" HAWB_EXP_TBL            HAWB,");
                strCondition1.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition1.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition1.Append(" USER_MST_TBL           UMT");
                strCondition1.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition1.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                strCondition1.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                strCondition1.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition1.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition1.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition1.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 3 & process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append("  AND JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 2 ");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
                strCondition1.Append(" JOB_CARD_TRN   JOB,");
                strCondition1.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition1.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition1.Append(" USER_MST_TBL           UMT");
                strCondition1.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition1.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition1.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition1.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition1.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition1.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 2 & process == 0)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append(" AND INV.PROCESS_TYPE=1 ");
                strCondition.Append(" AND INV.BUSINESS_TYPE=2 ");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

                strCondition1.Append(" JOB_CARD_TRN   JOB,");
                strCondition1.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition1.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition1.Append(" USER_MST_TBL           UMT,");
                strCondition1.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition1.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition1.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                strCondition1.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition1.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition1.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition1.Append("  AND INV.PROCESS_TYPE=2 ");
                strCondition1.Append("  AND INV.BUSINESS_TYPE=2 ");
                strCondition1.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition1.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

            }

            if (BizType == 1 & process == 0)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HAWB_EXP_TBL            HAWB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND INV.PROCESS_TYPE=1 ");
                strCondition.Append(" AND INV.BUSINESS_TYPE=1 ");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

                strCondition1.Append(" JOB_CARD_TRN   JOB,");
                strCondition1.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition1.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition1.Append(" USER_MST_TBL           UMT");
                strCondition1.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition1.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition1.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition1.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition1.Append(" AND INV.PROCESS_TYPE=2 ");
                strCondition1.Append(" AND INV.BUSINESS_TYPE=1 ");
                strCondition1.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition1.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 3 & process == 0)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append(" AND INV.PROCESS_TYPE=1 ");
                strCondition.Append(" AND INV.BUSINESS_TYPE=2 ");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

                strCondition1.Append(" JOB_CARD_TRN   JOB,");
                strCondition1.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition1.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition1.Append(" USER_MST_TBL           UMT,");
                strCondition1.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition1.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition1.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                strCondition1.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition1.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition1.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition1.Append(" AND INV.PROCESS_TYPE=2 ");
                strCondition1.Append(" AND INV.BUSINESS_TYPE=2 ");
                strCondition1.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition1.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

                strCondition2.Append(" JOB_CARD_TRN   JOB,");
                strCondition2.Append(" HAWB_EXP_TBL            HAWB,");
                strCondition2.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition2.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition2.Append(" USER_MST_TBL           UMT");
                strCondition2.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition2.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                strCondition2.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                strCondition2.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition2.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition2.Append(" AND INV.PROCESS_TYPE=1 ");
                strCondition2.Append(" AND INV.BUSINESS_TYPE=1 ");
                strCondition2.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition2.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

                strCondition3.Append(" JOB_CARD_TRN   JOB,");
                strCondition3.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition3.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition3.Append(" USER_MST_TBL           UMT");
                strCondition3.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition3.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition3.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition3.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition3.Append(" AND INV.PROCESS_TYPE=2 ");
                strCondition3.Append(" AND INV.BUSINESS_TYPE=1 ");
                strCondition3.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition3.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            strCondition.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            if (LocValue != 0)
            {
                strCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            }

            strCondition.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            strCondition.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            if (process != 0)
            {
                strCondition.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
            }
            if (BizType != 3)
            {
                strCondition.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");
            }
            if (BizType == 3 | process == 0)
            {
                strCondition1.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                if (LocValue != 0)
                {
                    strCondition1.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
                }
                strCondition1.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                strCondition1.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                if (process != 0)
                {
                    strCondition1.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
                }
                if (BizType != 3)
                {
                    strCondition1.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");
                }
                strCondition2.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                if (LocValue != 0)
                {
                    strCondition2.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
                }
                strCondition2.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                strCondition2.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                if (process != 0)
                {
                    strCondition2.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
                }
                if (BizType != 3)
                {
                    strCondition2.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");
                }
                strCondition3.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                if (LocValue != 0)
                {
                    strCondition3.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
                }
                strCondition3.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                strCondition3.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                if (process != 0)
                {
                    strCondition3.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
                }
                if (BizType != 3)
                {
                    strCondition3.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");
                }
            }

            if (!string.IsNullOrEmpty(strCustomer))
            {
                strCondition.Append(" AND CMT.CUSTOMER_MST_PK=" + strCustomer + "AND CMT.CUSTOMER_MST_PK IS NOT NULL ");
            }
            if ((BizType == 3 & !string.IsNullOrEmpty(strCustomer)) | (process == 0 & !string.IsNullOrEmpty(strCustomer)))
            {
                strCondition1.Append(" AND CMT.CUSTOMER_MST_PK=" + strCustomer + "AND CMT.CUSTOMER_MST_PK IS NOT NULL ");
                strCondition2.Append(" AND CMT.CUSTOMER_MST_PK=" + strCustomer + "AND CMT.CUSTOMER_MST_PK IS NOT NULL ");
                strCondition3.Append(" AND CMT.CUSTOMER_MST_PK=" + strCustomer + "AND CMT.CUSTOMER_MST_PK IS NOT NULL ");
            }
            if (BizType != 3 & process != 0)
            {
                strCondition.Append(" ORDER BY 2 DESC");
            }

            StringBuilder strCount = new StringBuilder();
            if (BizType == 3 & process == 0)
            {
                strCount.Append("SELECT COUNT(*) FROM ");
                strCount.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
                strCount.Append(" (select C.CUSTOMER_NAME Customer , SUM(NVL(INVOICE,0)) ,sum(NVL(Collection,0)),sum(NVL(Balance,0)),sum(NVL(REV,0)) , C.CUSTOMER_MST_PK CUST_PK, 'false' SEL from (" + strCondition.ToString() + " union " + strCondition1.ToString() + " union " + strCondition2.ToString() + " union " + strCondition3.ToString() + " ");
                strCount.Append("  ) S ");
                strCount.Append(" , CUSTOMER_MST_TBL C, CUSTOMER_CONTACT_DTLS CMTDTL, USER_MST_TBL UMT ");
                strCount.Append(" WHERE S.CUST_PK = C.CUSTOMER_MST_PK(+) ");
                strCount.Append(" AND C.CREATED_BY_FK = UMT.USER_MST_PK ");
                strCount.Append(" AND C.CUSTOMER_MST_PK = CMTDTL.CUSTOMER_MST_FK(+) ");
                strCount.Append(" AND C.TEMP_PARTY=0 ");
                strCount.Append(" AND C.ACTIVE_FLAG=1 ");
                if (LocValue != 0)
                {
                    strCount.Append(" AND CMTDTL.ADM_LOCATION_MST_FK = " + usrLocFK + " ");
                }
                strCount.Append(" group by  C.CUSTOMER_NAME ,C.CUSTOMER_MST_PK order by NVL(SUM(INVOICE),0) DESC) T) QRY ");
            }
            else if (BizType == 3 | (BizType == 1 & process == 0) | (BizType == 2 & process == 0))
            {
                strCount.Append("SELECT COUNT(*) FROM ");
                strCount.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
                strCount.Append(" (select C.CUSTOMER_NAME Customer , SUM(NVL(INVOICE,0)) ,sum(NVL(Collection,0)),sum(NVL(Balance,0)),sum(NVL(REV,0)) , C.CUSTOMER_MST_PK CUST_PK, 'false' SEL from (" + strCondition.ToString() + " union " + strCondition1.ToString() + " ");
                strCount.Append("  ) S ");
                strCount.Append(" , CUSTOMER_MST_TBL C, CUSTOMER_CONTACT_DTLS CMTDTL, USER_MST_TBL UMT ");
                strCount.Append(" WHERE S.CUST_PK = C.CUSTOMER_MST_PK(+) ");
                strCount.Append(" AND C.CREATED_BY_FK = UMT.USER_MST_PK ");
                strCount.Append(" AND C.CUSTOMER_MST_PK = CMTDTL.CUSTOMER_MST_FK(+) ");
                strCount.Append(" AND C.TEMP_PARTY=0 ");
                strCount.Append(" AND C.ACTIVE_FLAG=1 ");
                if (LocValue != 0)
                {
                    strCount.Append(" AND CMTDTL.ADM_LOCATION_MST_FK = " + usrLocFK + " ");
                }
                strCount.Append(" group by  C.CUSTOMER_NAME ,C.CUSTOMER_MST_PK order by NVL(SUM(INVOICE),0) DESC) T) QRY ");
            }
            else
            {
                strCount.Append("SELECT COUNT(*) FROM ");
                strCount.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
                strCount.Append(" (select C.CUSTOMER_NAME Customer , SUM(NVL(INVOICE,0)) ,sum(NVL(Collection,0)),sum(NVL(Balance,0)),sum(NVL(REV,0)), C.CUSTOMER_MST_PK CUST_PK, 'false' SEL from (" + strCondition.ToString() + " ");
                strCount.Append("  ) S ");
                strCount.Append(" , CUSTOMER_MST_TBL C, CUSTOMER_CONTACT_DTLS CMTDTL, USER_MST_TBL UMT ");
                strCount.Append(" WHERE S.CUST_PK = C.CUSTOMER_MST_PK(+) ");
                strCount.Append(" AND C.CREATED_BY_FK = UMT.USER_MST_PK ");
                strCount.Append(" AND C.CUSTOMER_MST_PK = CMTDTL.CUSTOMER_MST_FK(+) ");
                strCount.Append(" AND C.TEMP_PARTY=0 ");
                strCount.Append(" AND C.ACTIVE_FLAG=1 ");
                if (LocValue != 0)
                {
                    strCount.Append(" AND CMTDTL.ADM_LOCATION_MST_FK = " + usrLocFK + " ");
                }
                strCount.Append(" group by  C.CUSTOMER_NAME ,C.CUSTOMER_MST_PK order by NVL(SUM(INVOICE),0) DESC) T) QRY ");
            }
            dsTotalRecords = (DataSet)objWF.GetDataSet(strCount.ToString());
            TotalRecords = (Int32)dsTotalRecords.Tables[0].Rows[0][0];
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

            StringBuilder sqlstr = new StringBuilder();
            if (BizType == 3 & process == 0)
            {
                sqlstr.Append("SELECT QRY.* FROM ");
                sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
                sqlstr.Append(" (select C.CUSTOMER_NAME Customer , SUM(NVL(INVOICE,0)) ,sum(NVL(Collection,0)),sum(NVL(Balance,0)),sum(NVL(REV,0)) , C.CUSTOMER_MST_PK CUST_PK, 'false' SEL from (" + strCondition.ToString() + " union " + strCondition1.ToString() + " union " + strCondition2.ToString() + " union " + strCondition3.ToString() + " ");
                sqlstr.Append("  ) S ");
                sqlstr.Append(" , CUSTOMER_MST_TBL C, CUSTOMER_CONTACT_DTLS CMTDTL, USER_MST_TBL UMT ");
                sqlstr.Append(" WHERE S.CUST_PK = C.CUSTOMER_MST_PK(+) ");
                sqlstr.Append(" AND C.CREATED_BY_FK = UMT.USER_MST_PK ");
                sqlstr.Append(" AND C.CUSTOMER_MST_PK = CMTDTL.CUSTOMER_MST_FK(+) ");
                sqlstr.Append(" AND C.TEMP_PARTY=0 ");
                sqlstr.Append(" AND C.ACTIVE_FLAG=1 ");
                if (LocValue != 0)
                {
                    sqlstr.Append(" AND CMTDTL.ADM_LOCATION_MST_FK = " + usrLocFK + " ");
                }
                sqlstr.Append(" group by  C.CUSTOMER_NAME ,C.CUSTOMER_MST_PK order by NVL(SUM(INVOICE),0) DESC) T) QRY WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            }
            else if (BizType == 3 | (BizType == 1 & process == 0) | (BizType == 2 & process == 0))
            {
                sqlstr.Append("SELECT QRY.* FROM ");
                sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
                sqlstr.Append(" (select C.CUSTOMER_NAME Customer , SUM(NVL(INVOICE,0)) ,sum(NVL(Collection,0)),sum(NVL(Balance,0)),sum(NVL(REV,0)), C.CUSTOMER_MST_PK CUST_PK, 'false' SEL from (" + strCondition.ToString() + "  union  " + strCondition1.ToString() + " ");
                sqlstr.Append("  ) S ");
                sqlstr.Append(" , CUSTOMER_MST_TBL C, CUSTOMER_CONTACT_DTLS CMTDTL, USER_MST_TBL UMT ");
                sqlstr.Append(" WHERE S.CUST_PK = C.CUSTOMER_MST_PK(+) ");
                sqlstr.Append(" AND C.CREATED_BY_FK = UMT.USER_MST_PK ");
                sqlstr.Append(" AND C.CUSTOMER_MST_PK = CMTDTL.CUSTOMER_MST_FK(+) ");
                sqlstr.Append(" AND C.TEMP_PARTY=0 ");
                sqlstr.Append(" AND C.ACTIVE_FLAG=1 ");
                if (LocValue != 0)
                {
                    sqlstr.Append(" AND CMTDTL.ADM_LOCATION_MST_FK = " + usrLocFK + " ");
                }
                sqlstr.Append(" group by  C.CUSTOMER_NAME ,C.CUSTOMER_MST_PK order by NVL(SUM(INVOICE),0) DESC) T) QRY WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            }
            else
            {
                sqlstr.Append("SELECT QRY.* FROM ");
                sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
                sqlstr.Append(" (select C.CUSTOMER_NAME Customer , SUM(NVL(INVOICE,0)) ,sum(NVL(Collection,0)),sum(NVL(Balance,0)),sum(NVL(REV,0)) , C.CUSTOMER_MST_PK CUST_PK, 'false' SEL from (" + strCondition.ToString() + " ");
                sqlstr.Append("  ) S ");
                sqlstr.Append(" , CUSTOMER_MST_TBL C, CUSTOMER_CONTACT_DTLS CMTDTL, USER_MST_TBL UMT ");
                sqlstr.Append(" WHERE S.CUST_PK = C.CUSTOMER_MST_PK(+) ");
                sqlstr.Append(" AND C.CREATED_BY_FK = UMT.USER_MST_PK ");
                sqlstr.Append(" AND C.CUSTOMER_MST_PK = CMTDTL.CUSTOMER_MST_FK(+) ");
                sqlstr.Append(" AND C.TEMP_PARTY=0 ");
                sqlstr.Append(" AND C.ACTIVE_FLAG=1 ");
                if (LocValue != 0)
                {
                    sqlstr.Append(" AND CMTDTL.ADM_LOCATION_MST_FK = " + usrLocFK + " ");
                }
                sqlstr.Append(" group by  C.CUSTOMER_NAME ,C.CUSTOMER_MST_PK order by NVL(SUM(INVOICE),0) DESC) T) QRY WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            }


            string sql = null;
            bool BL1 = false;
            bool BL2 = false;
            sql = sqlstr.ToString();
            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(sql);

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

        public DataSet FetchListData(string FROM_DATE = "", string TO_DATE = "", string strCustomer = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long usrLocFK = 0, short BizType = 1, short process = 1)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strBuilder = new StringBuilder();
            StringBuilder strCondition = new StringBuilder();
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strCustomer = objWF.ExecuteScaler("select customer_mst_pk from CUSTOMER_MST_TBL where customer_name='" + strCustomer + "'");

            strCondition.Append(" select distinct(CMT.CUSTOMER_NAME) Customer,");
            strCondition.Append(" INV.NET_RECEIVABLE*get_ex_rate(inv.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",inv.invoice_date) Invoice,");
            strCondition.Append("NVL(CTRN.recd_amount_hdr_curr,0) Collection,");
            strCondition.Append("NVL((INV.NET_RECEIVABLE*get_ex_rate(inv.currency_mst_fk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",inv.invoice_date) - NVL(CTRN.recd_amount_hdr_curr,0.00)), 0) Balance,");

            if (BizType == 2 & process == 1)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_EXP_ACTREV(job.job_card_trn_pk) REV");
            }

            if (BizType == 2 & process == 2)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_IMP_ACTREV(JOB.job_card_trn_pk) REV");
            }

            if (BizType == 1 & process == 1)
            {
                strCondition.Append("FETCH_JOB_CARD_AIR_EXP_ACTREV(JOB.job_card_trn_pk) REV");
            }

            if (BizType == 1 & process == 2)
            {
                strCondition.Append("FETCH_JOB_CARD_AIR_IMP_ACTREV(JOB.job_card_trn_pk) REV");
            }

            strCondition.Append(" FROM");
            strCondition.Append(" collections_trn_tbl CTRN, ");
            strCondition.Append(" CONSOL_INVOICE_TBL INV, ");
            strCondition.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");

            if (BizType == 2 & process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }


            if (BizType == 1 & process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HAWB_EXP_TBL            HAWB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }


            if (BizType == 2 & process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }
            if (BizType == 1 & process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            strCondition.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            strCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            strCondition.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            strCondition.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            strCondition.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
            strCondition.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");

            if (!string.IsNullOrEmpty(strCustomer))
            {
                strCondition.Append(" AND CMT.CUSTOMER_MST_PK=" + strCustomer + "");
            }

            strCondition.Append(" ORDER BY INV.NET_RECEIVABLE DESC");
            StringBuilder strCount = new StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + strCondition.ToString() + ")"));
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

            StringBuilder sqlstr = new StringBuilder();
            sqlstr.Append("SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + strCondition.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);

            StringBuilder sqlstr1 = new StringBuilder();
            sqlstr1.Append("insert into invoice_temp_tbl");
            sqlstr1.Append("(");
            sqlstr1.Append("select customer,sum(invoice),sum(Collection),sum(Balance) from");
            sqlstr1.Append(" (" + strCondition.ToString() + " ");
            sqlstr1.Append(" )");
            sqlstr1.Append(" q group by customer ");
            sqlstr1.Append(")");


            string sql = null;
            bool BL1 = false;
            bool BL2 = false;
            sql = sqlstr.ToString();
            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(sql);
                BL1 = Delete_DATA();
                BL2 = objWF.ExecuteCommands(sqlstr1.ToString());
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

        #region "Procedures"


        public bool Delete_DATA()
        {
            bool BL1 = false;
            DataSet DS = null;
            WorkFlow objWF = new WorkFlow();
            objWF.MyCommand.Parameters.Clear();

            WorkFlow ObjWk = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            OracleTransaction TRAN = null;
            int nRecAfct = 0;
            string strSQL = null;
            string strSQL1 = null;
            Int16 upd = default(Int16);
            try
            {
                strSQL = "delete from invoice_temp_tbl";
                ObjWk.OpenConnection();
                TRAN = ObjWk.MyConnection.BeginTransaction();
                var _with1 = objCommand;
                _with1.Connection = ObjWk.MyConnection;
                _with1.CommandType = CommandType.Text;
                _with1.CommandText = strSQL;
                _with1.Transaction = TRAN;
                nRecAfct = _with1.ExecuteNonQuery();

                if (nRecAfct == 1)
                {
                    strSQL = "delete from invoice_temp_tbl";
                    _with1.CommandText = strSQL;
                    _with1.Transaction = TRAN;
                    nRecAfct = _with1.ExecuteNonQuery();
                }
                if (nRecAfct > 0)
                {
                    TRAN.Commit();
                }
                else
                {
                    TRAN.Rollback();
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
            ObjWk.MyConnection.Close();

            return true;
        }
        public string GetProcedure
        {
            get
            {


                if (type == "Both")
                {
                    return "FETCH_FRIEGHT_OUTSTANDING,FETCH_DATA_ALL_NEW";
                }
                else if (type == "ExcludeVslVoy")
                {
                    return "FETCH_FRIEGHT_OUTSTANDING,FETCH_DATA_ALL_EXCL_VSLVOY";

                }
                else if (type == "GroupExcludeVslVoy")
                {
                    return "FETCH_FRIEGHT_OUTSTANDING_IMP,FETCH_DATA_ALL_GRP_EXCL_VSLVOY";
                }
                else if (type == "Group")
                {
                    return "FETCH_FRIEGHT_OUTSTANDING_IMP,FETCH_DATA_ALL_GRP";
                }
                else if (type == "ExcludeVslVoyBNM")
                {
                    return "FETCH_FRIEGHT_OUTSTANDING_IMP,FETCH_DATA_ALL_EXCL_VSLVOY_BNM";
                }
                else if (type == "All")
                {
                    return "FETCH_FRIEGHT_OUTSTANDING_IMP,FETCH_DATA_ALL_BNM";
                }
                return "";
            }
            set { type = value; }
        }
        public string GetProcedure_Cust
        {
            get
            {
                if (type == "SEAEXP")
                {
                    return "FETCH_CUSTOMER_PROFILE,FETCH_DATA";
                }
                else if (type == "AIREXP")
                {
                    return "FETCH_CUSTOMER_PROFILE,FETCH_DATA_AIR_EXP";
                }
                else if (type == "SEAIMP")
                {
                    return "FETCH_CUSTOMER_PROFILE,FETCH_DATA_SEA_IMP";
                }
                else if (type == "AIRIMP")
                {
                    return "FETCH_CUSTOMER_PROFILE,FETCH_DATA_AIR_IMP";
                }
                return "";
            }
            set { type = value; }
        }
        public string GetProcedure_Perf
        {
            get
            {
                if (type == "SEAEXP")
                {
                    return "EXP_IMP_PERFORMANCE1,FETCH_DATA";
                }
                else if (type == "SEAIMP")
                {
                    return "EXP_IMP_PERFORMANCE1,FETCH_DATA_IMP";
                }
                else if (type == "SEAIMPEXP")
                {
                    return "EXP_IMP_PERFORMANCE1,FETCH_DATA_SEA_IMP_EXP";
                }
                else if (type == "AIREXP")
                {
                    return "EXP_IMP_PERFORMANCE_AIR,FETCH_DATA_AIR_EXP";
                }
                else if (type == "AIRIMP")
                {
                    return "EXP_IMP_PERFORMANCE_AIR,FETCH_DATA_AIR_IMP";
                }
                else if (type == "AIRIMPEXP")
                {
                    return "EXP_IMP_PERFORMANCE_AIR,FETCH_DATA_AIR_IMP_EXP";
                }
                else if (type == "AIRSEAIMPEXP")
                {
                    return "EXP_IMP_PERFORMANCE_AIR,FETCH_DATA_AIR_SEA_IMP_EXP";
                }
                else if (type == "AIRIMPEXP")
                {
                    return "EXP_IMP_PERFORMANCE_AIR,FETCH_DATA_AIR_IMP_EXP";
                }
                else if (type == "SEAIMPEXP")
                {
                    return "EXP_IMP_PERFORMANCE,FETCH_DATA_SEA_IMP_EXP";
                }
                else if (type == "AIRSEAIMP")
                {
                    return "EXP_IMP_PERFORMANCE_AIR,FETCH_DATA_AIR_SEA_IMP";
                }
                else if (type == "AIRSEAEXP")
                {
                    return "EXP_IMP_PERFORMANCE_AIR,FETCH_DATA_AIR_SEA_EXP";
                }
                return "";
            }
            set { type = value; }
        }
        #endregion

        #region "Fetch Function"


        public DataSet Fetch_Data_Cust_Sea(string FROM_DATE = "", string TO_DATE = "", string CUSTOMER = "", Int32 TotalPage = 0, Int32 CurrentPage = 0, string Mode = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtTrade = null;
            DataTable dtCust = null;
            DataTable dtLocation = null;
            DataTable dtCommodity = null;
            DataSet dsAll = null;
            string[] strPKGProc = null;
            Int32 TotalRec = default(Int32);

            TotalRec = Convert.ToInt32(GetTotalRecord(FROM_DATE, TO_DATE, CUSTOMER, Mode));
            objWF.MyCommand.Parameters.Clear();

            var _with2 = objWF.MyCommand.Parameters;

            _with2.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;

            _with2.Add("TO_DATE", getDefault(TO_DATE, DBNull.Value)).Direction = ParameterDirection.Input;

            _with2.Add("CUSTOMER", getDefault(CUSTOMER, "0")).Direction = ParameterDirection.Input;

            _with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;

            _with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;

            _with2.Add("TOTALREC_IN", TotalRec).Direction = ParameterDirection.InputOutput;

            _with2.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

            _with2.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;

            strPKGProc = GetProcedure_Cust.Split(',');
            dsAll = objWF.GetDataSet(strPKGProc[0], strPKGProc[1]);
            TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
            CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
            return dsAll;
        }


        private object GetTotalRecord(string FROM_DATE,string TO_DATE, string CUSTOMER,string Mode)
        {
            string strSQL = "";
            WorkFlow objWF = new WorkFlow();
            Int32 TotalRecords = default(Int32);
            string[] strPKGProc = null;
            try
            {
                if (Mode == "SEAEXP")
                {
                    strSQL = "SELECT Count(*) from(";
                    strSQL = strSQL + "SELECT distinct(jobcard_ref_no) from CUSTOMER_MST_TBL CUST,JOB_CARD_TRN JOB,";
                    strSQL = strSQL + "BOOKING_MST_TBL BKG WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK";
                    strSQL = strSQL + "AND BKG.CUST_CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK";
                    strSQL = strSQL + "AND CUST.CUSTOMER_NAME ='" + CUSTOMER + "'";
                    strSQL = strSQL + "AND (BKG.BOOKING_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)";
                    strSQL = strSQL + "AND TO_DATE(NVL('" + TO_DATE + "', DATEFORMAT), DATEFORMAT)))";

                    TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
                }
                if (Mode == "SEAIMP")
                {
                    strSQL = "SELECT Count(*) from(";
                    strSQL = strSQL + "SELECT distinct(jobcard_ref_no) from CUSTOMER_MST_TBL CUST,JOB_CARD_TRN JOB,";
                    strSQL = strSQL + "BOOKING_MST_TBL BKG WHERE JOB.CUST_CUSTOMER_MST_FK= BKG.CUST_CUSTOMER_MST_FK";
                    strSQL = strSQL + "AND BKG.CUST_CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK";
                    strSQL = strSQL + "AND CUST.CUSTOMER_NAME ='" + CUSTOMER + "'";
                    strSQL = strSQL + "AND (BKG.BOOKING_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)";
                    strSQL = strSQL + "AND TO_DATE(NVL('" + TO_DATE + "', DATEFORMAT), DATEFORMAT)))";

                    TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
                }
                if (Mode == "AIREXP")
                {
                    strSQL = "SELECT Count(*) from(";
                    strSQL = strSQL + "SELECT  distinct(jobcard_ref_no) from CUSTOMER_MST_TBL CUST,JOB_CARD_TRN JOB,";
                    strSQL = strSQL + "BOOKING_MST_TBL BKG WHERE JOB.BOOKING_MST_FK= BKG.BOOKING_MST_PK";
                    strSQL = strSQL + "AND BKG.CUST_CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK";
                    strSQL = strSQL + "AND CUST.CUSTOMER_NAME ='" + CUSTOMER + "'";
                    strSQL = strSQL + "AND (BKG.BOOKING_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)";
                    strSQL = strSQL + "AND TO_DATE(NVL('" + TO_DATE + "', DATEFORMAT), DATEFORMAT)))";

                    TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
                }
                if (Mode == "AIRIMP")
                {
                    strSQL = "SELECT Count(*) from(";
                    strSQL = strSQL + "SELECT  distinct(jobcard_ref_no) from CUSTOMER_MST_TBL CUST,JOB_CARD_TRN JOB,";
                    strSQL = strSQL + "BOOKING_MST_TBL BKG WHERE JOB.CUST_CUSTOMER_MST_FK= CUST.CUSTOMER_MST_PK";
                    strSQL = strSQL + "AND BKG.CUST_CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK";
                    strSQL = strSQL + "AND CUST.CUSTOMER_NAME ='" + CUSTOMER + "'";
                    strSQL = strSQL + "AND (BKG.BOOKING_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)";
                    strSQL = strSQL + "AND TO_DATE(NVL('" + TO_DATE + "', DATEFORMAT), DATEFORMAT)))";

                    TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
                }
                return TotalRecords;
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
        public DataSet Fetch_Data(Int32 ChkONLD = 0, string BizType = "", string ProcessType = "", string FROM_DATE = "", string TO_DATE = "", string POL_POD = "", string CUSTOMER = "", string LOCATION = "", string VslVoy = "", string SortColumn = "CUSTOMER",
        Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = "DESC", int VslVoyflg = 0, int JobType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtTrade = null;
            DataTable dtCust = null;
            DataTable dtLocation = null;
            DataTable dtCommodity = null;
            DataSet dsAll = null;
            string[] strPKGProc = null;

            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with3 = objWF.MyCommand.Parameters;

                _with3.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;

                _with3.Add("TO_DATE", getDefault(TO_DATE, DBNull.Value)).Direction = ParameterDirection.Input;

                _with3.Add("CUSTOMER", getDefault(CUSTOMER, "0")).Direction = ParameterDirection.Input;

                _with3.Add("LOCATION", getDefault(LOCATION, 0)).Direction = ParameterDirection.Input;

                _with3.Add("SECTOR", getDefault(POL_POD, 0)).Direction = ParameterDirection.Input;

                _with3.Add("VESSEL", getDefault(VslVoy, 0)).Direction = ParameterDirection.Input;

                _with3.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;

                _with3.Add("COLUMN", getDefault(SortColumn, "RECEIVABLE")).Direction = ParameterDirection.Input;

                _with3.Add("SORT", getDefault(SortType, "DESC")).Direction = ParameterDirection.Input;
                _with3.Add("BIZ_TYPE_IN", getDefault(BizType, 0)).Direction = ParameterDirection.Input;
                _with3.Add("PROCESS_TYPE_IN", getDefault(ProcessType, 0)).Direction = ParameterDirection.Input;
                _with3.Add("JOB_TYPE_IN", getDefault(JobType, 0)).Direction = ParameterDirection.Input;

                _with3.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;

                _with3.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with3.Add("ONPAGELD_IN", ChkONLD).Direction = ParameterDirection.InputOutput;
                _with3.Add("BASE_CURR", Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
                _with3.Add("VSL_VOY_HIDE", VslVoyflg).Direction = ParameterDirection.Input;

                _with3.Add("TOTAL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with3.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with3.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (VslVoyflg != 1)
                {
                    _with3.Add("POL_POD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                }
                _with3.Add("REF_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with3.Add("INVOICE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                strPKGProc = GetProcedure.Split(',');
                dsAll = objWF.GetDataSet(strPKGProc[0], strPKGProc[1]);
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (VslVoyflg == 0)
                {
                    CreateRelation(dsAll);
                }
                else
                {
                    CreateRelationExclVslVoy(dsAll);
                }

                return dsAll;
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
        public DataSet Fetch_Data_BNM(Int32 ChkONLD = 0, string BizType = "", string ProcessType = "", string FROM_DATE = "", string TO_DATE = "", string POL_POD = "", string CUSTOMER = "", string LOCATION = "", string VslVoy = "", string SortColumn = "CUSTOMER",
        Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = "DESC", int VslVoyflg = 0, int Group = 0, int GroupCat = 0, int JobType = 0, string GrpPKs = "")
        {

            WorkFlow objWF = new WorkFlow();
            DataTable dtTrade = null;
            DataTable dtCust = null;
            DataTable dtLocation = null;
            DataTable dtCommodity = null;
            DataSet dsAll = null;
            string[] strPKGProc = null;

            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with4 = objWF.MyCommand.Parameters;
                _with4.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;
                _with4.Add("TO_DATE", getDefault(TO_DATE, DBNull.Value)).Direction = ParameterDirection.Input;
                _with4.Add("CUSTOMER", getDefault(CUSTOMER, "0")).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION", getDefault(LOCATION, 0)).Direction = ParameterDirection.Input;
                _with4.Add("SECTOR", getDefault(POL_POD, 0)).Direction = ParameterDirection.Input;
                _with4.Add("VESSEL", getDefault(VslVoy, 0)).Direction = ParameterDirection.Input;
                _with4.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with4.Add("COLUMN", getDefault(SortColumn, "RECEIVABLE")).Direction = ParameterDirection.Input;
                _with4.Add("SORT", getDefault(SortType, "DESC")).Direction = ParameterDirection.Input;
                _with4.Add("BIZ_TYPE_IN", getDefault(BizType, 0)).Direction = ParameterDirection.Input;
                _with4.Add("PROCESS_TYPE_IN", getDefault(ProcessType, 0)).Direction = ParameterDirection.Input;
                _with4.Add("GROUP_CAT_IN", getDefault(GroupCat, 0)).Direction = ParameterDirection.Input;
                _with4.Add("JOB_TYPE_IN", getDefault(JobType, 0)).Direction = ParameterDirection.Input;
                _with4.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with4.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with4.Add("ONPAGELD_IN", ChkONLD).Direction = ParameterDirection.InputOutput;
                _with4.Add("BASE_CURR", Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
                _with4.Add("TOTAL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (Group == 1 & VslVoyflg == 1)
                {
                    _with4.Add("GRPPK_IN", (string.IsNullOrEmpty(GrpPKs) ? "" : GrpPKs)).Direction = ParameterDirection.Input;
                    _with4.Add("CUST_GROUP_CAT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with4.Add("CUST_GROUP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with4.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with4.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                }
                else if (Group == 1 & VslVoyflg == 0)
                {
                    _with4.Add("GRPPK_IN", (string.IsNullOrEmpty(GrpPKs) ? "" : GrpPKs)).Direction = ParameterDirection.Input;
                    _with4.Add("CUST_GROUP_CAT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with4.Add("CUST_GROUP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with4.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with4.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with4.Add("POL_POD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                }
                else if (VslVoyflg == 1 & Group == 0)
                {
                    _with4.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with4.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                }
                else
                {
                    _with4.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with4.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with4.Add("POL_POD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                }
                _with4.Add("REF_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with4.Add("INVOICE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                strPKGProc = GetProcedure.Split(',');
                dsAll = objWF.GetDataSet(strPKGProc[0], strPKGProc[1]);
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (VslVoyflg == 1 & Group == 1)
                {
                    CreateRelationGroupExclVslvoy(dsAll);
                }
                else if (Group == 1 & VslVoyflg == 0)
                {
                    CreateRelationBNMGroup(dsAll);
                }
                else if (VslVoyflg == 1 & Group == 0)
                {
                    CreateRelationExclVslVoy(dsAll);
                }
                else
                {
                    CreateRelationBNM(dsAll);
                }

                return dsAll;
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


        //Public Function Fetch_Data_Perf_sea(Optional ByVal FROM_DATE As String = "", _
        //                    Optional ByVal TO_DATE As String = "", _
        //                    Optional ByVal CUSTOMER As String = "", _
        //                    Optional ByVal Shipline As String = "", _
        //                    Optional ByVal LOCATION As String = "", _
        //                    Optional ByVal VslVoy As String = "", _
        //                    Optional ByVal pol As String = "", _
        //                    Optional ByVal Pod As String = "", _
        //                    Optional ByVal Cargo As Int32 = 0, _
        //                    Optional ByRef CurrentPage As Int32 = 0, _
        //                    Optional ByRef TotalPage As Int32 = 0, _
        //                     Optional ByVal Column As Integer = 0, _
        //                    Optional ByVal locCurrPk As Integer = 0) As DataSet
        //    Dim objWF As New WorkFlow
        //    Dim dtTrade As DataTable
        //    Dim dtCust As DataTable
        //    Dim dtLocation As DataTable
        //    Dim dtCommodity As DataTable
        //    Dim dsAll As DataSet
        //    Dim arr As Array
        //    Dim VslVoy1 As String
        //    arr = VslVoy.Split("/")
        //    VslVoy = arr(0)
        //    Try
        //        If VslVoy <> "" Then
        //            VslVoy1 = arr(1)
        //        Else
        //            VslVoy1 = ""
        //        End If
        //        Dim strPKGProc() As String
        //        strPKGProc = GetProcedure_Perf.Split(",")
        //         objWF.MyCommand.Parameters.Clear()
        //        With objWF.MyCommand.Parameters
        //            .Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input
        //             .Add("TO_DATE", getDefault(TO_DATE, DBNull.Value)).Direction = ParameterDirection.Input
        //             .Add("CUSTOMER", getDefault(CUSTOMER, "0")).Direction = ParameterDirection.Input
        //            .Add("POL", getDefault(pol, "0")).Direction = ParameterDirection.Input
        //             .Add("SHIPLINE", getDefault(Shipline, "0")).Direction = ParameterDirection.Input
        //            .Add("POD", getDefault(Pod, "0")).Direction = ParameterDirection.Input
        //            .Add("Loc", getDefault(LOCATION, 0)).Direction = ParameterDirection.Input
        //            .Add("VslVoy", getDefault(VslVoy, "0")).Direction = ParameterDirection.Input
        //            .Add("VslVoy1", getDefault(VslVoy1, "0")).Direction = ParameterDirection.Input
        //            .Add("Cargo", getDefault(Cargo, "0")).Direction = ParameterDirection.Input
        //            .Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input
        //            .Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput
        //            .Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput
        //            .Add("COLUMN", Column).Direction = ParameterDirection.Input
        //            If locCurrPk <> 0 Then
        //                .Add("CURRENCY_MST_FK_IN", locCurrPk).Direction = ParameterDirection.Input
        //            End If

        //            .Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output
        //            .Add("POL_POD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output
        //            .Add("SHIP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output
        //            .Add("VSL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output
        //            .Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output
        //            .Add("JOB_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output
        //        End With
        //        dsAll = objWF.GetDataSet(strPKGProc(0), strPKGProc(1))
        //        TotalPage = objWF.MyCommand.Parameters("TOTALPAGE_IN").Value
        //        CurrentPage = objWF.MyCommand.Parameters("CURRENTPAGE_IN").Value
        //        CreateRelation_Perf(dsAll)
        //         Return dsAll
        //    Catch oraexp As OracleException
        //        Throw oraexp
        //    Catch ex As Exception
        //        Throw ex
        //    End Try
        //End Function
        public DataSet Fetch_Data_Perf_air_Sea(string FROM_DATE = "", string TO_DATE = "", string CUSTOMER = "", string pol = "", string Shipline = "", string Pod = "", string LOCATION = "", string Flight_No = "", Int32 Cargo = 0, Int32 Biz_Type = 0,
        Int32 Process_Type = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, int Column = 0, int locCurrPk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtTrade = null;
            DataTable dtCust = null;
            DataTable dtLocation = null;
            DataTable dtCommodity = null;
            DataSet dsAll = null;
            string[] strPKGProc = null;
            string Vsl = null;
            string Voy = null;

            Array arr = null;
            if (!string.IsNullOrEmpty(Flight_No))
            {
                arr = Flight_No.Split('/');
                if (arr.Length == 1)
                {
                    Vsl = Flight_No;
                }
                else
                {
                    Vsl = Convert.ToString(arr.GetValue(0));
                    Voy = Convert.ToString(arr.GetValue(1));
                }
            }
            else
            {
                Vsl = "";
                Voy = "";
            }

            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with5 = objWF.MyCommand.Parameters;
                _with5.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;
                _with5.Add("TO_DATE", getDefault(TO_DATE, DBNull.Value)).Direction = ParameterDirection.Input;
                _with5.Add("CUSTOMER", getDefault(CUSTOMER, DBNull.Value)).Direction = ParameterDirection.Input;
                _with5.Add("POL", getDefault(pol, DBNull.Value)).Direction = ParameterDirection.Input;
                _with5.Add("SHIPLINE", getDefault(Shipline, DBNull.Value)).Direction = ParameterDirection.Input;
                _with5.Add("POD", getDefault(Pod, DBNull.Value)).Direction = ParameterDirection.Input;
                _with5.Add("Loc", getDefault(LOCATION, DBNull.Value)).Direction = ParameterDirection.Input;
                _with5.Add("FtNo", getDefault(Vsl, DBNull.Value)).Direction = ParameterDirection.Input;
                _with5.Add("Voy", getDefault(Voy, DBNull.Value)).Direction = ParameterDirection.Input;
                _with5.Add("Cargo", getDefault(Cargo, "0")).Direction = ParameterDirection.Input;
                _with5.Add("Biz_Type", getDefault(Biz_Type, "0")).Direction = ParameterDirection.Input;
                _with5.Add("Process_Type", getDefault(Process_Type, "0")).Direction = ParameterDirection.Input;

                _with5.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with5.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with5.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with5.Add("COLUMN", Column).Direction = ParameterDirection.Input;
                if (locCurrPk != 0)
                {
                    _with5.Add("CURRENCY_MST_FK_IN", locCurrPk).Direction = ParameterDirection.Input;
                }
                _with5.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with5.Add("POL_POD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with5.Add("SHIP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with5.Add("VSL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with5.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("EXP_IMP_PERFORMANCE_AIR", "FETCH_DATA_AIR_SEA_IMP_EXP");
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                CreateRelation_Perf_air(dsAll);
                return dsAll;
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

        #region "Relation"


        private void CreateRelation_Perf_air(DataSet dsMain)
        {
            DataColumn parentCol = null;
            DataColumn childCol = null;
            parentCol = dsMain.Tables[0].Columns["LOCATION_NAME"];
            childCol = dsMain.Tables[1].Columns["LOCATION_NAME"];
            DataRelation relLoc = null;
            relLoc = new DataRelation("LOC", parentCol, childCol);
            try
            {
                DataRelation relAOO = null;
                relAOO = new DataRelation("AOO", new DataColumn[] {
                    dsMain.Tables[1].Columns["LOCATION_NAME"],
                    dsMain.Tables[1].Columns["AOO"],
                    dsMain.Tables[1].Columns["AOD"],
                    (dsMain.Tables[1].Columns["FLIGHT_NO"]),
                    dsMain.Tables[1].Columns["CARGO_TYPE"]
                }, new DataColumn[] {
                    dsMain.Tables[2].Columns["LOCATION_NAME"],
                    dsMain.Tables[2].Columns["AOO"],
                    dsMain.Tables[2].Columns["AOD"],
                    dsMain.Tables[2].Columns["FLIGHT_NO"],
                    dsMain.Tables[2].Columns["CARGO_TYPE"]
                });

                DataRelation relARL = null;
                relARL = new DataRelation("ARL", new DataColumn[] {
                    dsMain.Tables[2].Columns["LOCATION_NAME"],
                    dsMain.Tables[2].Columns["AOO"],
                    dsMain.Tables[2].Columns["AOD"],
                    dsMain.Tables[2].Columns["FLIGHT_NO"],
                    dsMain.Tables[2].Columns["AIRLINE"],
                    dsMain.Tables[2].Columns["CARGO_TYPE"]
                }, new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["AOO"],
                    dsMain.Tables[3].Columns["AOD"],
                    dsMain.Tables[3].Columns["FLIGHT_NO"],
                    dsMain.Tables[3].Columns["AIRLINE"],
                    dsMain.Tables[3].Columns["CARGO_TYPE"]
                });

                DataRelation relCUS = null;
                relCUS = new DataRelation("CUS", new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["AOO"],
                    dsMain.Tables[3].Columns["AOD"],
                    dsMain.Tables[3].Columns["FLIGHT_NO"],
                    dsMain.Tables[3].Columns["AIRLINE"],
                    dsMain.Tables[3].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[3].Columns["CARGO_TYPE"]
                }, new DataColumn[] {
                    dsMain.Tables[4].Columns["LOCATION_NAME"],
                    dsMain.Tables[4].Columns["AOO"],
                    dsMain.Tables[4].Columns["AOD"],
                    dsMain.Tables[4].Columns["FLIGHT_NO"],
                    dsMain.Tables[4].Columns["AIRLINE"],
                    dsMain.Tables[4].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[4].Columns["CARGO_TYPE"]
                });

                relLoc.Nested = true;
                relAOO.Nested = true;
                relARL.Nested = true;
                relCUS.Nested = true;

                dsMain.Relations.Add(relLoc);
                dsMain.Relations.Add(relAOO);
                dsMain.Relations.Add(relARL);
                dsMain.Relations.Add(relCUS);
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
        private void CreateRelation_Perf_air1(DataSet dsMain)
        {
            DataColumn parentCol = null;
            DataColumn childCol = null;
            try
            {
                parentCol = dsMain.Tables[0].Columns["LOCATION_NAME"];
                childCol = dsMain.Tables[1].Columns["LOCATION_NAME"];
                DataRelation relLoc = null;
                relLoc = new DataRelation("LOC", parentCol, childCol);


                DataRelation relPOL = null;
                relPOL = new DataRelation("AOO", new DataColumn[] {
                    dsMain.Tables[1].Columns["LOCATION_NAME"],
                    dsMain.Tables[1].Columns["AOO"],
                    dsMain.Tables[1].Columns["AOD"],
                    dsMain.Tables[1].Columns["FLIGHT_NO"]
                }, new DataColumn[] {
                    dsMain.Tables[2].Columns["LOCATION_NAME"],
                    dsMain.Tables[2].Columns["AOO"],
                    dsMain.Tables[2].Columns["AOD"],
                    dsMain.Tables[2].Columns["FLIGHT_NO"]
                });

                DataRelation relSHIP = null;
                relSHIP = new DataRelation("SHIP", new DataColumn[] {
                    dsMain.Tables[2].Columns["LOCATION_NAME"],
                    dsMain.Tables[2].Columns["AOO"],
                    dsMain.Tables[2].Columns["AOD"],
                    dsMain.Tables[2].Columns["AIRLINE"],
                    dsMain.Tables[2].Columns["FLIGHT_NO"]
                }, new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["AOO"],
                    dsMain.Tables[3].Columns["AOD"],
                    dsMain.Tables[3].Columns["AIRLINE"],
                    dsMain.Tables[3].Columns["FLIGHT_NO"]
                });

                DataRelation relVSL = null;
                relVSL = new DataRelation("VSL", new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["AOO"],
                    dsMain.Tables[3].Columns["AOD"],
                    dsMain.Tables[3].Columns["AIRLINE"],
                    dsMain.Tables[3].Columns["FLIGHT_NO"],
                    dsMain.Tables[3].Columns["CUSTOMER_NAME"]
                }, new DataColumn[] {
                    dsMain.Tables[4].Columns["LOCATION_NAME"],
                    dsMain.Tables[4].Columns["AOO"],
                    dsMain.Tables[4].Columns["AOD"],
                    dsMain.Tables[4].Columns["AIRLINE"],
                    dsMain.Tables[4].Columns["FLIGHT_NO"],
                    dsMain.Tables[4].Columns["CUSTOMER_NAME"]
                });

                DataRelation relCUS = null;
                relCUS = new DataRelation("CUS", new DataColumn[] {
                    dsMain.Tables[4].Columns["LOCATION_NAME"],
                    dsMain.Tables[4].Columns["AOO"],
                    dsMain.Tables[4].Columns["AOD"],
                    dsMain.Tables[4].Columns["AIRLINE"],
                    dsMain.Tables[4].Columns["FLIGHT_NO"],
                    dsMain.Tables[4].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[4].Columns["JOBCARD_REF_NO"]
                }, new DataColumn[] {
                    dsMain.Tables[5].Columns["LOCATION_NAME"],
                    dsMain.Tables[5].Columns["AOO"],
                    dsMain.Tables[5].Columns["AOD"],
                    dsMain.Tables[5].Columns["AIRLINE"],
                    dsMain.Tables[5].Columns["FLIGHT_NO"],
                    dsMain.Tables[5].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[5].Columns["JOBCARD_REF_NO"]
                });

                relLoc.Nested = true;
                relPOL.Nested = true;
                relSHIP.Nested = true;
                relVSL.Nested = true;
                relCUS.Nested = true;

                dsMain.Relations.Add(relLoc);
                dsMain.Relations.Add(relPOL);
                dsMain.Relations.Add(relSHIP);
                dsMain.Relations.Add(relVSL);
                dsMain.Relations.Add(relCUS);
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
        private void CreateRelation_Perf(DataSet dsMain)
        {
            DataColumn parentCol = null;
            DataColumn childCol = null;
            try
            {
                parentCol = dsMain.Tables[0].Columns["LOCATION_NAME"];
                childCol = dsMain.Tables[1].Columns["LOCATION_NAME"];
                DataRelation relLoc = null;
                relLoc = new DataRelation("LOC", parentCol, childCol);

                DataRelation relPOL = null;

                relPOL = new DataRelation("POL", new DataColumn[] {
                    dsMain.Tables[1].Columns["LOCATION_NAME"],
                    dsMain.Tables[1].Columns["POL"],
                    dsMain.Tables[1].Columns["POD"],
                    dsMain.Tables[1].Columns["SHIPPING_LINE"],
                    dsMain.Tables[1].Columns["VSLVOY"],
                    dsMain.Tables[1].Columns["CARGO_TYPE"]
                }, new DataColumn[] {
                    dsMain.Tables[2].Columns["LOCATION_NAME"],
                    dsMain.Tables[2].Columns["POL"],
                    dsMain.Tables[2].Columns["POD"],
                    dsMain.Tables[2].Columns["SHIPPING_LINE"],
                    dsMain.Tables[2].Columns["VSLVOY"],
                    dsMain.Tables[2].Columns["CARGO_TYPE"]
                });

                DataRelation relSHIP = null;
                relSHIP = new DataRelation("SHIP", new DataColumn[] {
                    dsMain.Tables[2].Columns["LOCATION_NAME"],
                    dsMain.Tables[2].Columns["POL"],
                    dsMain.Tables[2].Columns["POD"],
                    dsMain.Tables[2].Columns["SHIPPING_LINE"],
                    dsMain.Tables[2].Columns["VSLVOY"],
                    dsMain.Tables[2].Columns["CARGO_TYPE"]
                }, new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["POL"],
                    dsMain.Tables[3].Columns["POD"],
                    dsMain.Tables[3].Columns["SHIPPING_LINE"],
                    dsMain.Tables[3].Columns["VSLVOY"],
                    dsMain.Tables[3].Columns["CARGO_TYPE"]
                });

                DataRelation relVSL = null;
                relVSL = new DataRelation("VSL", new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["POL"],
                    dsMain.Tables[3].Columns["POD"],
                    dsMain.Tables[3].Columns["SHIPPING_LINE"],
                    dsMain.Tables[3].Columns["VSLVOY"],
                    dsMain.Tables[3].Columns["CARGO_TYPE"],
                    dsMain.Tables[3].Columns["CUSTOMER_NAME"]
                }, new DataColumn[] {
                    dsMain.Tables[4].Columns["LOCATION_NAME"],
                    dsMain.Tables[4].Columns["POL"],
                    dsMain.Tables[4].Columns["POD"],
                    dsMain.Tables[4].Columns["SHIPPING_LINE"],
                    dsMain.Tables[4].Columns["VSLVOY"],
                    dsMain.Tables[4].Columns["CARGO_TYPE"],
                    dsMain.Tables[4].Columns["CUSTOMER_NAME"]
                });

                DataRelation relCUS = null;
                relCUS = new DataRelation("CUS", new DataColumn[] {
                    dsMain.Tables[4].Columns["LOCATION_NAME"],
                    dsMain.Tables[4].Columns["POL"],
                    dsMain.Tables[4].Columns["POD"],
                    dsMain.Tables[4].Columns["SHIPPING_LINE"],
                    dsMain.Tables[4].Columns["VSLVOY"],
                    dsMain.Tables[4].Columns["CARGO_TYPE"],
                    dsMain.Tables[4].Columns["JOBCARD_REF_NO"]
                }, new DataColumn[] {
                    dsMain.Tables[5].Columns["LOCATION_NAME"],
                    dsMain.Tables[5].Columns["POL"],
                    dsMain.Tables[5].Columns["POD"],
                    dsMain.Tables[5].Columns["SHIPPING_LINE"],
                    dsMain.Tables[5].Columns["VSLVOY"],
                    dsMain.Tables[5].Columns["CARGO_TYPE"],
                    dsMain.Tables[5].Columns["JOBCARD_REF_NO"]
                });


                DataRelation relJOB = null;
                relJOB = new DataRelation("JOB", new DataColumn[] { dsMain.Tables[4].Columns["JOBCARD_REF_NO"] }, new DataColumn[] { dsMain.Tables[5].Columns["JOBCARD_REF_NO"] });

                relLoc.Nested = true;
                relPOL.Nested = true;
                relSHIP.Nested = true;
                relVSL.Nested = true;
                relCUS.Nested = true;

                dsMain.Relations.Add(relLoc);
                dsMain.Relations.Add(relPOL);
                dsMain.Relations.Add(relSHIP);
                dsMain.Relations.Add(relVSL);
                dsMain.Relations.Add(relCUS);
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
        private void CreateRelation(DataSet dsMain)
        {
            DataColumn parentCol = null;
            DataColumn childCol = null;
            try
            {
                parentCol = dsMain.Tables[0].Columns["GRAND_TOTAL"];
                childCol = dsMain.Tables[1].Columns["GRAND_TOTAL"];
                DataRelation relGT = null;
                relGT = new DataRelation("Total", parentCol, childCol);

                parentCol = dsMain.Tables[1].Columns["CUSTOMER_NAME"];
                childCol = dsMain.Tables[2].Columns["CUSTOMER_NAME"];
                DataRelation relCust = null;
                relCust = new DataRelation("Trade", parentCol, childCol);

                DataRelation relLoc = null;
                relLoc = new DataRelation("Cust", new DataColumn[] {
                    dsMain.Tables[2].Columns["LOCATION_NAME"],
                    dsMain.Tables[2].Columns["CUSTOMER_NAME"]
                }, new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["CUSTOMER_NAME"]
                });

                DataRelation relPOL = null;
                relPOL = new DataRelation("Loc", new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[3].Columns["POL"],
                    dsMain.Tables[3].Columns["POD"],
                    dsMain.Tables[3].Columns["VSLVOY"]
                }, new DataColumn[] {
                    dsMain.Tables[4].Columns["LOCATION_NAME"],
                    dsMain.Tables[4].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[4].Columns["POL"],
                    dsMain.Tables[4].Columns["POD"],
                    dsMain.Tables[4].Columns["VSLVOY"]
                });

                parentCol = dsMain.Tables[4].Columns["JOBCARD_REF_NO"];
                childCol = dsMain.Tables[5].Columns["JOBCARD_REF_NO"];
                DataRelation reljob = null;

                reljob = new DataRelation("JOBINV", new DataColumn[] {
                    dsMain.Tables[4].Columns["JOBCARD_REF_NO"],
                    dsMain.Tables[4].Columns["BIZ_TYPE"]
                }, new DataColumn[] {
                    dsMain.Tables[5].Columns["JOBCARD_REF_NO"],
                    dsMain.Tables[5].Columns["BIZ_TYPE"]
                });
                relGT.Nested = true;
                relPOL.Nested = true;
                relCust.Nested = true;
                relLoc.Nested = true;
                reljob.Nested = true;

                dsMain.Relations.Add(relGT);
                dsMain.Relations.Add(relCust);
                dsMain.Relations.Add(relLoc);
                dsMain.Relations.Add(relPOL);
                dsMain.Relations.Add(reljob);
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
        private void CreateRelationExclVslVoy(DataSet dsMain)
        {
            DataColumn parentCol = null;
            DataColumn childCol = null;
            try
            {
                parentCol = dsMain.Tables[0].Columns["GRAND_TOTAL"];
                childCol = dsMain.Tables[1].Columns["GRAND_TOTAL"];
                DataRelation relGT = null;
                relGT = new DataRelation("Total", parentCol, childCol);

                parentCol = dsMain.Tables[1].Columns["CUSTOMER_NAME"];
                childCol = dsMain.Tables[2].Columns["CUSTOMER_NAME"];
                DataRelation relCust = null;
                relCust = new DataRelation("Trade", parentCol, childCol);

                DataRelation relLoc = null;
                relLoc = new DataRelation("Cust", new DataColumn[] {
                    dsMain.Tables[2].Columns["LOCATION_NAME"],
                    dsMain.Tables[2].Columns["CUSTOMER_NAME"]
                }, new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["CUSTOMER_NAME"]
                });

                parentCol = dsMain.Tables[3].Columns["JOBCARD_REF_NO"];
                childCol = dsMain.Tables[4].Columns["JOBCARD_REF_NO"];
                DataRelation reljob = null;

                reljob = new DataRelation("JOBINV", new DataColumn[] {
                    dsMain.Tables[3].Columns["JOBCARD_REF_NO"],
                    dsMain.Tables[3].Columns["BIZ_TYPE"]
                }, new DataColumn[] {
                    dsMain.Tables[4].Columns["JOBCARD_REF_NO"],
                    dsMain.Tables[4].Columns["BIZ_TYPE"]
                });

                relGT.Nested = true;
                relCust.Nested = true;
                relLoc.Nested = true;
                reljob.Nested = true;

                dsMain.Relations.Add(relGT);
                dsMain.Relations.Add(relCust);
                dsMain.Relations.Add(relLoc);
                dsMain.Relations.Add(reljob);

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

        #region "Enhance Search"
        public string FetchVesselVoyageBYPOLandpod(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = "";
            string POLPK = "";
            string PODPK = "";
            string SECTORPK = "";
            string strReq = null;
            string strprocess = "1";
            string BizType = null;
            string Location = null;

            try
            {
                arr = strCond.Split('~');
                strReq = Convert.ToString(arr.GetValue(0));
                strprocess = Convert.ToString(arr.GetValue(1));
                strVES = Convert.ToString(arr.GetValue(2));
                if (arr.Length > 3)
                    strVOY = Convert.ToString(arr.GetValue(3));
                if (arr.Length > 4)
                    POLPK = Convert.ToString(arr.GetValue(4));
                if (arr.Length > 5)
                    PODPK = Convert.ToString(arr.GetValue(5));
                if (arr.Length > 6)
                    BizType = Convert.ToString(arr.GetValue(6));
                if (arr.Length > 7)
                    Location = Convert.ToString(arr.GetValue(7));
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.VESSEL_VOYAGE_POL_MIS";
                var _with6 = selectCommand.Parameters;
                _with6.Add("PROCESS_IN", strprocess).Direction = ParameterDirection.Input;
                _with6.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with6.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with6.Add("POL_PK_IN", (!string.IsNullOrEmpty(POLPK) ? POLPK : "")).Direction = ParameterDirection.Input;
                _with6.Add("POD_PK_IN", (!string.IsNullOrEmpty(PODPK) ? PODPK : "")).Direction = ParameterDirection.Input;
                _with6.Add("BizType_IN", (!string.IsNullOrEmpty(BizType) ? BizType : "")).Direction = ParameterDirection.Input;
                _with6.Add("LOC_PK_IN", (!string.IsNullOrEmpty(Location) ? Location : "")).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
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
                selectCommand.Connection.Close();
            }
        }

        #endregion

        #region "Get Dataset For Print Functionality"
        public DataSet Fetch_Print_Data(string FROM_DATE = "", string TO_DATE = "", string POL_POD = "", string CUSTOMER = "", string LOCATION = "", string VslVoy = "", string bizType = "", string Process = "", int JobType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            string[] strPKGProc = null;
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with7 = objWF.MyCommand.Parameters;
                //FROM_DATE
                _with7.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;
                //TO_DATE()
                _with7.Add("TO_DATE", getDefault(TO_DATE, DBNull.Value)).Direction = ParameterDirection.Input;
                //CUSTOMER()
                _with7.Add("CUSTOMER", getDefault(CUSTOMER, "0")).Direction = ParameterDirection.Input;
                //LOCATION()
                _with7.Add("LOCATION", getDefault(LOCATION, 0)).Direction = ParameterDirection.Input;
                //SECTOR()
                _with7.Add("SECTOR", getDefault(POL_POD, 0)).Direction = ParameterDirection.Input;
                _with7.Add("BIZ_TYPE_IN", getDefault(bizType, 0)).Direction = ParameterDirection.Input;
                _with7.Add("PROCESS_TYPE_IN", getDefault(Process, 0)).Direction = ParameterDirection.Input;
                //VESSEL()
                _with7.Add("VESSEL", getDefault(VslVoy, 0)).Direction = ParameterDirection.Input;
                _with7.Add("JOB_TYPE_IN", getDefault(JobType, 0)).Direction = ParameterDirection.Input;
                _with7.Add("BASE_CURR", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with7.Add("CURSOR_PRINT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                dsAll = objWF.GetDataSet("FETCH_FRIEGHT_OUTSTANDING", "FETCH_PRINT_DS_ALL");

                return dsAll;
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
        public DataSet Fetch_Print_Data_BNM(string FROM_DATE = "", string TO_DATE = "", string POL_POD = "", string CUSTOMER = "", string LOCATION = "", string VslVoy = "", string bizType = "", string Process = "", int JobType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            string[] strPKGProc = null;
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with8 = objWF.MyCommand.Parameters;
                _with8.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;
                _with8.Add("TO_DATE", getDefault(TO_DATE, DBNull.Value)).Direction = ParameterDirection.Input;
                _with8.Add("CUSTOMER", getDefault(CUSTOMER, "0")).Direction = ParameterDirection.Input;
                _with8.Add("LOCATION", getDefault(LOCATION, 0)).Direction = ParameterDirection.Input;
                _with8.Add("SECTOR", getDefault(POL_POD, 0)).Direction = ParameterDirection.Input;
                _with8.Add("BIZ_TYPE_IN", getDefault(bizType, 0)).Direction = ParameterDirection.Input;
                _with8.Add("PROCESS_TYPE_IN", getDefault(Process, 0)).Direction = ParameterDirection.Input;
                _with8.Add("JOB_TYPE_IN", getDefault(JobType, 0)).Direction = ParameterDirection.Input;
                _with8.Add("VESSEL", getDefault(VslVoy, 0)).Direction = ParameterDirection.Input;
                _with8.Add("BASE_CURR", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with8.Add("CURSOR_PRINT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("FETCH_FRIEGHT_OUTSTANDING_IMP", "FETCH_PRINT_DS_ALL_BNM");

                return dsAll;
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
        public object inv_sea_exp(string jcpk)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            StringBuilder sb = new StringBuilder();
            try
            {
                sb.Append("SELECT INV.INV_CUST_SEA_EXP_PK INV_PK,");
                sb.Append("       JOB.JOB_CARD_SEA_EXP_PK JCPK,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       NVL((NVL(GETINBASECURRENCY(INV.NET_PAYABLE,");
                sb.Append("                                  INV.CURRENCY_MST_FK,");
                sb.Append("                                  INV.INVOICE_DATE),");
                sb.Append("                0) -");
                sb.Append("           NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                             CRA.CURRENCY_MST_FK,");
                sb.Append("                                             CRA.CREDIT_NOTE_DATE))");
                sb.Append("                  FROM CR_CUST_SEA_EXP_TBL CRA");
                sb.Append("                 WHERE CRA.INV_CUST_SEA_EXP_FK = INV.INV_CUST_SEA_EXP_PK),");
                sb.Append("                0)),");
                sb.Append("           0) INVOICES,");
                sb.Append("       (SELECT NVL(SUM(COLT.RECD_AMOUNT_HDR_CURR), 0)");
                sb.Append("          FROM COLLECTIONS_TRN_TBL COLT");
                sb.Append("         WHERE COLT.INVOICE_REF_NR = INV.INVOICE_REF_NO) COLLECTION,");
                sb.Append("       NVL((NVL(GETINBASECURRENCY(INV.NET_PAYABLE,");
                sb.Append("                                  INV.CURRENCY_MST_FK,");
                sb.Append("                                  INV.INVOICE_DATE),");
                sb.Append("                0) -");
                sb.Append("           NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                             CRA.CURRENCY_MST_FK,");
                sb.Append("                                             CRA.CREDIT_NOTE_DATE))");
                sb.Append("                  FROM CR_CUST_SEA_EXP_TBL CRA");
                sb.Append("                 WHERE CRA.INV_CUST_SEA_EXP_FK = INV.INV_CUST_SEA_EXP_PK),");
                sb.Append("                0)),");
                sb.Append("           0) - NVL((SELECT SUM(COLT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                      FROM COLLECTIONS_TRN_TBL COLT");
                sb.Append("                     WHERE COLT.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                    0) BALANCE");
                sb.Append("  FROM INV_CUST_SEA_EXP_TBL INV, JOB_CARD_SEA_EXP_TBL JOB");
                sb.Append(" WHERE JOB.JOB_CARD_SEA_EXP_PK = INV.JOB_CARD_SEA_EXP_FK");
                sb.Append("    AND JOB.JOB_CARD_SEA_EXP_PK IN (" + jcpk + ")");
                sb.Append("UNION ");
                sb.Append("SELECT INA.INV_AGENT_PK INV_PK,");
                sb.Append("       JOB.JOB_CARD_SEA_EXP_PK JCPK,");
                sb.Append("       INA.INVOICE_REF_NO,");
                sb.Append("       NVL(NVL(GETINBASECURRENCY(INA.NET_INV_AMT,");
                sb.Append("                                 INA.CURRENCY_MST_FK,");
                sb.Append("                                 INA.INVOICE_DATE),");
                sb.Append("               0) -");
                sb.Append("           NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                            CRA.CURRENCY_MST_FK,");
                sb.Append("                                            CRA.CREDIT_NOTE_DATE))");
                sb.Append("                 FROM CR_AGENT_TBL CRA");
                sb.Append("                WHERE CRA.INV_AGENT_FK = INA.INV_AGENT_PK),");
                sb.Append("               0),");
                sb.Append("           0) INVOICES,");
                sb.Append("       0 COLLECTION,");
                sb.Append("       NVL(NVL(GETINBASECURRENCY(INA.NET_INV_AMT,");
                sb.Append("                                 INA.CURRENCY_MST_FK,");
                sb.Append("                                 INA.INVOICE_DATE),");
                sb.Append("               0) -");
                sb.Append("           NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                            CRA.CURRENCY_MST_FK,");
                sb.Append("                                            CRA.CREDIT_NOTE_DATE))");
                sb.Append("                 FROM CR_AGENT_TBL CRA");
                sb.Append("                WHERE CRA.INV_AGENT_FK = INA.INV_AGENT_PK),");
                sb.Append("               0),");
                sb.Append("           0) - (SELECT NVL(SUM(COLT.RECD_AMOUNT_HDR_CURR), 0)");
                sb.Append("                   FROM COLLECTIONS_TRN_TBL COLT");
                sb.Append("                  WHERE COLT.INVOICE_REF_NR = INA.INVOICE_REF_NO) BALANCE");
                sb.Append("  FROM INV_AGENT_TBL INA, JOB_CARD_SEA_EXP_TBL JOB");
                sb.Append(" WHERE JOB.JOB_CARD_SEA_EXP_PK = INA.JOB_CARD_SEA_EXP_FK");
                sb.Append("    AND JOB.JOB_CARD_SEA_EXP_PK IN (" + jcpk + ")");
                sb.Append("UNION ");
                sb.Append("SELECT CONS.CONSOL_INVOICE_PK INV_PK,");
                sb.Append("       JOB.JOB_CARD_SEA_EXP_PK JCPK,");
                sb.Append("       CONS.INVOICE_REF_NO,");
                sb.Append("       SUM(CONT.TOT_AMT_IN_LOC_CURR) INVOICES,");
                sb.Append("       NVL((SELECT SUM(CT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("             FROM COLLECTIONS_TRN_TBL CT");
                sb.Append("            WHERE CT.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("           0) COLLECTIONS,");
                sb.Append("       SUM(CONT.TOT_AMT_IN_LOC_CURR) -");
                sb.Append("       (NVL((SELECT SUM(CT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("              FROM COLLECTIONS_TRN_TBL CT");
                sb.Append("             WHERE CT.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("            0)) BALANCE");
                sb.Append("  FROM CONSOL_INVOICE_TBL     CONS,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL CONT,");
                sb.Append("       JOB_CARD_SEA_EXP_TBL   JOB,");
                sb.Append("       INV_CUST_SEA_EXP_TBL   INV");
                sb.Append(" WHERE CONS.CONSOL_INVOICE_PK = CONT.CONSOL_INVOICE_FK");
                sb.Append("   AND JOB.JOB_CARD_SEA_EXP_PK = CONT.JOB_CARD_FK(+)");
                sb.Append("   AND JOB.JOB_CARD_SEA_EXP_PK = INV.JOB_CARD_SEA_EXP_FK(+)");
                sb.Append("    AND JOB.JOB_CARD_SEA_EXP_PK IN (" + jcpk + ")");
                sb.Append(" GROUP BY CONS.INVOICE_REF_NO,");
                sb.Append("          CONS.CONSOL_INVOICE_PK,");
                sb.Append("          INV.INVOICE_REF_NO,");
                sb.Append("          JOB.JOB_CARD_SEA_EXP_PK");

                dsAll = objWF.GetDataSet(sb.ToString());
                return dsAll;
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
        public object inv_air_exp(string jcpk)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            StringBuilder sb = new StringBuilder();
            try
            {
                sb.Append("SELECT INV.INV_CUST_AIR_EXP_PK INV_PK,");
                sb.Append("       JOB.JOB_CARD_AIR_EXP_PK JCPK,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       NVL(GETINBASECURRENCY(INV.NET_PAYABLE,");
                sb.Append("                             INV.CURRENCY_MST_FK,");
                sb.Append("                             INV.INVOICE_DATE),");
                sb.Append("           0) -");
                sb.Append("       NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                        CRA.CURRENCY_MST_FK,");
                sb.Append("                                        CRA.CREDIT_NOTE_DATE))");
                sb.Append("             FROM CR_CUST_AIR_EXP_TBL CRA");
                sb.Append("            WHERE CRA.INV_CUST_AIR_EXP_FK = INV.INV_CUST_AIR_EXP_PK),");
                sb.Append("           0) INVOICES,");
                sb.Append("       (SELECT NVL(SUM(COLT.RECD_AMOUNT_HDR_CURR), 0)");
                sb.Append("          FROM COLLECTIONS_TRN_TBL COLT");
                sb.Append("         WHERE COLT.INVOICE_REF_NR = INV.INVOICE_REF_NO) COLLECTION,");
                sb.Append("       NVL(GETINBASECURRENCY(INV.NET_PAYABLE,");
                sb.Append("                             INV.CURRENCY_MST_FK,");
                sb.Append("                             INV.INVOICE_DATE),");
                sb.Append("           0) -");
                sb.Append("       NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                        CRA.CURRENCY_MST_FK,");
                sb.Append("                                        CRA.CREDIT_NOTE_DATE))");
                sb.Append("             FROM CR_CUST_AIR_EXP_TBL CRA");
                sb.Append("            WHERE CRA.INV_CUST_AIR_EXP_FK = INV.INV_CUST_AIR_EXP_PK),");
                sb.Append("           0) - NVL((SELECT SUM(COLT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                      FROM COLLECTIONS_TRN_TBL COLT");
                sb.Append("                     WHERE COLT.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                    0) BALANCE");
                sb.Append("");
                sb.Append("  FROM INV_CUST_AIR_EXP_TBL INV, JOB_CARD_AIR_EXP_TBL JOB");
                sb.Append(" WHERE JOB.JOB_CARD_AIR_EXP_PK = INV.JOB_CARD_AIR_EXP_FK");
                sb.Append(" AND JOB.JOB_CARD_AIR_EXP_PK IN (" + jcpk + ")   ");
                sb.Append("UNION ");
                sb.Append("SELECT INA.INV_AGENT_PK INV_PK,");
                sb.Append("       JOB.JOB_CARD_AIR_EXP_PK JCPK,");
                sb.Append("       INA.INVOICE_REF_NO,");
                sb.Append("       NVL(NVL(GETINBASECURRENCY(INA.NET_INV_AMT,");
                sb.Append("                                 INA.CURRENCY_MST_FK,");
                sb.Append("                                 INA.INVOICE_DATE),");
                sb.Append("               0) -");
                sb.Append("           NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                            CRA.CURRENCY_MST_FK,");
                sb.Append("                                            CRA.CREDIT_NOTE_DATE))");
                sb.Append("                 FROM CR_AGENT_TBL CRA");
                sb.Append("                WHERE CRA.INV_AGENT_FK = INA.INV_AGENT_PK),");
                sb.Append("               0),");
                sb.Append("           0) INVOICES,");
                sb.Append("       0 COLLECTION,");
                sb.Append("       NVL((GETINBASECURRENCY(INA.NET_INV_AMT,");
                sb.Append("                              INA.CURRENCY_MST_FK,");
                sb.Append("                              INA.INVOICE_DATE) -");
                sb.Append("           (SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                          CRA.CURRENCY_MST_FK,");
                sb.Append("                                          CRA.CREDIT_NOTE_DATE))");
                sb.Append("               FROM CR_AGENT_TBL CRA");
                sb.Append("              WHERE CRA.INV_AGENT_FK = INA.INV_AGENT_PK)),");
                sb.Append("           0) - NVL((SELECT SUM(COLT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                      FROM COLLECTIONS_TRN_TBL COLT");
                sb.Append("                     WHERE COLT.INVOICE_REF_NR = INA.INVOICE_REF_NO),");
                sb.Append("                    0) BALANCE");
                sb.Append("  FROM INV_AGENT_TBL INA, JOB_CARD_AIR_EXP_TBL JOB");
                sb.Append(" WHERE JOB.JOB_CARD_AIR_EXP_PK = INA.JOB_CARD_AIR_EXP_FK");
                sb.Append(" AND JOB.JOB_CARD_AIR_EXP_PK IN (" + jcpk + ")   ");
                sb.Append("UNION ");
                sb.Append("SELECT CONS.CONSOL_INVOICE_PK INV_PK,");
                sb.Append("       JOB.JOB_CARD_AIR_EXP_PK JCPK,");
                sb.Append("       CONS.INVOICE_REF_NO,");
                sb.Append("       SUM(CONT.TOT_AMT_IN_LOC_CURR) INVOICES,");
                sb.Append("       NVL((SELECT SUM(CT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("             FROM COLLECTIONS_TRN_TBL CT");
                sb.Append("            WHERE CT.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("           0) COLLECTIONS,");
                sb.Append("       SUM(CONT.TOT_AMT_IN_LOC_CURR) -");
                sb.Append("       (NVL((SELECT SUM(CT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("              FROM COLLECTIONS_TRN_TBL CT");
                sb.Append("             WHERE CT.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("            0)) BALANCE");
                sb.Append("  FROM CONSOL_INVOICE_TBL     CONS,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL CONT,");
                sb.Append("       JOB_CARD_AIR_EXP_TBL   JOB,");
                sb.Append("       INV_CUST_AIR_EXP_TBL   INV");
                sb.Append(" WHERE CONS.CONSOL_INVOICE_PK = CONT.CONSOL_INVOICE_FK");
                sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = CONT.JOB_CARD_FK(+)");
                sb.Append("   AND JOB.JOB_CARD_AIR_EXP_PK = INV.JOB_CARD_AIR_EXP_FK(+)");
                sb.Append(" AND JOB.JOB_CARD_AIR_EXP_PK IN (" + jcpk + ")   ");
                sb.Append(" GROUP BY CONS.INVOICE_REF_NO,");
                sb.Append("          CONS.CONSOL_INVOICE_PK,");
                sb.Append("          INV.INVOICE_REF_NO,");
                sb.Append("          JOB.JOBCARD_REF_NO,");
                sb.Append("          JOB.JOB_CARD_AIR_EXP_PK");

                dsAll = objWF.GetDataSet(sb.ToString());
                return dsAll;
                //
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
        public object inv_sea_imp(string jcpk)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            StringBuilder sb = new StringBuilder();
            try
            {
                sb.Append("SELECT INV.INV_CUST_SEA_IMP_PK INV_PK,");
                sb.Append("       JOB.JOB_CARD_SEA_IMP_PK JCPK,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       NVL(GETINBASECURRENCY(INV.NET_PAYABLE,");
                sb.Append("                             INV.CURRENCY_MST_FK,");
                sb.Append("                             INV.INVOICE_DATE),");
                sb.Append("           0) -");
                sb.Append("       NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                        CRA.CURRENCY_MST_FK,");
                sb.Append("                                        CRA.CREDIT_NOTE_DATE))");
                sb.Append("             FROM CR_CUST_SEA_IMP_TBL CRA");
                sb.Append("            WHERE CRA.INV_CUST_SEA_IMP_FK = INV.INV_CUST_SEA_IMP_PK),");
                sb.Append("           0) INVOICES,");
                sb.Append("       (SELECT NVL(SUM(COLT.RECD_AMOUNT_HDR_CURR), 0)");
                sb.Append("          FROM COLLECTIONS_TRN_TBL COLT");
                sb.Append("         WHERE COLT.INVOICE_REF_NR = INV.INVOICE_REF_NO) COLLECTION,");
                sb.Append("       NVL(GETINBASECURRENCY(INV.NET_PAYABLE,");
                sb.Append("                             INV.CURRENCY_MST_FK,");
                sb.Append("                             INV.INVOICE_DATE),");
                sb.Append("           0) -");
                sb.Append("       NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                        CRA.CURRENCY_MST_FK,");
                sb.Append("                                        CRA.CREDIT_NOTE_DATE))");
                sb.Append("             FROM CR_CUST_SEA_IMP_TBL CRA");
                sb.Append("            WHERE CRA.INV_CUST_SEA_IMP_FK = INV.INV_CUST_SEA_IMP_PK),");
                sb.Append("           0) - NVL((SELECT SUM(COLT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                      FROM COLLECTIONS_TRN_TBL COLT");
                sb.Append("                     WHERE COLT.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                    0) -");
                sb.Append("       (SELECT NVL(SUM(COLT.RECD_AMOUNT_HDR_CURR), 0)");
                sb.Append("          FROM COLLECTIONS_TRN_TBL COLT");
                sb.Append("         WHERE COLT.INVOICE_REF_NR = INV.INVOICE_REF_NO) BALANCE");
                sb.Append("");
                sb.Append("  FROM INV_CUST_SEA_IMP_TBL INV, JOB_CARD_SEA_IMP_TBL JOB");
                sb.Append(" WHERE JOB.JOB_CARD_SEA_IMP_PK = INV.JOB_CARD_SEA_IMP_FK");
                sb.Append("   AND JOB.JOB_CARD_SEA_IMP_PK IN(" + jcpk + ")");
                sb.Append("UNION ");
                sb.Append("SELECT INA.INV_AGENT_PK INV_PK,");
                sb.Append("       JOB.JOB_CARD_SEA_IMP_PK,");
                sb.Append("       INA.INVOICE_REF_NO,");
                sb.Append("       NVL(GETINBASECURRENCY(INA.NET_INV_AMT,");
                sb.Append("                             INA.CURRENCY_MST_FK,");
                sb.Append("                             INA.INVOICE_DATE),");
                sb.Append("           0) -");
                sb.Append("       NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                        CRA.CURRENCY_MST_FK,");
                sb.Append("                                        CRA.CREDIT_NOTE_DATE))");
                sb.Append("             FROM CR_AGENT_TBL CRA");
                sb.Append("            WHERE CRA.INV_AGENT_FK = INA.INV_AGENT_PK),");
                sb.Append("           0) INVOICES,");
                sb.Append("       0 COLLECTION,");
                sb.Append("       NVL(GETINBASECURRENCY(INA.NET_INV_AMT,");
                sb.Append("                             INA.CURRENCY_MST_FK,");
                sb.Append("                             INA.INVOICE_DATE),");
                sb.Append("           0) -");
                sb.Append("       NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                        CRA.CURRENCY_MST_FK,");
                sb.Append("                                        CRA.CREDIT_NOTE_DATE))");
                sb.Append("             FROM CR_AGENT_TBL CRA");
                sb.Append("            WHERE CRA.INV_AGENT_FK = INA.INV_AGENT_PK),");
                sb.Append("           0) - (SELECT NVL(SUM(COLT.RECD_AMOUNT_HDR_CURR), 0)");
                sb.Append("                   FROM COLLECTIONS_TRN_TBL COLT");
                sb.Append("                  WHERE COLT.INVOICE_REF_NR = INA.INVOICE_REF_NO) BALANCE");
                sb.Append("");
                sb.Append("  FROM INV_AGENT_TBL INA, JOB_CARD_SEA_IMP_TBL JOB");
                sb.Append(" WHERE JOB.JOB_CARD_SEA_IMP_PK = INA.JOB_CARD_SEA_IMP_FK");
                sb.Append("   AND JOB.JOB_CARD_SEA_IMP_PK IN(" + jcpk + ")");
                sb.Append("UNION ");
                sb.Append("SELECT CONS.CONSOL_INVOICE_PK INV_PK,");
                sb.Append("       JOB.JOB_CARD_SEA_IMP_PK JCPK,");
                sb.Append("       CONS.INVOICE_REF_NO,");
                sb.Append("       SUM(CONT.TOT_AMT_IN_LOC_CURR) INVOICES,");
                sb.Append("       NVL((SELECT SUM(CT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("             FROM COLLECTIONS_TRN_TBL CT");
                sb.Append("            WHERE CT.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("           0) COLLECTIONS,");
                sb.Append("       SUM(CONT.TOT_AMT_IN_LOC_CURR) -");
                sb.Append("       (NVL((SELECT SUM(CT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("              FROM COLLECTIONS_TRN_TBL CT");
                sb.Append("             WHERE CT.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("            0)) BALANCE");
                sb.Append("  FROM CONSOL_INVOICE_TBL     CONS,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL CONT,");
                sb.Append("       JOB_CARD_SEA_IMP_TBL   JOB,");
                sb.Append("       INV_CUST_SEA_IMP_TBL   INV");
                sb.Append(" WHERE CONS.CONSOL_INVOICE_PK = CONT.CONSOL_INVOICE_FK");
                sb.Append("   AND JOB.JOB_CARD_SEA_IMP_PK = CONT.JOB_CARD_FK(+)");
                sb.Append("   AND JOB.JOB_CARD_SEA_IMP_PK = INV.JOB_CARD_SEA_IMP_FK(+)");
                sb.Append("   AND JOB.JOB_CARD_SEA_IMP_PK IN(" + jcpk + ")");
                sb.Append(" GROUP BY CONS.INVOICE_REF_NO,");
                sb.Append("          CONS.CONSOL_INVOICE_PK,");
                sb.Append("          INV.INVOICE_REF_NO,");
                sb.Append("          JOB.JOBCARD_REF_NO,");
                sb.Append("          JOB.JOB_CARD_SEA_IMP_PK");

                dsAll = objWF.GetDataSet(sb.ToString());
                return dsAll;
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
        public object inv_air_imp(string jcpk)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            StringBuilder sb = new StringBuilder();
            try
            {
                sb.Append("SELECT INV.INV_CUST_AIR_IMP_PK INV_PK,");
                sb.Append("       JOB.JOB_CARD_AIR_IMP_PK JCPK,");
                sb.Append("       INV.INVOICE_REF_NO,");
                sb.Append("       NVL(GETINBASECURRENCY(INV.NET_PAYABLE,");
                sb.Append("                             INV.CURRENCY_MST_FK,");
                sb.Append("                             INV.INVOICE_DATE),");
                sb.Append("           0) -");
                sb.Append("       NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                        CRA.CURRENCY_MST_FK,");
                sb.Append("                                        CRA.CREDIT_NOTE_DATE))");
                sb.Append("             FROM CR_CUST_AIR_IMP_TBL CRA");
                sb.Append("            WHERE CRA.INV_CUST_AIR_IMP_FK = INV.INV_CUST_AIR_IMP_PK),");
                sb.Append("           0) INVOICES,");
                sb.Append("       (SELECT NVL(SUM(COLT.RECD_AMOUNT_HDR_CURR), 0)");
                sb.Append("          FROM COLLECTIONS_TRN_TBL COLT");
                sb.Append("         WHERE COLT.INVOICE_REF_NR = INV.INVOICE_REF_NO) COLLECTION,");
                sb.Append("       NVL(GETINBASECURRENCY(INV.NET_PAYABLE,");
                sb.Append("                             INV.CURRENCY_MST_FK,");
                sb.Append("                             INV.INVOICE_DATE),");
                sb.Append("           0) -");
                sb.Append("       NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                        CRA.CURRENCY_MST_FK,");
                sb.Append("                                        CRA.CREDIT_NOTE_DATE))");
                sb.Append("             FROM CR_CUST_AIR_IMP_TBL CRA");
                sb.Append("            WHERE CRA.INV_CUST_AIR_IMP_FK = INV.INV_CUST_AIR_IMP_PK),");
                sb.Append("           0) - NVL((SELECT SUM(COLT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                      FROM COLLECTIONS_TRN_TBL COLT");
                sb.Append("                     WHERE COLT.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("                    0) BALANCE");
                sb.Append("  FROM INV_CUST_AIR_IMP_TBL INV, JOB_CARD_AIR_IMP_TBL JOB");
                sb.Append(" WHERE JOB.JOB_CARD_AIR_IMP_PK = INV.JOB_CARD_AIR_IMP_FK");
                sb.Append(" AND JOB.JOB_CARD_AIR_IMP_PK IN (" + jcpk + ")   ");
                sb.Append("UNION ");
                sb.Append("SELECT INA.INV_AGENT_PK INV_PK,");
                sb.Append("       JOB.JOB_CARD_AIR_IMP_PK JCPK,");
                sb.Append("       INA.INVOICE_REF_NO,");
                sb.Append("       NVL(NVL(GETINBASECURRENCY(INA.NET_INV_AMT,");
                sb.Append("                                 INA.CURRENCY_MST_FK,");
                sb.Append("                                 INA.INVOICE_DATE),");
                sb.Append("               0) -");
                sb.Append("           NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                            CRA.CURRENCY_MST_FK,");
                sb.Append("                                            CRA.CREDIT_NOTE_DATE))");
                sb.Append("                 FROM CR_AGENT_TBL CRA");
                sb.Append("                WHERE CRA.INV_AGENT_FK = INA.INV_AGENT_PK),");
                sb.Append("               0),");
                sb.Append("           0) INVOICES,");
                sb.Append("       0 COLLECTION,");
                sb.Append("       NVL(NVL(GETINBASECURRENCY(INA.NET_INV_AMT,");
                sb.Append("                                 INA.CURRENCY_MST_FK,");
                sb.Append("                                 INA.INVOICE_DATE),");
                sb.Append("               0) -");
                sb.Append("           NVL((SELECT SUM(GETINBASECURRENCY(CRA.CREDIT_NOTE_AMT,");
                sb.Append("                                            CRA.CURRENCY_MST_FK,");
                sb.Append("                                            CRA.CREDIT_NOTE_DATE))");
                sb.Append("                 FROM CR_AGENT_TBL CRA");
                sb.Append("                WHERE CRA.INV_AGENT_FK = INA.INV_AGENT_PK),");
                sb.Append("               0),");
                sb.Append("           0) - NVL((SELECT SUM(COLT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("                      FROM COLLECTIONS_TRN_TBL COLT");
                sb.Append("                     WHERE COLT.INVOICE_REF_NR = INA.INVOICE_REF_NO),");
                sb.Append("                    0) BALANCE");
                sb.Append("  FROM INV_AGENT_TBL INA, JOB_CARD_AIR_IMP_TBL JOB");
                sb.Append(" WHERE JOB.JOB_CARD_AIR_IMP_PK = INA.JOB_CARD_AIR_IMP_FK");
                sb.Append(" AND JOB.JOB_CARD_AIR_IMP_PK IN (" + jcpk + ")   ");
                sb.Append("UNION ");
                sb.Append("SELECT CONS.CONSOL_INVOICE_PK INV_PK,");
                sb.Append("       JOB.JOB_CARD_AIR_IMP_PK JCPK,");
                sb.Append("       CONS.INVOICE_REF_NO,");
                sb.Append("       SUM(CONT.TOT_AMT_IN_LOC_CURR) INVOICES,");
                sb.Append("       NVL((SELECT SUM(CT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("             FROM COLLECTIONS_TRN_TBL CT");
                sb.Append("            WHERE CT.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("           0) COLLECTIONS,");
                sb.Append("       SUM(CONT.TOT_AMT_IN_LOC_CURR) -");
                sb.Append("       (NVL((SELECT SUM(CT.RECD_AMOUNT_HDR_CURR)");
                sb.Append("              FROM COLLECTIONS_TRN_TBL CT");
                sb.Append("             WHERE CT.INVOICE_REF_NR = INV.INVOICE_REF_NO),");
                sb.Append("            0)) BALANCE");
                sb.Append("  FROM CONSOL_INVOICE_TBL     CONS,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL CONT,");
                sb.Append("       JOB_CARD_AIR_IMP_TBL   JOB,");
                sb.Append("       INV_CUST_AIR_IMP_TBL   INV");
                sb.Append(" WHERE CONS.CONSOL_INVOICE_PK = CONT.CONSOL_INVOICE_FK");
                sb.Append("   AND JOB.JOB_CARD_AIR_IMP_PK = CONT.JOB_CARD_FK(+)");
                sb.Append("   AND JOB.JOB_CARD_AIR_IMP_PK = INV.JOB_CARD_AIR_IMP_FK(+)");
                sb.Append(" AND JOB.JOB_CARD_AIR_IMP_PK IN (" + jcpk + ")   ");
                sb.Append(" GROUP BY CONS.INVOICE_REF_NO,");
                sb.Append("          CONS.CONSOL_INVOICE_PK,");
                sb.Append("          INV.INVOICE_REF_NO,");
                sb.Append("          JOB.JOBCARD_REF_NO,");
                sb.Append("          JOB.JOB_CARD_AIR_IMP_PK");

                dsAll = objWF.GetDataSet(sb.ToString());
                return dsAll;
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

        #region "For ALL BIZTYype and ProcessType"

        public DataSet inv_all(string jcpk, string BizType, string ProcessType, string JcRefNrs)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            string[] strPKGProc = null;
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with9 = objWF.MyCommand.Parameters;
                _with9.Add("BIZ_TYPE_IN", getDefault(BizType, DBNull.Value)).Direction = ParameterDirection.Input;
                _with9.Add("PROCESS_TYPE_IN", getDefault(ProcessType, DBNull.Value)).Direction = ParameterDirection.Input;
                _with9.Add("JC_NO_IN", getDefault(JcRefNrs, DBNull.Value)).Direction = ParameterDirection.Input;
                _with9.Add("BASE_CURR", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with9.Add("CURSOR_PRINT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("FETCH_FRIEGHT_OUTSTANDING", "FETCH_PRINT_DS_ALL_INVOICE");
                return dsAll;
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

        #region "Get Invoice Details for the selected customer for PRINT"
        public DataSet FetchInvoiceDet(string FROM_DATE = "", string TO_DATE = "", string CustomerPKS = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long usrLocFK = 0, short BizType = 1, short process = 1, long CurrPk = 0)
        {


            Int32 last = default(Int32);
            Int32 start = default(Int32);
            StringBuilder strBuilder = new StringBuilder();
            StringBuilder strCondition = new StringBuilder();
            StringBuilder strCondition1 = new StringBuilder();
            StringBuilder strCondition2 = new StringBuilder();
            StringBuilder strCondition3 = new StringBuilder();
            Int32 TotalRecords = default(Int32);

            CurrPk = (CurrPk == 0 ? Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]) : CurrPk);

            WorkFlow objWF = new WorkFlow();

            strCondition.Append(" select distinct(CMT.CUSTOMER_NAME) Customer,");
            strCondition.Append(" INV.NET_RECEIVABLE*get_ex_rate(inv.currency_mst_fk, " + CurrPk + " ,inv.invoice_date) Invoice,");
            strCondition.Append("NVL(CTRN.recd_amount_hdr_curr,0) * GET_EX_RATE(COL.CURRENCY_MST_FK, " + CurrPk + " , COL.COLLECTIONS_DATE) Collection,");
            strCondition.Append("NVL((INV.NET_RECEIVABLE*get_ex_rate(inv.currency_mst_fk," + CurrPk + " ,inv.invoice_date) - NVL(CTRN.recd_amount_hdr_curr,0.00)* GET_EX_RATE(COL.CURRENCY_MST_FK, " + CurrPk + ", COL.COLLECTIONS_DATE)), 0) Balance,");
            if (BizType == 3 | process == 0)
            {
                strCondition1.Append(" select distinct(CMT.CUSTOMER_NAME) Customer,");
                strCondition1.Append(" INV.NET_RECEIVABLE*GET_EX_RATE(inv.currency_mst_fk," + CurrPk + ",inv.invoice_date) Invoice,");
                strCondition1.Append(" NVL(CTRN.recd_amount_hdr_curr,0)*GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPk + ",COL.COLLECTIONS_DATE) Collection,");
                strCondition1.Append(" NVL((INV.NET_RECEIVABLE*GET_EX_RATE(inv.currency_mst_fk," + CurrPk + ",inv.invoice_date) - NVL(CTRN.recd_amount_hdr_curr,0.00)*GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPk + ",COL.COLLECTIONS_DATE)), 0) Balance,");
                strCondition2.Append(" select distinct(CMT.CUSTOMER_NAME) Customer,");
                strCondition2.Append(" INV.NET_RECEIVABLE*GET_EX_RATE(inv.currency_mst_fk," + CurrPk + ",inv.invoice_date) Invoice,");
                strCondition2.Append(" NVL(CTRN.recd_amount_hdr_curr,0)*GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPk + ",COL.COLLECTIONS_DATE) Collection,");
                strCondition2.Append(" NVL((INV.NET_RECEIVABLE*GET_EX_RATE(inv.currency_mst_fk," + CurrPk + ",inv.invoice_date) - NVL(CTRN.recd_amount_hdr_curr,0.00)*GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPk + ",COL.COLLECTIONS_DATE)), 0) Balance,");
                strCondition3.Append(" select distinct(CMT.CUSTOMER_NAME) Customer,");
                strCondition3.Append(" INV.NET_RECEIVABLE*GET_EX_RATE(inv.currency_mst_fk," + CurrPk + ",inv.invoice_date) Invoice,");
                strCondition3.Append(" NVL(CTRN.recd_amount_hdr_curr,0)*GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPk + ",COL.COLLECTIONS_DATE) Collection,");
                strCondition3.Append(" NVL((INV.NET_RECEIVABLE*get_ex_rate(inv.currency_mst_fk," + CurrPk + ",inv.invoice_date) - NVL(CTRN.recd_amount_hdr_curr,0.00)*GET_EX_RATE(COL.CURRENCY_MST_FK," + CurrPk + ",COL.COLLECTIONS_DATE)), 0) Balance,");
            }

            if (BizType == 2 & process == 1)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_EXP_ACTREV(job.job_card_trn_pk, " + CurrPk + ") REV,");
            }

            if (BizType == 2 & process == 2)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_IMP_ACTREV(JOB.job_card_trn_pk," + CurrPk + ") REV,");
            }

            if (BizType == 1 & process == 1)
            {
                strCondition.Append("FETCH_JOB_CARD_AIR_EXP_ACTREV(JOB.job_card_trn_pk," + CurrPk + ") REV,");
            }

            if (BizType == 1 & process == 2)
            {
                strCondition.Append("FETCH_JOB_CARD_AIR_IMP_ACTREV(JOB.job_card_trn_pk," + CurrPk + ") REV,");
            }

            if (BizType == 3 & process == 1)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_EXP_ACTREV(job.job_card_trn_pk ," + CurrPk + ") REV,");
                strCondition1.Append("FETCH_JOB_CARD_AIR_EXP_ACTREV(JOB.job_card_trn_pk ," + CurrPk + ") REV,");
            }
            if (BizType == 3 & process == 2)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_IMP_ACTREV(JOB.job_card_trn_pk ," + CurrPk + ") REV,");
                strCondition1.Append("FETCH_JOB_CARD_AIR_IMP_ACTREV(JOB.job_card_trn_pk ," + CurrPk + ") REV,");
            }
            if (BizType == 2 & process == 0)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_EXP_ACTREV(job.job_card_trn_pk ," + CurrPk + ") REV,");
                strCondition1.Append("FETCH_JOB_CARD_SEA_IMP_ACTREV(JOB.job_card_trn_pk ," + CurrPk + ") REV,");
            }
            if (BizType == 1 & process == 0)
            {
                strCondition.Append("FETCH_JOB_CARD_AIR_EXP_ACTREV(JOB.job_card_trn_pk ," + CurrPk + ") REV,");
                strCondition1.Append("FETCH_JOB_CARD_AIR_IMP_ACTREV(JOB.job_card_trn_pk ," + CurrPk + ") REV,");
            }
            if (BizType == 3 & process == 0)
            {
                strCondition.Append("FETCH_JOB_CARD_SEA_EXP_ACTREV(job.job_card_trn_pk ," + CurrPk + ") REV,");
                strCondition1.Append("FETCH_JOB_CARD_SEA_IMP_ACTREV(JOB.job_card_trn_pk ," + CurrPk + ") REV,");
                strCondition2.Append("FETCH_JOB_CARD_AIR_EXP_ACTREV(JOB.job_card_trn_pk ," + CurrPk + ") REV,");
                strCondition3.Append("FETCH_JOB_CARD_AIR_IMP_ACTREV(JOB.job_card_trn_pk ," + CurrPk + ") REV,");
            }

            strCondition.Append(" CMT.CUSTOMER_MST_PK CUST_PK");
            strCondition.Append(" FROM ");
            strCondition.Append(" COLLECTIONS_TBL COL, ");
            strCondition.Append(" COLLECTIONS_TRN_TBL CTRN, ");
            strCondition.Append(" CONSOL_INVOICE_TBL INV, ");
            strCondition.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");

            if (BizType == 3 | process == 0)
            {
                strCondition1.Append(" CMT.CUSTOMER_MST_PK CUST_PK");
                strCondition1.Append(" FROM");
                strCondition1.Append(" COLLECTIONS_TBL COL, ");
                strCondition1.Append(" COLLECTIONS_TRN_TBL CTRN, ");
                strCondition1.Append(" CONSOL_INVOICE_TBL INV, ");
                strCondition1.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");

                strCondition2.Append(" CMT.CUSTOMER_MST_PK CUST_PK");
                strCondition2.Append(" FROM");
                strCondition2.Append(" COLLECTIONS_TBL COL, ");
                strCondition2.Append(" COLLECTIONS_TRN_TBL CTRN, ");
                strCondition2.Append(" CONSOL_INVOICE_TBL INV, ");
                strCondition2.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");

                strCondition3.Append(" CMT.CUSTOMER_MST_PK CUST_PK");
                strCondition3.Append(" FROM");
                strCondition3.Append(" COLLECTIONS_TBL COL, ");
                strCondition3.Append(" COLLECTIONS_TRN_TBL CTRN, ");
                strCondition3.Append(" CONSOL_INVOICE_TBL INV, ");
                strCondition3.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");
            }

            if (BizType == 2 & process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 1 & process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HAWB_EXP_TBL            HAWB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 2 & process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 1 & process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 3 & process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

                strCondition1.Append(" JOB_CARD_TRN   JOB,");
                strCondition1.Append(" HAWB_EXP_TBL            HAWB,");
                strCondition1.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition1.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition1.Append(" USER_MST_TBL           UMT");
                strCondition1.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition1.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                strCondition1.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                strCondition1.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition1.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition1.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition1.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 3 & process == 2)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

                strCondition1.Append(" JOB_CARD_TRN   JOB,");
                strCondition1.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition1.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition1.Append(" USER_MST_TBL           UMT");
                strCondition1.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition1.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition1.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition1.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition1.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition1.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 2 & process == 0)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append(" AND INV.PROCESS_TYPE=1 ");
                strCondition.Append(" AND INV.BUSINESS_TYPE=2 ");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

                strCondition1.Append(" JOB_CARD_TRN   JOB,");
                strCondition1.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition1.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition1.Append(" USER_MST_TBL           UMT,");
                strCondition1.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition1.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition1.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                strCondition1.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition1.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition1.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition1.Append(" AND INV.PROCESS_TYPE=2 ");
                strCondition1.Append(" AND INV.BUSINESS_TYPE=2 ");
                strCondition1.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition1.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

            }

            if (BizType == 1 & process == 0)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HAWB_EXP_TBL            HAWB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND INV.PROCESS_TYPE=1 ");
                strCondition.Append(" AND INV.BUSINESS_TYPE=1 ");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

                strCondition1.Append(" JOB_CARD_TRN   JOB,");
                strCondition1.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition1.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition1.Append(" USER_MST_TBL           UMT");
                strCondition1.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition1.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition1.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition1.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition1.Append(" AND INV.PROCESS_TYPE=2 ");
                strCondition1.Append(" AND INV.BUSINESS_TYPE=1 ");
                strCondition1.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition1.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }

            if (BizType == 3 & process == 0)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT,");
                strCondition.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+) ");
                strCondition.Append(" AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
                strCondition.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition.Append(" AND INV.PROCESS_TYPE=1 ");
                strCondition.Append(" AND INV.BUSINESS_TYPE=2 ");
                strCondition.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

                strCondition1.Append(" JOB_CARD_TRN   JOB,");
                strCondition1.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition1.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition1.Append(" USER_MST_TBL           UMT,");
                strCondition1.Append(" VESSEL_VOYAGE_TRN      VTRN");
                strCondition1.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition1.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                strCondition1.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition1.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition1.Append(" AND JOB.VOYAGE_TRN_FK=VTRN.VOYAGE_TRN_PK(+)");
                strCondition1.Append(" AND INV.PROCESS_TYPE=2 ");
                strCondition1.Append(" AND INV.BUSINESS_TYPE=2 ");
                strCondition1.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition1.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

                strCondition2.Append(" JOB_CARD_TRN   JOB,");
                strCondition2.Append(" HAWB_EXP_TBL            HAWB,");
                strCondition2.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition2.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition2.Append(" USER_MST_TBL           UMT");
                strCondition2.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition2.Append(" AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                strCondition2.Append(" AND INV.CUSTOMER_MST_FK= CMT.CUSTOMER_MST_PK");
                strCondition2.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition2.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition2.Append(" AND INV.PROCESS_TYPE=1 ");
                strCondition2.Append(" AND INV.BUSINESS_TYPE=1 ");
                strCondition2.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition2.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");

                strCondition3.Append(" JOB_CARD_TRN   JOB,");
                strCondition3.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition3.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition3.Append(" USER_MST_TBL           UMT");
                strCondition3.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK(+)");
                strCondition3.Append(" AND INV.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                strCondition3.Append(" AND COL.COLLECTIONS_TBL_PK=CTRN.COLLECTIONS_TBL_FK ");
                strCondition3.Append(" AND CTRN.INVOICE_REF_NR(+)  =INV.INVOICE_REF_NO");
                strCondition3.Append(" AND INV.PROCESS_TYPE=2 ");
                strCondition3.Append(" AND INV.BUSINESS_TYPE=1 ");
                strCondition3.Append(" AND (INV.INVOICE_DATE BETWEEN TO_DATE('" + FROM_DATE + "', DATEFORMAT)");
                strCondition3.Append(" AND TO_DATE(NVL('" + TO_DATE + "', '1/1/9999'), DATEFORMAT)) ");
            }
            strCondition.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
            strCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            strCondition.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            strCondition.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
            if (process != 0)
            {
                strCondition.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
            }
            if (BizType != 3)
            {
                strCondition.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");
            }
            if (CustomerPKS != "0")
            {
                strCondition.Append(" AND CMT.CUSTOMER_MST_PK IN (" + CustomerPKS + ")");
                strCondition1.Append(" AND CMT.CUSTOMER_MST_PK IN (" + CustomerPKS + ")");
                strCondition2.Append(" AND CMT.CUSTOMER_MST_PK IN (" + CustomerPKS + ")");
                strCondition3.Append(" AND CMT.CUSTOMER_MST_PK IN (" + CustomerPKS + ")");
            }
            if (BizType == 3 | process == 0)
            {
                strCondition1.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                strCondition1.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
                strCondition1.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                strCondition1.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                if (process != 0)
                {
                    strCondition1.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
                }
                if (BizType != 3)
                {
                    strCondition1.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");
                }
                strCondition2.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                strCondition2.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
                strCondition2.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                strCondition2.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                if (process != 0)
                {
                    strCondition2.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
                }
                if (BizType != 3)
                {
                    strCondition2.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");
                }
                strCondition3.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK(+)");
                strCondition3.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
                strCondition3.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
                strCondition3.Append(" AND INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK(+)");
                if (process != 0)
                {
                    strCondition3.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
                }
                if (BizType != 3)
                {
                    strCondition3.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");
                }
            }
            if (BizType != 3 & process != 0)
            {
                strCondition.Append(" ORDER BY 2 DESC");
            }
            StringBuilder sqlstr = new StringBuilder();
            if (BizType == 3 & process == 0)
            {
                sqlstr.Append("SELECT QRY.* FROM ");
                sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
                sqlstr.Append(" (select Customer , sum(Invoice),sum(Collection),sum(Balance),sum(REV),CUST_PK, 'false' SEL from (" + strCondition.ToString() + " union " + strCondition1.ToString() + " union " + strCondition2.ToString() + " union " + strCondition3.ToString() + " ");
                sqlstr.Append("  ) S group by  Customer ,CUST_PK order by Customer) T) QRY ");
            }
            else if (BizType == 3 | (BizType == 1 & process == 0) | (BizType == 2 & process == 0))
            {
                sqlstr.Append("SELECT QRY.* FROM ");
                sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
                sqlstr.Append(" (select Customer , sum(Invoice),sum(Collection),sum(Balance),sum(REV),CUST_PK, 'false' SEL from (" + strCondition.ToString() + "  union  " + strCondition1.ToString() + " ");
                sqlstr.Append("  ) S group by  Customer ,CUST_PK order by Customer) T) QRY ");
            }
            else
            {
                sqlstr.Append("SELECT QRY.* FROM ");
                sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
                sqlstr.Append(" (select Customer , sum(Invoice),sum(Collection),sum(Balance),sum(REV),CUST_PK from (" + strCondition.ToString() + " ");
                sqlstr.Append("  ) S group by  Customer ,CUST_PK order by Customer) T) QRY ");
            }
            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(sqlstr.ToString());
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

        #region "Relations for BNM"
        private void CreateRelationBNM(DataSet dsMain)
        {
            // Get the DataColumn objects from two DataTable objects in a DataSet.
            DataColumn parentCol = null;
            DataColumn childCol = null;
            try
            {
                parentCol = dsMain.Tables[0].Columns["GRAND_TOTAL"];
                childCol = dsMain.Tables[1].Columns["GRAND_TOTAL"];
                DataRelation relGT = null;
                relGT = new DataRelation("Total", parentCol, childCol);

                parentCol = dsMain.Tables[1].Columns["CUSTOMER_NAME"];
                childCol = dsMain.Tables[2].Columns["CUSTOMER_NAME"];
                DataRelation relCust = null;
                relCust = new DataRelation("Trade", parentCol, childCol);

                DataRelation relLoc = null;
                relLoc = new DataRelation("Cust", new DataColumn[] {
                    dsMain.Tables[2].Columns["LOCATION_NAME"],
                    dsMain.Tables[2].Columns["CUSTOMER_NAME"]
                }, new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["CUSTOMER_NAME"]
                });

                DataRelation relPOL = null;
                relPOL = new DataRelation("Loc", new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[3].Columns["POL"],
                    dsMain.Tables[3].Columns["POD"],
                    dsMain.Tables[3].Columns["VSLVOY"]
                }, new DataColumn[] {
                    dsMain.Tables[4].Columns["LOCATION_NAME"],
                    dsMain.Tables[4].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[4].Columns["POL"],
                    dsMain.Tables[4].Columns["POD"],
                    dsMain.Tables[4].Columns["VSLVOY"]
                });

                parentCol = dsMain.Tables[4].Columns["JOBCARD_REF_NO"];
                childCol = dsMain.Tables[5].Columns["JOBCARD_REF_NO"];
                DataRelation reljob = null;
                reljob = new DataRelation("JOBINV", new DataColumn[] {
                    dsMain.Tables[4].Columns["JOBCARD_REF_NO"],
                    dsMain.Tables[4].Columns["BIZ_TYPE"]
                }, new DataColumn[] {
                    dsMain.Tables[5].Columns["JOBCARD_REF_NO"],
                    dsMain.Tables[5].Columns["BIZ_TYPE"]
                });

                relGT.Nested = true;
                relPOL.Nested = true;
                relCust.Nested = true;
                relLoc.Nested = true;
                reljob.Nested = true;

                dsMain.Relations.Add(relGT);
                dsMain.Relations.Add(relCust);
                dsMain.Relations.Add(relLoc);
                dsMain.Relations.Add(relPOL);
                dsMain.Relations.Add(reljob);
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
        private void CreateRelationExclVslVoyBNM(DataSet dsMain)
        {
            DataColumn parentCol = null;
            DataColumn childCol = null;
            try
            {
                parentCol = dsMain.Tables[0].Columns["GRAND_TOTAL"];
                childCol = dsMain.Tables[1].Columns["GRAND_TOTAL"];
                DataRelation relGT = null;
                relGT = new DataRelation("Total", parentCol, childCol);

                parentCol = dsMain.Tables[1].Columns["CUSTOMER_NAME"];
                childCol = dsMain.Tables[2].Columns["CUSTOMER_NAME"];
                DataRelation relCust = null;
                relCust = new DataRelation("Trade", parentCol, childCol);

                DataRelation relLoc = null;
                relLoc = new DataRelation("Cust", new DataColumn[] {
                    dsMain.Tables[2].Columns["LOCATION_NAME"],
                    dsMain.Tables[2].Columns["CUSTOMER_NAME"]
                }, new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["CUSTOMER_NAME"]
                });

                parentCol = dsMain.Tables[3].Columns["JOBCARD_REF_NO"];
                childCol = dsMain.Tables[4].Columns["JOBCARD_REF_NO"];
                DataRelation reljob = null;

                reljob = new DataRelation("JOBINV", new DataColumn[] {
                    dsMain.Tables[3].Columns["JOBCARD_REF_NO"],
                    dsMain.Tables[3].Columns["BIZ_TYPE"]
                }, new DataColumn[] {
                    dsMain.Tables[4].Columns["JOBCARD_REF_NO"],
                    dsMain.Tables[4].Columns["BIZ_TYPE"]
                });

                relGT.Nested = true;
                relCust.Nested = true;
                relLoc.Nested = true;
                reljob.Nested = true;

                dsMain.Relations.Add(relGT);
                dsMain.Relations.Add(relCust);
                dsMain.Relations.Add(relLoc);
                dsMain.Relations.Add(reljob);

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
        private void CreateRelationGroupExclVslvoy(DataSet dsMain)
        {
            DataColumn parentCol = null;
            DataColumn childCol = null;
            try
            {
                parentCol = dsMain.Tables[0].Columns["GRAND_TOTAL"];
                childCol = dsMain.Tables[1].Columns["GRAND_TOTAL"];
                DataRelation relGT = null;
                relGT = new DataRelation("Total", parentCol, childCol);

                parentCol = dsMain.Tables[1].Columns["GROUP_CATEGORY"];
                childCol = dsMain.Tables[2].Columns["GROUP_CATEGORY"];

                DataRelation relGroupCat = null;
                relGroupCat = new DataRelation("GroupCat", parentCol, childCol);

                parentCol = dsMain.Tables[2].Columns["GRP_HDR_PK"];
                childCol = dsMain.Tables[3].Columns["GRP_HDR_PK"];

                DataRelation relGroup = null;
                relGroup = new DataRelation("Group", parentCol, childCol);

                DataRelation relLoc = null;
                relLoc = new DataRelation("Loc", new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[3].Columns["GRP_HDR_PK"]
                }, new DataColumn[] {
                    dsMain.Tables[4].Columns["LOCATION_NAME"],
                    dsMain.Tables[4].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[4].Columns["GRP_HDR_PK"]
                });


                DataRelation relCust = null;
                relCust = new DataRelation("Cust", new DataColumn[] {
                    dsMain.Tables[4].Columns["LOCATION_NAME"],
                    dsMain.Tables[4].Columns["CUSTOMER_NAME"]
                }, new DataColumn[] {
                    dsMain.Tables[5].Columns["LOCATION_NAME"],
                    dsMain.Tables[5].Columns["CUSTOMER_NAME"]
                });

                parentCol = dsMain.Tables[5].Columns["JOBCARD_REF_NO"];
                childCol = dsMain.Tables[6].Columns["JOBCARD_REF_NO"];
                DataRelation reljob = null;

                reljob = new DataRelation("JOBINV", new DataColumn[] {
                    dsMain.Tables[5].Columns["JOBCARD_REF_NO"],
                    dsMain.Tables[5].Columns["BIZ_TYPE"]
                }, new DataColumn[] {
                    dsMain.Tables[6].Columns["JOBCARD_REF_NO"],
                    dsMain.Tables[6].Columns["BIZ_TYPE"]
                });

                relGT.Nested = true;
                relGroupCat.Nested = true;
                relGroup.Nested = true;
                relCust.Nested = true;
                relLoc.Nested = true;
                reljob.Nested = true;

                dsMain.Relations.Add(relGT);
                dsMain.Relations.Add(relGroupCat);
                dsMain.Relations.Add(relGroup);
                dsMain.Relations.Add(relLoc);
                dsMain.Relations.Add(relCust);
                dsMain.Relations.Add(reljob);
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
        private void CreateRelationBNMGroup(DataSet dsMain)
        {
            DataColumn parentCol = null;
            DataColumn childCol = null;
            try
            {
                parentCol = dsMain.Tables[0].Columns["GRAND_TOTAL"];
                childCol = dsMain.Tables[1].Columns["GRAND_TOTAL"];
                DataRelation relGT = null;
                relGT = new DataRelation("Total", parentCol, childCol);

                parentCol = dsMain.Tables[1].Columns["GROUP_CATEGORY"];
                childCol = dsMain.Tables[2].Columns["GROUP_CATEGORY"];

                DataRelation relGroupCat = null;
                relGroupCat = new DataRelation("GroupCat", parentCol, childCol);

                parentCol = dsMain.Tables[2].Columns["GRP_HDR_PK"];
                childCol = dsMain.Tables[3].Columns["GRP_HDR_PK"];

                DataRelation relGroup = null;
                relGroup = new DataRelation("Group", parentCol, childCol);

                DataRelation relLoc = null;
                relLoc = new DataRelation("Loc", new DataColumn[] {
                    dsMain.Tables[3].Columns["LOCATION_NAME"],
                    dsMain.Tables[3].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[3].Columns["GRP_HDR_PK"]
                }, new DataColumn[] {
                    dsMain.Tables[4].Columns["LOCATION_NAME"],
                    dsMain.Tables[4].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[4].Columns["GRP_HDR_PK"]
                });


                DataRelation relCust = null;
                relCust = new DataRelation("Cust", new DataColumn[] {
                    dsMain.Tables[4].Columns["LOCATION_NAME"],
                    dsMain.Tables[4].Columns["CUSTOMER_NAME"]
                }, new DataColumn[] {
                    dsMain.Tables[5].Columns["LOCATION_NAME"],
                    dsMain.Tables[5].Columns["CUSTOMER_NAME"]
                });

                DataRelation relPOL = null;
                relPOL = new DataRelation("VSLVOY", new DataColumn[] {
                    dsMain.Tables[5].Columns["LOCATION_NAME"],
                    dsMain.Tables[5].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[5].Columns["POL"],
                    dsMain.Tables[5].Columns["POD"],
                    dsMain.Tables[5].Columns["VSLVOY"]
                }, new DataColumn[] {
                    dsMain.Tables[6].Columns["LOCATION_NAME"],
                    dsMain.Tables[6].Columns["CUSTOMER_NAME"],
                    dsMain.Tables[6].Columns["POL"],
                    dsMain.Tables[6].Columns["POD"],
                    dsMain.Tables[6].Columns["VSLVOY"]
                });

                parentCol = dsMain.Tables[6].Columns["JOBCARD_REF_NO"];
                childCol = dsMain.Tables[7].Columns["JOBCARD_REF_NO"];
                DataRelation reljob = null;

                reljob = new DataRelation("JOBINV", new DataColumn[] {
                    dsMain.Tables[6].Columns["JOBCARD_REF_NO"],
                    dsMain.Tables[6].Columns["BIZ_TYPE"]
                }, new DataColumn[] {
                    dsMain.Tables[7].Columns["JOBCARD_REF_NO"],
                    dsMain.Tables[7].Columns["BIZ_TYPE"]
                });
                relGT.Nested = true;
                relGroupCat.Nested = true;
                relGroup.Nested = true;
                relPOL.Nested = true;
                relCust.Nested = true;
                relLoc.Nested = true;
                reljob.Nested = true;

                dsMain.Relations.Add(relGT);
                dsMain.Relations.Add(relGroupCat);
                dsMain.Relations.Add(relGroup);
                dsMain.Relations.Add(relLoc);
                dsMain.Relations.Add(relCust);
                dsMain.Relations.Add(relPOL);
                dsMain.Relations.Add(reljob);
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
    }
}