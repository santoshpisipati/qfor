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
    public class clsRemovalConsolidatedInvoices : CommonFeatures
    {
        #region "Property"

        private long lngInvPk;
        public string uniqueReferenceNr;

        public long ReturnSavePk
        {
            get { return lngInvPk; }
            set { lngInvPk = value; }
        }

        #endregion "Property"

        #region "FetchCredteDays"

        public DataSet GetDtVat(Int32 invpk)
        {
            System.Text.StringBuilder strquery = new System.Text.StringBuilder();
            WorkFlow objWf = new WorkFlow();
            try
            {
                strquery.Append(" select c.invoice_date,cust.vat_no from consol_invoice_tbl c , customer_mst_tbl cust where ");
                strquery.Append(" c.customer_mst_fk=cust.customer_mst_pk and  c.consol_invoice_pk=" + invpk);
                return objWf.GetDataSet(strquery.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        public string GetInvCrday(Int32 invpk, string invdate, Int32 biztype, Int32 Process)
        {
            System.Text.StringBuilder strquery = new System.Text.StringBuilder();
            WorkFlow objWf = new WorkFlow();
            string strqry = null;
            DataSet dsinv = new DataSet();
            Int32 i = default(Int32);
            Int32 j = default(Int32);
            try
            {
                strquery.Append(" select J.BOOKING_SEA_FK,cust.customer_id,cust.credit_days from job_card_sea_exp_tbl j,");
                strquery.Append(" CUSTOMER_MST_TBL CUST where j.job_card_sea_exp_pk in ");
                strquery.Append(" (select c.job_card_fk  from consol_invoice_trn_tbl c where c.consol_invoice_fk= " + invpk);
                strquery.Append(" group by c.job_card_fk) and j.shipper_cust_mst_fk=cust.customer_mst_pk ");
                dsinv = objWf.GetDataSet(strquery.ToString());
                if (dsinv.Tables[0].Rows.Count > 1)
                {
                    strquery = strquery.Remove(0, strquery.Length - 1);

                    strquery.Append("  select to_char(c1.invoice_date+" + dsinv.Tables[0].Rows[0]["credit_days"] + " ,'dd/mm/yyyy') from consol_invoice_tbl c1 where c1.consol_invoice_pk= " + invpk);
                    return objWf.ExecuteScaler(strquery.ToString());
                }

                string strSQL = null;

                strSQL = " select b.credit_days,to_CHAR(con.invoice_date,'dd/mm/yyyy')invdt  , (case when b.credit_days>0 then ";
                strSQL += " to_CHAR(con.invoice_date+b.credit_days,'dd/mm/yyyy') end )crdate ";
                strSQL += " from job_card_sea_EXP_tbl j,booking_sea_tbl b,consol_invoice_tbl con,consol_invoice_trn_tbl inv ";

                if (Process == 2)
                {
                    if (biztype == 2)
                    {
                        strSQL += " , job_card_sea_imp_tbl impj ";
                    }
                    else
                    {
                        strSQL += " , job_card_air_imp_tbl impj ";
                    }
                }
                strSQL += " where inv.consol_invoice_fk= " + invpk;
                strSQL += "  and con.consol_invoice_pk=inv.consol_invoice_fk ";
                if (Process == 1)
                {
                    strSQL += " and inv.job_card_fk=j.job_card_sea_exp_pk and j.booking_sea_fk=b.booking_sea_pk";
                }
                else
                {
                    strSQL += " and inv.job_card_fk=impj.job_card_sea_exp_pk and j.booking_sea_fk=b.booking_sea_pk";
                }
                if (Process == 2)
                {
                    strSQL += " and impj.jobcard_ref_no=j.jobcard_ref_no";
                }
                if (biztype == 1)
                {
                    strSQL = strSQL.Replace("sea", "air");
                }
                if (Process == 2)
                {
                    strSQL = strSQL.Replace("exp", "imp");
                }
                dsinv = objWf.GetDataSet(strSQL);
                if (dsinv.Tables[0].Rows.Count > 0)
                {
                    return Convert.ToString(getDefault(dsinv.Tables[0].Rows[0][2], ""));
                }
                else
                {
                    if (Process == 2)
                    {
                        strqry = "  select distinct (case  when cust.credit_days > 0 then  to_CHAR(con.invoice_date +cust.credit_days,'dd/mm/yyyy')  ";
                        strqry += "    end) crdate   from  consol_invoice_tbl     con,consol_invoice_trn_tbl inv, ";
                        strqry += "  job_card_sea_imp_tbl   impj,   customer_mst_tbl cust  where inv.consol_invoice_fk = " + invpk;
                        strqry += "  and con.consol_invoice_pk = inv.consol_invoice_fk and inv.job_card_fk = impj.job_card_sea_imp_pk ";
                        strqry += "  and impj.consignee_cust_mst_fk=cust.customer_mst_pk ";
                        if (biztype == 1)
                        {
                            strqry = strqry.Replace("sea", "air");
                        }
                        return objWf.ExecuteScaler(strqry);
                    }
                    else
                    {
                        return "";
                    }
                }
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                return "";
            }
        }

        public string GetVatNo(string custname)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objWf = new WorkFlow();
            try
            {
                strsql.Append(" select co.vat_no from customer_mst_tbl co where co.customer_name like '" + custname + "'");
                return objWf.ExecuteScaler(strsql.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "FetchCredteDays"

        #region "Fetch Records"

        public DataSet FetchAllJCsForFreightPayers(string partyPK = "", int JobPK = 0, string JobCardDt = "")
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWf = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder strSelectionQuery = new System.Text.StringBuilder();
            //strSelectionQuery.Append("AND FRTPYR.CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM CUSTOMER_MST_TBL ) " & vbCrLf)
            try
            {
                if (JobPK > 0)
                {
                    strSelectionQuery.Append(" and JOB.JOB_CARD_PK = " + JobPK);
                }
                if (Convert.ToInt32(partyPK) > 0)
                {
                    strSelectionQuery.Append(" and CUST.CUSTOMER_MST_PK IN (" + partyPK + ")");
                }
                if (JobCardDt.Trim().Length > 0)
                {
                    strSelectionQuery.Append(" and TO_DATE(JOB.JOB_CARD_DATE,'" + dateFormat + "') = ");
                    strSelectionQuery.Append(" TO_DATE('" + JobCardDt + "','" + dateFormat + "') ");
                }
                
                strQuery.Append("SELECT JOB.JOB_CARD_PK JOBPK,");
                strQuery.Append("       JOB.JOB_CARD_REF JOBCARD,");
                strQuery.Append("       TO_CHAR(JOB.JOB_CARD_DATE,'" + dateFormat + "') JOBDATE,");
                strQuery.Append("       CUST.CUSTOMER_MST_PK PARTYPK,");
                strQuery.Append("       CUST.CUSTOMER_NAME PARTY,");
                strQuery.Append("       PLR.PLACE_CODE PLR,");
                strQuery.Append("       PFD.PLACE_CODE PFD,");
                strQuery.Append("       ROUND(SUM(NVL(JFD.FREIGHT_AMT, 0) * JFD.EXCHANGE_RATE),2) FRAMT,");
                strQuery.Append("       '' SEL");
                strQuery.Append("  FROM REM_T_JC_FRT_DTLS_TBL  JFD,");
                strQuery.Append("       REM_M_JOB_CARD_MST_TBL JOB,");
                strQuery.Append("       CUSTOMER_MST_TBL       CUST,");
                strQuery.Append("       REM_M_QUOT_MST_TBL     QUOT,");
                strQuery.Append("       PLACE_MST_TBL          PLR,");
                strQuery.Append("       PLACE_MST_TBL          PFD");
                strQuery.Append(" WHERE 1 = 1");
                strQuery.Append("   AND JFD.JOB_CARD_FK = JOB.JOB_CARD_PK");
                strQuery.Append("   AND (NVL(JFD.FREIGHT_AMT, 0) * JFD.EXCHANGE_RATE) > 0");
                strQuery.Append("   AND jfd.INV_REM_REF_PK is null");
                strQuery.Append("   AND JFD.FRTPAYER_CUST_MST_FK = CUST.CUSTOMER_MST_PK");
                strQuery.Append("   AND PLR.PLACE_PK = JOB.JOB_CARD_PLR_FK");
                strQuery.Append("   AND PFD.PLACE_PK = JOB.JOB_CARD_PFD_FK");
                strQuery.Append("   AND QUOT.QUOT_PK = JOB.JOB_CARD_QUOT_FK");
                strQuery.Append(strSelectionQuery);
                strQuery.Append(" GROUP BY JOB.JOB_CARD_PK,");
                strQuery.Append("          JOB.JOB_CARD_REF,");
                strQuery.Append("          JOB.JOB_CARD_DATE,");
                strQuery.Append("          CUST.CUSTOMER_NAME,");
                strQuery.Append("          PLR.PLACE_CODE,");
                strQuery.Append("          PFD.PLACE_CODE,CUST.CUSTOMER_MST_PK");

                return objWf.GetDataSet(strQuery.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        public DataSet FetchCurrId(int invpk)
        {
            System.Text.StringBuilder strsql = new System.Text.StringBuilder();
            WorkFlow objWf = new WorkFlow();
            try
            {
                strsql.Append(" select curr.currency_id CURRID ,curr.currency_mst_pk CURRPK  from REM_M_INVOICE_TBL con , currency_type_mst_tbl curr ");
                strsql.Append(" where con.currency_mst_fk=curr.currency_mst_pk");
                strsql.Append(" and con.REMOVALS_INVOICE_PK=" + invpk);
                return objWf.GetDataSet(strsql.ToString());
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

        public DataSet FetchAll(bool blnFetch, string strCustomer, string JCdate, string strJobNo, string strBLNo, short BizType, short Process)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                WorkFlow objWf = new WorkFlow();
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                strQuery.Append("SELECT JOB.JOB_CARD_SEA_EXP_PK JOBPK,");
                strQuery.Append("       JOB.JOBCARD_REF_NO JOBCARD,");
                strQuery.Append("       JOB.JOBCARD_DATE JOBDATE,");
                strQuery.Append("       CUST.CUSTOMER_ID SHIPPER,");
                strQuery.Append("       CNSGN.CUSTOMER_ID CONSIGNEE,");
                strQuery.Append("       POL.PORT_ID POL,");
                strQuery.Append("       POD.PORT_ID POD,");
                strQuery.Append("       NVL((NVL((SELECT SUM(NVL(JOBFD.FREIGHT_AMT, 0)*JOBFD.EXCHANGE_RATE)");
                strQuery.Append("           FROM JOB_TRN_SEA_EXP_FD JOBFD");
                strQuery.Append("          WHERE JOBFD.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_SEA_EXP_PK");

                if (Process == 1)
                {
                    strQuery.Append("            AND JOBFD.FREIGHT_TYPE = 1");
                }
                else
                {
                    strQuery.Append("            AND JOBFD.FREIGHT_TYPE = 2");
                }
                strQuery.Append("            AND (JOBFD.INVOICE_SEA_TBL_FK IS NULL AND");
                strQuery.Append("                JOBFD.CONSOL_INVOICE_TRN_FK IS NULL AND");
                strQuery.Append("                JOBFD.INV_AGENT_TRN_SEA_EXP_FK IS NULL)),0) +");
                strQuery.Append("       NVL((SELECT SUM(NVL(JOBOTH.AMOUNT, 0)*JOBOTH.exchange_rate)");
                strQuery.Append("              FROM JOB_TRN_SEA_EXP_OTH_CHRG JOBOTH");
                strQuery.Append("             WHERE JOBOTH.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_SEA_EXP_PK");
                strQuery.Append("               AND (JOBOTH.INV_CUST_TRN_SEA_EXP_FK IS NULL AND");
                strQuery.Append("                   JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL AND");
                strQuery.Append("                   JOBOTH.INV_AGENT_TRN_SEA_EXP_FK IS NULL)),");
                strQuery.Append("            0)),0) FRAMT,");
                strQuery.Append("       0 SEL,");
                if (Process == 1)
                {
                    strQuery.Append("       CUST.CUSTOMER_NAME CUST,");
                    strQuery.Append("       CUST.CUSTOMER_MST_PK CUST_PK");
                }
                else
                {
                    strQuery.Append("       CNSGN.CUSTOMER_NAME CUST,");
                    strQuery.Append("       CNSGN.CUSTOMER_MST_PK CUST_PK");
                }
                strQuery.Append("  FROM JOB_CARD_SEA_EXP_TBL JOB,");
                if (Process == 1)
                {
                    strQuery.Append("       BOOKING_SEA_TBL      BKG,");
                }
                strQuery.Append("       PORT_MST_TBL         POL,");
                strQuery.Append("       PORT_MST_TBL         POD,");
                strQuery.Append("       CUSTOMER_MST_TBL     CUST,");
                strQuery.Append("       CUSTOMER_MST_TBL     CNSGN,");
                strQuery.Append("       USER_MST_TBL        UMT");
                strQuery.Append(" WHERE 1 = 1");
                if (Process == 1)
                {
                    strQuery.Append("   AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                }
                strQuery.Append("   AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK");
                strQuery.Append("   AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
                strQuery.Append("   AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK");
                strQuery.Append("   AND JOB.CONSIGNEE_CUST_MST_FK = CNSGN.CUSTOMER_MST_PK");
                strQuery.Append("  AND NVL((NVL((SELECT SUM(NVL(JOBFD.FREIGHT_AMT, 0)*JOBFD.exchange_rate)");
                strQuery.Append("           FROM JOB_TRN_SEA_EXP_FD JOBFD");
                strQuery.Append("          WHERE JOBFD.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_SEA_EXP_PK");
                if (Process == 1)
                {
                    strQuery.Append("            AND JOBFD.FREIGHT_TYPE = 1");
                }
                else
                {
                    strQuery.Append("            AND JOBFD.FREIGHT_TYPE = 2");
                }
                strQuery.Append("            AND (JOBFD.INVOICE_SEA_TBL_FK IS NULL AND");
                strQuery.Append("                JOBFD.CONSOL_INVOICE_TRN_FK IS NULL AND");
                strQuery.Append("                JOBFD.INV_AGENT_TRN_SEA_EXP_FK IS NULL)),0) +");
                strQuery.Append("       NVL((SELECT SUM(NVL(JOBOTH.AMOUNT, 0)*JOBOTH.exchange_rate)");
                strQuery.Append("              FROM JOB_TRN_SEA_EXP_OTH_CHRG JOBOTH");
                strQuery.Append("             WHERE JOBOTH.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_SEA_EXP_PK");
                strQuery.Append("               AND (JOBOTH.INV_CUST_TRN_SEA_EXP_FK IS NULL AND");
                strQuery.Append("                   JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL AND");
                strQuery.Append("                   JOBOTH.INV_AGENT_TRN_SEA_EXP_FK IS NULL)),");
                strQuery.Append("            0)),0) >0");
                strQuery.Append(" AND UMT.USER_MST_PK=JOB.CREATED_BY_FK ");
                //AND UMT.DEFAULT_LOCATION_FK=
                //Export
                if (Process == 1)
                {
                    strQuery.Append(" AND UMT.DEFAULT_LOCATION_FK = " + LoggedIn_Loc_FK);
                }
                //Sea Export
                if (BizType == 1 & Process == 1)
                {
                    strQuery.Replace("SEA", "AIR");
                    strQuery.Replace("INVOICE_AIR_TBL_FK", "INV_CUST_TRN_AIR_EXP_FK");
                    //Air Export
                }
                else if (BizType == 1 & Process == 2)
                {
                    strQuery.Replace("SEA", "AIR");
                    strQuery.Replace("INVOICE_AIR_TBL_FK", "INV_CUST_TRN_AIR_IMP_FK");
                    strQuery.Replace("EXP", "IMP");
                    strQuery.Replace("BKG", "JOB");
                    // Sea Import
                }
                else if (BizType == 2 & Process == 2)
                {
                    strQuery.Replace("EXP", "IMP");
                    strQuery.Replace("BKG", "JOB");
                }

                if (strCustomer.Length > 0 && Process == 1)
                {
                    strQuery.Append(" and cust.customer_id = '" + strCustomer + "' ");
                }
                else if (strCustomer.Length > 0)
                {
                    strQuery.Append(" and CNSGN.customer_id = '" + strCustomer + "' ");
                }

                if (JCdate.Trim().Length > 0 && Process == 1)
                {
                    strQuery.Append(" and TO_DATE(TO_CHAR(JOB.JOBCARD_DATE,DATEFORMAT),DATEFORMAT) = TO_DATE('" + JCdate + "' ,'" + dateFormat + "')");
                    //" &  & "'")
                }

                if (strJobNo.Length > 0)
                {
                    strQuery.Append(" and job.jobcard_ref_no = '" + strJobNo + "'");
                }

                if (strBLNo.Length > 0)
                {
                    strQuery.Append(" ");
                }

                if (blnFetch == false)
                {
                    strQuery.Append(" and   1=2 ");
                }
                strQuery.Append("   ORDER BY JOBPK DESC");

                return objWf.GetDataSet(strQuery.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        public DataSet FetchConsolidatable(string strJobPks, string CustPk, bool Edit = false, int ExType = 1, string Mode = "New")
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWk = new WorkFlow();

            strBuilder.Append(" select JOB_CARD_REF ,JOB_CARD_DATE,JC_FRT_DTLS_PK,JOBFK,FREIGHT_ELEMENT_MST_FK,");
            strBuilder.Append(" CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PC,FREIGHT_AMT, CURRENCY_ID,INV_AMT, CHK from ");
            strBuilder.Append(" ( SELECT distinct JOBFRT.Freight_Element_Mst_Fk, ");
            strBuilder.Append(" JOB.JOB_CARD_REF,");
            strBuilder.Append(" JOB.JOB_CARD_DATE,");

            strBuilder.Append(" JOBFRT.JC_FRT_DTLS_PK,");
            strBuilder.Append(" JOBFRT.JOB_CARD_FK JOBFK,");

            strBuilder.Append(" JOBFRT.Currency_Mst_Fk,");
            strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,");
            strBuilder.Append(" DECODE(JOBFRT.FREIGHT_TYPE,1,'P',2,'C') AS PC,");
            strBuilder.Append(" JOBFRT.FREIGHT_AMT,");
            strBuilder.Append(" CUMT.CURRENCY_ID,");

            strBuilder.Append(" (CASE ");
            strBuilder.Append(" WHEN (JOBFRT.inv_rem_ref_pk IS NULL ");
            strBuilder.Append(" ) THEN NULL ");
            strBuilder.Append(" WHEN JOBFRT.inv_rem_ref_pk IS NOT NULL THEN ");
            strBuilder.Append("   (SELECT TRN.invoice_amt from rem_m_invoice_tbl TRN  ");
            strBuilder.Append("   WHERE TRN.removals_invoice_pk=JOBFRT.Inv_Rem_Ref_Pk )");
            strBuilder.Append("    END) INV_AMT, ");
            strBuilder.Append("( CASE");
            strBuilder.Append(" WHEN JOBFRT.inv_rem_ref_pk IS NULL THEN");
            strBuilder.Append(" 'False'");
            strBuilder.Append(" ELSE");
            strBuilder.Append(" 'True' ");
            strBuilder.Append(" END) CHK");

            strBuilder.Append(" FROM ");
            strBuilder.Append(" rem_m_job_card_mst_tbl JOB, ");
            strBuilder.Append(" rem_t_jc_frt_dtls_tbl JOBFRT,");
            strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,");
            strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT ");
            if (Mode != "New")
            {
                strBuilder.Append(" , REM_M_INVOICE_TBL       INV, ");
                strBuilder.Append(" REM_INVOICE_TRN_TBL     INVTRN ");
            }
            strBuilder.Append(" WHERE");
            strBuilder.Append(" JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK ");
            strBuilder.Append(" AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK ");
            strBuilder.Append(" AND JOB.JOB_CARD_PK = JOBFRT.JOB_CARD_FK ");
            if (Mode != "New")
            {
                strBuilder.Append(" AND INVTRN.REMOVALS_INVOICE_FK = INV.REMOVALS_INVOICE_PK ");
                strBuilder.Append(" AND INVTRN.JOB_CARD_FK = JOB.JOB_CARD_PK ");
            }
            //strBuilder.Append(" AND JOBFRT.FREIGHT_TYPE=1 ")

            strBuilder.Append(" AND JOBFRT.JOB_CARD_FK IN (" + strJobPks + ")");
            strBuilder.Append(" AND JOBFRT.Frtpayer_Cust_Mst_Fk in ( " + CustPk + "))");
            try
            {
                return objWk.GetDataSet("SELECT Q.* FROM ( " + strBuilder.ToString() + " )Q  ");
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        public string FetchCreditLimit(string CustPk)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                WorkFlow objWF = new WorkFlow();
                strQuery.Append(" select distinct nvl(cmt.credit_limit,0) from ");
                strQuery.Append("  customer_mst_tbl cmt,");
                strQuery.Append(" rem_t_jc_frt_dtls_tbl JOBFRT");
                strQuery.Append(" where  cmt.customer_mst_pk=JOBFRT.FRTPAYER_CUST_MST_FK ");
                strQuery.Append(" and JOBFRT.FRTPAYER_CUST_MST_FK in ( " + CustPk + ")");
                return objWF.ExecuteScaler(strQuery.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        public DataSet FetchInvoiceData(string strJobPks, int intInvPk, int nBaseCurrPK, int UserPk, string CustPk, string CreditLimit, string amount, int ExType = 1, int JCPK = 0)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;
            Int32 rowcunt = 0;
            Int32 Contpk = 0;
            try
            {
                if (intInvPk == 0)
                {
                    strQuery.Append(" select type,JOB_CARD_REF,pk,jobcard_fk,freight_or_oth,element_fk,currency_mst_fk,element_name,element,currency_id, ");
                    strQuery.Append("curr,amount,exchange_rate,inv_amount,vat_code,vat_percent,tax_amount,total_amount,remarks,mode1 as \"MODE\",chk from ( ");
                    strQuery.Append(" SELECT distinct JOBFRT.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,'FREIGHT' AS TYPE,");
                    strQuery.Append(" JOB.JOB_CARD_REF,");
                    strQuery.Append("       JOBFRT.JC_FRT_DTLS_PK AS PK,");
                    strQuery.Append("       JOBFRT.JOB_CARD_FK AS JOBCARD_FK,");
                    strQuery.Append("       1 FREIGHT_OR_OTH,");
                    strQuery.Append("       JOBFRT.CURRENCY_MST_FK,");
                    strQuery.Append("       FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,");
                    strQuery.Append("       '' AS ELEMENT,");
                    strQuery.Append("       CUMT.CURRENCY_ID,");
                    strQuery.Append("       '' AS CURR,");
                    strQuery.Append("       JOBFRT.FREIGHT_AMT AS AMOUNT,");
                    strQuery.Append("       JOBFRT.EXCHANGE_RATE,");
                    strQuery.Append("       JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE AS INV_AMOUNT,");
                    strQuery.Append("NVL((select Distinct(frtv.vat_code) from frt_vat_country_tbl frtv,");
                    strQuery.Append("      user_mst_tbl umt,location_mst_tbl loc");
                    strQuery.Append("where umt.default_location_fk = loc.location_mst_pk");
                    strQuery.Append(" and loc.country_mst_fk = frtv.country_mst_fk");
                    strQuery.Append("and umt.user_mst_pk = " + UserPk + " ");
                    strQuery.Append("and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)");
                    strQuery.Append("),'') AS VAT_CODE,");
                    strQuery.Append("NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,");
                    strQuery.Append("      user_mst_tbl umt,location_mst_tbl loc");
                    strQuery.Append("where umt.default_location_fk = loc.location_mst_pk");
                    strQuery.Append(" and loc.country_mst_fk = frtv.country_mst_fk");
                    strQuery.Append("and umt.user_mst_pk = " + UserPk + " ");
                    strQuery.Append("and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)");
                    strQuery.Append("),null) AS VAT_PERCENT,");
                    strQuery.Append("(NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,");
                    strQuery.Append("      user_mst_tbl umt,location_mst_tbl loc");
                    strQuery.Append("where umt.default_location_fk = loc.location_mst_pk");
                    strQuery.Append(" and loc.country_mst_fk = frtv.country_mst_fk");
                    strQuery.Append("and umt.user_mst_pk = " + UserPk + " ");
                    strQuery.Append("and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)");
                    strQuery.Append("),0)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100)  AS TAX_AMOUNT,");
                    strQuery.Append("((NVL((select Distinct(frtv.vat_percentage) from frt_vat_country_tbl frtv,");
                    strQuery.Append("      user_mst_tbl umt,location_mst_tbl loc");
                    strQuery.Append("where umt.default_location_fk = loc.location_mst_pk");
                    strQuery.Append(" and loc.country_mst_fk = frtv.country_mst_fk");
                    strQuery.Append("and umt.user_mst_pk =" + UserPk + " ");
                    strQuery.Append("and FMT.freight_element_mst_pk = frtv.freight_element_mst_fk(+)");
                    strQuery.Append("),0)*(JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) + JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) AS TOTAL_AMOUNT,");
                    strQuery.Append("       '' AS REMARKS,");
                    strQuery.Append("       'NEW' AS \"MODE1\",");
                    strQuery.Append("       'FALSE' AS CHK ");
                    strQuery.Append("  FROM rem_t_jc_frt_dtls_tbl   JOBFRT,");
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                    strQuery.Append("       CORPORATE_MST_TBL       CORP,");
                    strQuery.Append("        rem_m_job_card_mst_tbl  JOB ");
                    strQuery.Append("        WHERE JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                    strQuery.Append("        AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                    strQuery.Append("  AND JOB.JOB_CARD_PK=JOBFRT.JOB_CARD_FK  ");
                    strQuery.Append("  AND JOBFRT.INV_REM_REF_PK IS NULL  ");
                    strQuery.Append("    AND JOBFRT.JOB_CARD_FK IN (" + strJobPks + ")");
                    strQuery.Append("   AND JOBFRT.Frtpayer_Cust_Mst_Fk in ( " + CustPk + "))");
                }
                else
                {
                    strQuery.Append(" select Q.* from ( ");
                    strQuery.Append(" select type,JOB_CARD_REF,pk,jobcard_fk,freight_or_oth,element_fk,currency_mst_fk,element_name,element,currency_id, ");
                    strQuery.Append(" curr,amount,exchange_rate,inv_amount,vat_code,tax_percent,tax_amount,total_amount,NOTES,mode1 as \"MODE\",chk from ( ");
                    strQuery.Append(" SELECT  distinct TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK, DECODE(TRN.FRT_OTH_ELEMENT_FK, 1, 'COST', 2, 'FREIGHT', 3, 'OTHER') AS TYPE,");
                    strQuery.Append("     JOB.JOB_CARD_REF,");
                    strQuery.Append(" TRN.REMOVALS_INVOICE_TRN_PK AS PK,");
                    strQuery.Append("       TRN.JOB_CARD_FK AS JOBCARD_FK,");
                    strQuery.Append("       '' FREIGHT_OR_OTH,");
                    strQuery.Append("       TRN.CURRENCY_MST_FK,");
                    strQuery.Append("    trn.frt_desc AS ELEMENT_NAME,");
                    strQuery.Append("       '' AS ELEMENT,");
                    strQuery.Append("       CUMT.CURRENCY_ID,");
                    strQuery.Append("       '' AS CURR,");
                    strQuery.Append("       TRN.ELEMENT_AMT AS AMOUNT,");
                    strQuery.Append("       ROUND((CASE TRN.ELEMENT_AMT");
                    strQuery.Append("               WHEN 0 THEN");
                    strQuery.Append("1");
                    strQuery.Append("               ELSE");
                    strQuery.Append("                TRN.AMT_IN_INV_CURR / TRN.ELEMENT_AMT");
                    strQuery.Append("             END),");
                    strQuery.Append("             6) AS EXCHANGE_RATE,");
                    strQuery.Append("       TRN.AMT_IN_INV_CURR AS INV_AMOUNT,");
                    strQuery.Append("       TRN.VAT_CODE AS VAT_CODE,");
                    strQuery.Append("       TRN.TAX_PCNT AS TAX_PERCENT,");
                    strQuery.Append("       TRN.TAX_AMT AS TAX_AMOUNT,");
                    strQuery.Append("       TRN.TOT_AMT AS TOTAL_AMOUNT,");
                    strQuery.Append("       TRN.NOTES,");
                    strQuery.Append("       'EDIT' AS \"MODE1\",");
                    strQuery.Append("       'TRUE' AS CHK ");
                    strQuery.Append("  FROM REM_INVOICE_TRN_TBL TRN,");
                    strQuery.Append("       REM_M_INVOICE_TBL     HDR,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL  CUMT,freight_element_mst_tbl fmt,rem_t_jc_frt_dtls_tbl JOBFRT,CONTAINER_TYPE_MST_TBL CON,PARAMETERS_TBL PAR, ");
                    strQuery.Append("        rem_m_job_card_mst_tbl JOB");
                    strQuery.Append(" WHERE TRN.REMOVALS_INVOICE_FK = HDR.REMOVALS_INVOICE_PK");
                    strQuery.Append("   AND TRN.REMOVALS_INVOICE_FK =" + intInvPk);
                    strQuery.Append("   AND JOB.JOB_CARD_PK IN (" + strJobPks + ")");
                    strQuery.Append("   AND TRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                    strQuery.Append("   AND JOB.JOB_CARD_PK=TRN.JOB_CARD_FK");
                    strQuery.Append("   AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                    if ((Convert.ToInt32(CreditLimit) > 0 & Convert.ToInt32(CreditLimit) > Convert.ToInt32(amount)))
                    {
                        strQuery.Append(" and (FMT.CREDIT=0 or  FMT.CREDIT=1)  ");
                    }
                    strQuery.Append(" AND TRN.JOB_CARD_FK  = JOBFRT.JOB_CARD_FK ");
                    if (rowcunt <= 0)
                    {
                        strQuery.Append(" and jobfrt.INV_REM_REF_PK=trn.REMOVALS_INVOICE_TRN_PK ");
                    }
                    strQuery.Append("  )) Q  ");
                }
                DS = objWK.GetDataSet(strQuery.ToString());
                return DS;
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        public Int32 FetchDetContPk(int Jobpk, Int32 invPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            DataSet dspk = null;
            Int32 i = default(Int32);
            string strpk = "";
            try
            {
                strBuilder.Append(" SELECT MAX(FD.JOB_TRN_SEA_IMP_FD_PK) FROM JOB_TRN_SEA_IMP_FD FD WHERE  ");
                strBuilder.Append(" FD.JOB_CARD_SEA_IMP_FK IN (" + Jobpk + ")");
                return Convert.ToInt32(objWK.ExecuteScaler(strBuilder.ToString()));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        public DataSet FetchHeader(int invPk)
        {
            try
            {
                WorkFlow objWK = new WorkFlow();
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                strBuilder.Append(" select cm.customer_name cust, ct.currency_id Cur,");
                strBuilder.Append(" ci.INVOICE_REF_NO REFNO, ");
                strBuilder.Append(" ci.INVOICE_DATE CDATE, ");
                strBuilder.Append(" ci.INVOICE_AMT INVAMT,");
                strBuilder.Append(" ci.DISCOUNT_AMT DISAMT, ");
                strBuilder.Append(" ci.NET_RECEIVABLE NET,ci.INV_UNIQUE_REF_NR, ");
                strBuilder.Append(" ci.NOTES ");
                strBuilder.Append(" from REM_M_INVOICE_TBL ci,customer_mst_tbl cm, currency_type_mst_tbl ct");
                strBuilder.Append(" where ci.REMOVALS_INVOICE_PK = " + invPk + "");
                strBuilder.Append(" and cm.customer_mst_pk = ci.customer_mst_fk and");
                strBuilder.Append(" ct.currency_mst_pk = ci.currency_mst_fk");
                return objWK.GetDataSet(strBuilder.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "Fetch Records"

        #region "Save"

        public DataSet FetchAmount(string refno)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strsql = new StringBuilder();
            StringBuilder sqlinv = new StringBuilder();
            DataSet dsinvoice = new DataSet();
            try
            {
                strsql.Append(" select con.discount_amt disamt,con.net_receivable netamt , cur.currency_id currid  ");
                strsql.Append(" from consol_invoice_tbl con ,currency_type_mst_tbl cur where con.invoice_ref_no like '" + refno + "' ");
                strsql.Append(" and con.currency_mst_fk=cur.currency_mst_pk ");
                dsinvoice.Tables.Add(objWK.GetDataTable(strsql.ToString()));
                sqlinv.Append(" select sum(cc.tax_amt) taxamt ,sum(cc.amt_in_inv_curr) invamt from consol_invoice_trn_tbl cc,consol_invoice_tbl c where ");
                sqlinv.Append(" c.invoice_ref_no like '" + refno + "' and c.consol_invoice_pk=cc.consol_invoice_fk");
                dsinvoice.Tables.Add(objWK.GetDataTable(sqlinv.ToString()));
                return dsinvoice;
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        public int SaveData(DataSet dsSave, object InvRefNo, long nLocationPk, string CREATED_BY_FK, long nEmpId, double CrLimit, string Customer, double NetAmt, double CrLimitUsed, int extype = 1,
        string uniqueRefNr = "")
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();

            int intSaveSucceeded = 0;
            OracleTransaction TRAN = null;
            int intPkValue = 0;
            int intChldCnt = 0;
            Int32 cargotype = default(Int32);
            bool chkFlag = false;
            try
            {
                if (string.IsNullOrEmpty(Convert.ToString(InvRefNo)))
                {
                    InvRefNo = GenerateInvoiceNo(nLocationPk, nEmpId, Convert.ToInt64(dsSave.Tables[0].Rows[0]["CREATED_BY_FK_IN"]), objWK);
                    chkFlag = true;
                    if (Convert.ToString(InvRefNo) == "Protocol Not Defined.")
                    {
                        InvRefNo = "";
                        return -1;
                    }
                }
                TRAN = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = TRAN;

                int UNIQUE = 0;
                if (string.IsNullOrEmpty(uniqueRefNr))
                {
                    uniqueRefNr = GetVEKInvoiceRef(10000000, 99999999);
                    //while (IsUniqueRefNr(uniqueRefNr, objWK.MyCommand) > 0)
                    //{
                    //    uniqueRefNr = GetVEKInvoiceRef(10000000, 99999999);
                    //}
                }
                uniqueReferenceNr = uniqueRefNr;

                var _with1 = dsSave.Tables[0].Rows[0];
                objWK.MyCommand.Connection = objWK.MyConnection;
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;

                objWK.MyCommand.CommandText = objWK.MyUserName + ".REMOVAL_CONSOL_INV_PKG.REMOVAL_CONSOL_INV_HDR_INS";

                objWK.MyCommand.Parameters.Clear();
                objWK.MyCommand.Parameters.Add("CUSTOMER_MST_FK_IN", Convert.ToInt32(_with1["CUSTOMER_MST_FK_IN"]));
                objWK.MyCommand.Parameters.Add("INVOICE_REF_NO_IN", Convert.ToString(InvRefNo));
                objWK.MyCommand.Parameters.Add("INVOICE_DATE_IN", _with1["INVOICE_DATE_IN"]);
                objWK.MyCommand.Parameters.Add("CURRENCY_MST_FK_IN", _with1["CURRENCY_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("INVOICE_AMT_IN", _with1["INVOICE_AMT_IN"]);
                objWK.MyCommand.Parameters.Add("DISCOUNT_AMT_IN", _with1["DISCOUNT_AMT_IN"]);
                objWK.MyCommand.Parameters.Add("NET_RECEIVABLE_IN", _with1["NET_RECEIVABLE_IN"]);
                objWK.MyCommand.Parameters.Add("REMARKS_IN", _with1["REMARKS_IN"]);
                objWK.MyCommand.Parameters.Add("CREATED_BY_FK_IN", _with1["CREATED_BY_FK_IN"]);
                objWK.MyCommand.Parameters.Add("EXCH_RATE_TYPE_PK_IN", getDefault(extype, ""));
                objWK.MyCommand.Parameters.Add("INV_UNIQUE_REF_NR_IN", getDefault(uniqueRefNr, ""));
                objWK.MyCommand.Parameters.Add("RETURN_VALUE", _with1["CREATED_BY_FK_IN"]).Direction = ParameterDirection.Output;
                intSaveSucceeded = objWK.MyCommand.ExecuteNonQuery();
                intPkValue = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                lngInvPk = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);

                for (intChldCnt = 0; intChldCnt <= dsSave.Tables[1].Rows.Count - 1; intChldCnt++)
                {
                    objWK.MyCommand.Parameters.Clear();
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".REMOVAL_CONSOL_INV_PKG.REMOVAL_CONSOL_INV_DETAILS_INS";

                    var _with2 = dsSave.Tables[1].Rows[intChldCnt];
                    objWK.MyCommand.Parameters.Add("CONSOL_INVOICE_FK_IN", intPkValue);
                    objWK.MyCommand.Parameters.Add("JOB_CARD_FK_IN", _with2["JOB_CARD_FK_IN"]);
                    objWK.MyCommand.Parameters.Add("FRT_OTH_ELEMENT_IN", _with2["FRT_OTH_ELEMENT_IN"]);
                    objWK.MyCommand.Parameters.Add("FRT_OTH_ELEMENT_FK_IN", _with2["FRT_OTH_ELEMENT_FK_IN"]);

                    objWK.MyCommand.Parameters.Add("FRTS_TBL_FK_IN", _with2["FRT_TBL_FK_IN"]);

                    objWK.MyCommand.Parameters.Add("CURRENCY_MST_FK_IN", _with2["CURRENCY_MST_FK_IN"]);
                    objWK.MyCommand.Parameters.Add("ELEMENT_AMT_IN", _with2["ELEMENT_AMT_IN"]);
                    objWK.MyCommand.Parameters.Add("EXCHANGE_RATE_IN", _with2["EXCHANGE_RATE_IN"]);
                    objWK.MyCommand.Parameters.Add("AMT_IN_INV_CURR_IN", _with2["AMT_IN_INV_CURR_IN"]);
                    objWK.MyCommand.Parameters.Add("VAT_CODE_IN", _with2["VAT_CODE_IN"]);
                    objWK.MyCommand.Parameters.Add("TAX_PCNT_IN", _with2["TAX_PCNT_IN"]);
                    objWK.MyCommand.Parameters.Add("TAX_AMT_IN", _with2["TAX_AMT_IN"]);
                    objWK.MyCommand.Parameters.Add("TOT_AMT_IN", _with2["TOT_AMT_IN"]);
                    objWK.MyCommand.Parameters.Add("TOT_AMT_IN_LOC_CURR_IN", _with2["TOT_AMT_IN_LOC_CURR_IN"]);
                    objWK.MyCommand.Parameters.Add("REMARKS_IN", _with2["REMARKS_IN"]);
                    objWK.MyCommand.Parameters.Add("FRT_DESC_IN", _with2["FRT_DESC_IN"]);
                    intSaveSucceeded = objWK.MyCommand.ExecuteNonQuery();
                }
                if (intSaveSucceeded > 0)
                {
                    //TRAN.Commit()
                    //'adding by thiyagarajan on 23/1/09:TrackNTrace Task:VEK Req.
                    //SaveInTrackNTrace(InvRefNo, dsSave.Tables(1).Rows(0)["JOB_CARD_FK_IN"), "", 5, Customer, objWK)
                    //'end

                    SaveInTrackNTrace(Convert.ToString(InvRefNo), Convert.ToInt32(dsSave.Tables[1].Rows[0]["JOB_CARD_FK_IN"]), "", 5, Customer, objWK);

                    if (CrLimit > 0)
                    {
                        SaveCreditLimit(NetAmt, Customer, CrLimitUsed, TRAN);
                    }
                    TRAN.Commit();
                    //   If BizType = 1 And Process = 1 Then 'Air - Export
                    //    SaveTrackAndTraceForInv(TRAN, lngInvPk, 1, 1, "Invoice", "INV-AIR-EXP", _
                    //                                 nLocationPk, objWK, "INS", CREATED_BY_FK, "O")
                    //ElseIf BizType = 1 And Process = 2 Then 'Air - Import
                    //    SaveTrackAndTraceForInv(TRAN, lngInvPk, 1, 2, "Invoice", "INV-AIR-IMP", _
                    //                                 nLocationPk, objWK, "INS", CREATED_BY_FK, "O")
                    //ElseIf BizType = 2 And Process = 1 Then 'Sea - Export
                    //    SaveTrackAndTraceForInv(TRAN, lngInvPk, 2, 1, "Invoice", "INV-SEA-EXP", _
                    //                                 nLocationPk, objWK, "INS", CREATED_BY_FK, "O")
                    //ElseIf BizType = 2 And Process = 2 Then 'Sea - Import
                    //    SaveTrackAndTraceForInv(TRAN, lngInvPk, 2, 2, "Invoice", "INV-SEA-IMP", _
                    //                                  nLocationPk, objWK, "INS", CREATED_BY_FK, "O")
                    //End If
                    //End
                    //TRAN.Commit()
                }
                else
                {
                    TRAN.Rollback();
                    if (chkFlag)
                    {
                        RollbackProtocolKey("CONSOLIDATED INVOICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), InvRefNo.ToString(), System.DateTime.Now);
                    }
                }
                //Next intChldCnt
                //If CrLimit > 0 Then
                //    SaveCreditLimit(NetAmt, Customer, CrLimitUsed, TRAN)
                //End If
                return intSaveSucceeded;
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                if (chkFlag)
                {
                    RollbackProtocolKey("CONSOLIDATED INVOICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), InvRefNo.ToString(), System.DateTime.Now);
                }
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        //adding by thiyagarajan on 23/1/09:TrackNTrace Task:VEK Req.
        public void SaveInTrackNTrace(string refno, Int32 jobpk, string status, Int32 Doctype, string Customer, WorkFlow objWF)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            OracleTransaction TRAN = null;
            Int32 Return_value = default(Int32);
            try
            {
                status = chkjobforinvoice(jobpk);
                status += Customer;
                //objWF.OpenConnection()
                //TRAN = objWF.MyConnection.BeginTransaction()
                objWF.MyCommand.Connection = objWF.MyConnection;
                TRAN = objWF.MyCommand.Transaction;
                objWF.MyCommand.Transaction = TRAN;
                objWF.MyCommand.CommandType = CommandType.StoredProcedure;
                objWF.MyCommand.CommandText = objWF.MyUserName + ".REM_TRACK_N_TRACE_PKG.REM_TRACK_N_TRACE_INS";
                var _with3 = objWF.MyCommand.Parameters;
                _with3.Clear();
                _with3.Add("REF_NO_IN", refno).Direction = ParameterDirection.Input;
                _with3.Add("REF_FK_IN", jobpk).Direction = ParameterDirection.Input;
                _with3.Add("LOC_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with3.Add("STATUS_IN", status).Direction = ParameterDirection.Input;
                _with3.Add("CREATED_BY_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with3.Add("DOCTYPE_IN", Doctype).Direction = ParameterDirection.Input;
                Return_value = objWF.MyCommand.ExecuteNonQuery();
                if (Return_value > 0)
                {
                    //TRAN.Commit()
                }
                else
                {
                    TRAN.Rollback();
                }
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        private string chkjobforinvoice(Int32 jobpk)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            string Return_value = null;
            try
            {
                strQuery.Append("select  count(*) from rem_t_jc_frt_dtls_tbl frt where frt.job_card_fk=" + jobpk + "and frt.inv_rem_ref_pk is null");
                if (Convert.ToInt32(getDefault(objWF.ExecuteScaler(strQuery.ToString()), 0)) > 0)
                {
                    return "Part Invoice generated for the ";
                }
                else
                {
                    return "Invoice generated for the  ";
                }
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        //end by thiyagarajan 23/1/09

        #endregion "Save"

        #region "save TrackAndTrace"

        public ArrayList SaveTrackAndTraceForInv(OracleTransaction TRAN, int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, WorkFlow objWF, string flagInsUpd, long lngCreatedby,
        string PkStatus)
        {
            Int32 retVal = default(Int32);
            Int32 RecAfct = default(Int32);
            objWF.OpenConnection();

            OracleTransaction TRAN1 = null;
            TRAN1 = objWF.MyConnection.BeginTransaction();
            objWF.MyCommand.Transaction = TRAN1;
            try
            {
                arrMessage.Clear();
                var _with4 = objWF.MyCommand;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
                _with4.Transaction = TRAN1;
                _with4.Parameters.Clear();
                _with4.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("Container_Data_in", "").Direction = ParameterDirection.Input;
                _with4.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with4.ExecuteNonQuery();
                TRAN1.Commit();
                arrMessage.Add("All Data Saved Successfully");
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

        #endregion "save TrackAndTrace"

        #region " Protocol Reference Int32"

        public string GenerateInvoiceNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow ObjWK = null)
        {
            string functionReturnValue = null;
            try
            {
                functionReturnValue = GenerateProtocolKey("CONSOLIDATED INVOICE", nLocationId, nEmployeeId, DateTime.Now,"","" ,"" , nCreatedBy, ObjWK);
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }

        #endregion " Protocol Reference Int32"

        #region "Parent"

        public DataSet FetchListData(string strInvRefNo = "", string strJobRefNo = "", string strHblRefNo = "", string strCustomer = "", string strVessel = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", long usrLocFK = 0,
        short BizType = 1, short process = 1)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition = new System.Text.StringBuilder();
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            string a = null;
            string b = null;
            a = strCustomer;
            if (a == "0")
            {
                strCustomer = "";
            }
            else
            {
                strCustomer = strCustomer;
            }
            b = strVessel;
            if (b == "0")
            {
                strVessel = "";
            }
            else
            {
                strVessel = strVessel;
            }
            strCondition.Append(" SELECT  INV.CONSOL_INVOICE_PK PK,");
            strCondition.Append(" INV.INVOICE_REF_NO,");
            strCondition.Append(" CMT.CUSTOMER_NAME, ");
            strCondition.Append(" INV.INVOICE_DATE,");
            strCondition.Append(" (select SUM(INV.NET_RECEIVABLE) INV_AMT  from  dual) INVAMT ,");
            strCondition.Append(" CUMT.CURRENCY_ID");
            strCondition.Append(" FROM");
            strCondition.Append(" CONSOL_INVOICE_TBL INV, ");
            strCondition.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");

            if (BizType == 2 & process == 1)
            {
                strCondition.Append(" JOB_CARD_SEA_EXP_TBL   JOB,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_SEA_EXP_PK");
                strCondition.Append(" AND JOB.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK(+)");
                strCondition.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    strCondition.Append(" AND hbl.hbl_ref_no='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    strCondition.Append(" AND JOB.VOYAGE_TRN_FK=" + strVessel + "");
                }
            }
            //Air Export
            if (BizType == 1 & process == 1)
            {
                strCondition.Append(" JOB_CARD_AIR_EXP_TBL   JOB,");
                strCondition.Append(" HAWB_EXP_TBL            HAWB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_AIR_EXP_PK");
                strCondition.Append(" AND JOB.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                strCondition.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    strCondition.Append(" AND HAWB.HAWB_REF_NO='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    strCondition.Append(" AND HAWB.HAWB_REF_NO='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    strCondition.Append(" AND JOB.flight_no  LIKE '%" + strVessel.Trim() + "%' ");
                }
            }

            //Sea Import
            if (BizType == 2 & process == 2)
            {
                strCondition.Append(" JOB_CARD_SEA_IMP_TBL   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_SEA_IMP_PK");
                strCondition.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strVessel))
                {
                    strCondition.Append(" AND JOB.VOYAGE_TRN_FK=" + strVessel + "");
                }
            }
            //Air Import
            if (BizType == 1 & process == 2)
            {
                strCondition.Append(" JOB_CARD_AIR_IMP_TBL   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_AIR_IMP_PK");
                strCondition.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strVessel))
                {
                    strCondition.Append(" AND JOB.flight_no  LIKE '%" + strVessel.Trim() + "%' ");
                }
            }
            strCondition.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            strCondition.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            strCondition.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            strCondition.Append(" AND INV.consol_invoice_pk = INVTRN.consol_invoice_fk");
            strCondition.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
            strCondition.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");

            if (!string.IsNullOrEmpty(strJobRefNo))
            {
                strCondition.Append(" AND job.jobcard_ref_no='" + strJobRefNo + "'");
            }

            if (!string.IsNullOrEmpty(strCustomer))
            {
                strCondition.Append(" AND CMT.Customer_Mst_Pk=" + strCustomer + "");
            }

            if (!string.IsNullOrEmpty(strInvRefNo))
            {
                strCondition.Append(" AND inv.invoice_ref_no = '" + strInvRefNo + "' ");
            }

            strCondition.Append(" GROUP BY");
            strCondition.Append(" INV.CONSOL_INVOICE_PK,");
            strCondition.Append(" INV.INVOICE_REF_NO,");
            strCondition.Append(" INV.INVOICE_DATE ,");
            strCondition.Append(" CUMT.CURRENCY_ID,CMT.CUSTOMER_NAME,");
            strCondition.Append(" INV.NET_RECEIVABLE   ORDER BY " + SortColumn + "  " + SortType + "  ");

            // Get the Count of Records
            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + strCondition.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            //Get the Total Pages
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            //Get the last page and start page
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append("SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + strCondition.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);

            string sql = null;
            sql = sqlstr.ToString();
            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(sql);
                DataRelation CONTRel = null;
                DS.Tables.Add(fetchchild(AllMasterPKs(DS), strInvRefNo, strJobRefNo, strHblRefNo, strCustomer, strVessel, usrLocFK, BizType, process));
                CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["PK"], DS.Tables[1].Columns["PK"], true);
                CONTRel.Nested = true;
                DS.Relations.Add(CONTRel);
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

        #endregion "Parent"

        #region "Child Table"

        public DataTable fetchchild(string CONTSpotPKs = "", string strInvRefNo = "", string strJobRefNo = "", string strHblRefNo = "", string strCustomer = "", string strVessel = "", long usrLocFK = 0, short BizType = 1, short process = 1, int extype = 1)
        {
            string a = null;
            string b = null;

            a = strCustomer;
            if (a == "0")
            {
                strCustomer = "";
            }
            else
            {
                strCustomer = strCustomer;
            }
            b = strVessel;
            if (b == "0")
            {
                strVessel = "";
            }
            else
            {
                strVessel = strVessel;
            }

            System.Text.StringBuilder BuildQuery = new System.Text.StringBuilder();

            string strsql = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dt = null;
            int RowCnt = 0;
            int Rno = 0;
            int pk = 0;
            BuildQuery.Append("SELECT ROWNUM \"SL.NR\", T.*");
            BuildQuery.Append("FROM (");
            BuildQuery.Append(" SELECT INV.CONSOL_INVOICE_PK PK,");
            BuildQuery.Append(" JOB.JOBCARD_REF_NO, ");

            //Sea Export
            if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" HBL.HBL_REF_NO,");
                //Air Export
            }
            else if (BizType == 1 & process == 1)
            {
                BuildQuery.Append(" HAWB.HAWB_REF_NO,");
                //Sea Import
            }
            else if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" JOB.Hbl_REF_NO,");
                //Air Import
            }
            else if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" JOB.HAWB_REF_NO,");
            }
            //Sea
            if (BizType == 2)
            {
                BuildQuery.Append(" (CASE");
                BuildQuery.Append(" WHEN (NVL(JOB.VESSEL_NAME, '') || '/' ||");
                BuildQuery.Append(" NVL(JOB.VOYAGE, '') = '/') THEN");
                BuildQuery.Append(" ''");
                BuildQuery.Append(" ELSE");
                BuildQuery.Append(" NVL(JOB.VESSEL_NAME, '') || '/' || NVL(JOB.VOYAGE, '')");
                BuildQuery.Append(" END) AS VESVOYAGE,");
                //Air
            }
            else if (BizType == 1)
            {
                BuildQuery.Append(" JOB.FLIGHT_NO ,");
            }
            BuildQuery.Append(" INVTRN.TOT_AMT_IN_LOC_CURR,");
            BuildQuery.Append(" CUMT.CURRENCY_ID");
            BuildQuery.Append(" FROM");
            BuildQuery.Append(" CONSOL_INVOICE_TBL INV, ");
            BuildQuery.Append(" CONSOL_INVOICE_TRN_TBL INVTRN,");

            //Sea Export
            if (BizType == 2 & process == 1)
            {
                BuildQuery.Append(" JOB_CARD_SEA_EXP_TBL   JOB,");
                BuildQuery.Append(" HBL_EXP_TBL            HBL,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_SEA_EXP_PK");
                BuildQuery.Append(" AND JOB.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK(+)");
                BuildQuery.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    BuildQuery.Append(" AND hbl.hbl_ref_no='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    BuildQuery.Append(" AND JOB.VOYAGE_TRN_FK=" + strVessel + "");
                }
            }
            //Air Export
            if (BizType == 1 & process == 1)
            {
                BuildQuery.Append(" JOB_CARD_AIR_EXP_TBL   JOB,");
                BuildQuery.Append(" HAWB_EXP_TBL           HAWB,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_AIR_EXP_PK");
                BuildQuery.Append(" AND JOB.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK(+)");
                BuildQuery.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strHblRefNo))
                {
                    BuildQuery.Append(" AND HAWB.HAWB_REF_NO='" + strHblRefNo + "'");
                }
                if (!string.IsNullOrEmpty(strVessel))
                {
                    BuildQuery.Append(" AND JOB.flight_no  LIKE '%" + strVessel.Trim() + "%' ");
                }
            }
            //Sea Import
            if (BizType == 2 & process == 2)
            {
                BuildQuery.Append(" JOB_CARD_SEA_IMP_TBL   JOB,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_SEA_IMP_PK");
                BuildQuery.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strVessel))
                {
                    BuildQuery.Append(" AND JOB.VOYAGE_TRN_FK=" + strVessel + "");
                }
            }
            //Air Import
            if (BizType == 1 & process == 2)
            {
                BuildQuery.Append(" JOB_CARD_AIR_IMP_TBL   JOB,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_AIR_IMP_PK");
                BuildQuery.Append(" AND JOB.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                if (!string.IsNullOrEmpty(strVessel))
                {
                    BuildQuery.Append(" AND JOB.flight_no  LIKE '%" + strVessel.Trim() + "%' ");
                }
            }

            BuildQuery.Append(" AND INV.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            BuildQuery.Append(" AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ");
            BuildQuery.Append(" AND INV.CREATED_BY_FK = UMT.USER_MST_PK");
            BuildQuery.Append(" AND INV.consol_invoice_pk = INVTRN.consol_invoice_fk");

            BuildQuery.Append(" AND INV.PROCESS_TYPE ='" + process + "' ");
            BuildQuery.Append(" AND INV.BUSINESS_TYPE ='" + BizType + "' ");

            if (!string.IsNullOrEmpty(strJobRefNo))
            {
                BuildQuery.Append(" AND job.jobcard_ref_no='" + strJobRefNo + "'");
            }

            if (!string.IsNullOrEmpty(strCustomer))
            {
                BuildQuery.Append(" AND CMT.Customer_Mst_Pk=" + strCustomer + "");
            }

            if (!string.IsNullOrEmpty(strInvRefNo))
            {
                BuildQuery.Append(" AND inv.invoice_ref_no = '" + strInvRefNo + "' ");
            }

            if (CONTSpotPKs.Trim().Length > 0 | CONTSpotPKs != "-1")
            {
                BuildQuery.Append(" AND INV.CONSOL_INVOICE_PK in (" + CONTSpotPKs + ") ");
            }
            BuildQuery.Append(" )  T ");

            strsql = BuildQuery.ToString();
            try
            {
                pk = -1;
                dt = objWF.GetDataTable(strsql);
                for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++)
                {
                    if (Convert.ToInt32(dt.Rows[RowCnt]["PK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["PK"]);
                        Rno = 0;
                    }
                    Rno += 1;
                    dt.Rows[RowCnt]["SL.NR"] = Rno;
                }
                return dt;
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

        #endregion "Child Table"

        #region "GET PK"

        public Int32 GetInvPk(string RefNo)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            try
            {
                Strsql = "select con.REMOVALS_INVOICE_PK from REM_M_INVOICE_TBL con where con.INVOICE_REF_NO like '" + RefNo + "'";
                return Convert.ToInt32(Objwk.ExecuteScaler(Strsql));
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "GET PK"

        #region "PK Value"

        private string AllMasterPKs(DataSet ds)
        {
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            System.Text.StringBuilder strBuild = new System.Text.StringBuilder();
            strBuild.Append("-1,");
            for (RowCnt = 0; RowCnt <= Convert.ToInt16(ds.Tables[0].Rows.Count - 1); RowCnt++)
            {
                strBuild.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["PK"]).Trim() + ",");
            }
            strBuild.Remove(strBuild.Length - 1, 1);
            return strBuild.ToString();
        }

        #endregion "PK Value"

        #region "Enhance Search"

        public string FetchInvoice(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            string[] arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strProcessType = null;
            string strloc = "";

            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strProcessType = arr[3];
            strloc = arr[4];

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".en_consol_invoice_pkg.get_invoice";

                var _with5 = selectCommand.Parameters;
                _with5.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with5.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with5.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input;
                _with5.Add("LOCFK_IN", strloc).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "Enhance Search"

        public void SaveCreditLimit(double NetAmt, string Customer, double CrLimitUsed, OracleTransaction TRAN)
        {
            WorkFlow objWK = new WorkFlow();
            //objWK.OpenConnection()

            //Dim TRAN1 As OracleClient.OracleTransaction
            OracleCommand cmd = new OracleCommand();
            //TRAN1 = objWK.MyConnection.BeginTransaction()
            //cmd.Transaction = TRAN1
            cmd.Transaction = TRAN;
            string strSQL = null;
            double temp = 0;

            temp = CrLimitUsed - NetAmt;
            try
            {
                cmd.CommandType = CommandType.Text;
                //cmd.Connection = TRAN1.Connection
                //cmd.Transaction = TRAN1
                cmd.Connection = TRAN.Connection;
                cmd.Transaction = TRAN;
                cmd.Parameters.Clear();
                strSQL = "update customer_mst_tbl a set a.credit_limit_used = " + temp;
                strSQL = strSQL + " where a.customer_name in ('" + Customer + "')";
                cmd.CommandText = strSQL;
                cmd.ExecuteNonQuery();
                //TRAN1.Commit()
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #region "Fetch Job Card Sea/Air Details For Report"

        public DataSet FetchJobCardSeaDetails(string jobcardpk, int process)
        {
            // created by thiyagarajan
            StringBuilder Strsql = new StringBuilder();
            WorkFlow Objwk = new WorkFlow();
            if (process == 2)
            {
                Strsql.Append(" SELECT JSI.JOB_CARD_SEA_IMP_PK AS JOBCARDPK,");
                Strsql.Append(" JSI.JOBCARD_REF_NO JOBCARDNO,");
                Strsql.Append(" JSI.UCR_NO AS UCRNO,");
                Strsql.Append(" CONSIGCUST.CUSTOMER_NAME CONSIGNAME,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_1 CONSIGADD1,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_2 CONSIGADD2,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_3 CONSIGADD3,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ZIP_CODE CONSIGZIP,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_PHONE_NO_1 CONSIGPHONE,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_CITY CONSIGCITY,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_FAX_NO CONFAX,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_EMAIL_ID CONEMAIL,");
                Strsql.Append(" CONSCOUNTRY.COUNTRY_NAME CONSCOUNTRY,");
                Strsql.Append(" SHIPPERCUST.CUSTOMER_NAME SHIPPERNAME,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_1 SHIPPERADD1,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_2 SHIPPERADD2,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_3 SHIPPERADD3,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ZIP_CODE SHIPPERZIP,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_CITY SHIPPERCITY,");
                Strsql.Append("  SHIPPERCUSTDTLS.ADM_FAX_NO SHIPPERFAX,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_EMAIL_ID SHIPPEREMAIL,");
                Strsql.Append(" SHIPCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,");
                Strsql.Append(" AGENTMST.AGENT_NAME AGENTNAME,");
                Strsql.Append(" AGENTDTLS.ADM_ADDRESS_1 AGENTADD1,");
                Strsql.Append(" AGENTDTLS.ADM_ADDRESS_2 AGENTADD2,");
                Strsql.Append(" AGENTDTLS.ADM_ADDRESS_3 AGENTADD3,");
                Strsql.Append(" AGENTDTLS.ADM_CITY      AGENTCITY,");
                Strsql.Append(" AGENTDTLS.ADM_ZIP_CODE  AGENTZIP,");
                Strsql.Append(" AGENTDTLS.ADM_PHONE_NO_1 AGENTPHONE,");
                Strsql.Append(" AGENTDTLS.ADM_FAX_NO    AGENTFAX,");
                Strsql.Append(" AGENTDTLS.ADM_EMAIL_ID  AGENTEMAIL,");
                Strsql.Append(" AGENTCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,");
                Strsql.Append(" (CASE WHEN JSI.VOYAGE IS NOT NULL THEN");
                Strsql.Append(" JSI.VESSEL_NAME || '/' || JSI.VOYAGE");
                Strsql.Append(" ELSE");
                Strsql.Append(" JSI.VESSEL_NAME END ) VES_VOY,");
                Strsql.Append(" CTMST.COMMODITY_GROUP_CODE COMMTYPE,");
                Strsql.Append(" (CASE WHEN JSI.HBL_REF_NO IS NOT NULL THEN");
                Strsql.Append(" JSI.HBL_REF_NO");
                Strsql.Append("  ELSE");
                Strsql.Append(" JSI.MBL_REF_NO END ) BLREFNO,");

                Strsql.Append("POL.PORT_ID || ','|| (select cont.country_name from country_mst_tbl cont where cont.country_mst_pk=POL.country_mst_fk) POLNAME,");
                Strsql.Append("POD.PORT_ID || ','|| (select cont1.country_name from country_mst_tbl cont1 where cont1.country_mst_pk=POD.country_mst_fk) PODNAME,");

                Strsql.Append(" DELMST.PLACE_NAME DEL_PLACE_NAME,");
                Strsql.Append(" JSI.GOODS_DESCRIPTION,");
                Strsql.Append(" JSI.ETA_DATE ETA,");
                Strsql.Append(" JSI.ETD_DATE ETD,");
                Strsql.Append(" JSI.CLEARANCE_ADDRESS CLEARANCEPOINT,");
                Strsql.Append(" JSI.MARKS_NUMBERS MARKS,");
                Strsql.Append(" STMST.INCO_CODE TERMS,");
                Strsql.Append(" NVL(JSI.INSURANCE_AMT, 0) INSURANCE,");
                Strsql.Append(" JSI.PYMT_TYPE PYMT_TYPE,");
                Strsql.Append(" JSI.CARGO_TYPE CARGO_TYPE,");
                Strsql.Append(" SUM(JTSC.GROSS_WEIGHT) GROSSWEIGHT,");
                Strsql.Append("  SUM(JTSC.NET_WEIGHT) NETWEIGHT,");
                Strsql.Append("  SUM(JTSC.CHARGEABLE_WEIGHT) CHARWT,");
                Strsql.Append(" SUM(JTSC.VOLUME_IN_CBM) VOLUME");
                //Strsql.Append(" MAX(JTSIT.ETA_DATE) ETA")
                Strsql.Append(" from JOB_CARD_SEA_IMP_TBL JSI,");
                Strsql.Append(" CUSTOMER_MST_TBL CONSIGCUST,");
                Strsql.Append(" CUSTOMER_CONTACT_DTLS CONSIGCUSTDTLS,");
                Strsql.Append(" CUSTOMER_CONTACT_DTLS SHIPPERCUSTDTLS,");
                Strsql.Append(" CUSTOMER_MST_TBL SHIPPERCUST,");
                Strsql.Append(" AGENT_MST_TBL AGENTMST,");
                Strsql.Append(" AGENT_CONTACT_DTLS AGENTDTLS,");
                Strsql.Append(" PORT_MST_TBL POL,");
                Strsql.Append(" PORT_MST_TBL POD,");
                Strsql.Append(" JOB_TRN_SEA_IMP_CONT JTSC,");
                Strsql.Append(" SHIPPING_TERMS_MST_TBL STMST,");
                Strsql.Append(" COUNTRY_MST_TBL SHIPCOUNTRY,");
                Strsql.Append(" COUNTRY_MST_TBL CONSCOUNTRY,");
                Strsql.Append(" COUNTRY_MST_TBL AGENTCOUNTRY,");
                Strsql.Append(" PLACE_MST_TBL DELMST,");
                Strsql.Append(" JOB_TRN_SEA_IMP_TP JTSIT,");
                Strsql.Append(" COMMODITY_GROUP_MST_TBL CTMST");
                Strsql.Append(" WHERE CONSIGCUST.CUSTOMER_MST_PK(+) = JSI.CONSIGNEE_CUST_MST_FK");
                Strsql.Append(" AND   SHIPPERCUST.CUSTOMER_MST_PK(+)=JSI.SHIPPER_CUST_MST_FK");
                Strsql.Append(" AND   CONSIGCUSTDTLS.CUSTOMER_MST_FK(+)=CONSIGCUST.CUSTOMER_MST_PK");
                Strsql.Append(" AND   SHIPPERCUSTDTLS.CUSTOMER_MST_FK(+)=SHIPPERCUST.CUSTOMER_MST_PK");
                Strsql.Append(" AND   POL.PORT_MST_PK(+)=JSI.PORT_MST_POL_FK");
                Strsql.Append(" AND   POD.PORT_MST_PK(+)=JSI.PORT_MST_POD_FK");
                Strsql.Append(" AND   JTSC.JOB_CARD_SEA_IMP_FK(+)=JSI.JOB_CARD_SEA_IMP_PK");
                Strsql.Append(" AND   STMST.SHIPPING_TERMS_MST_PK(+)=JSI.SHIPPING_TERMS_MST_FK");
                Strsql.Append(" AND   CONSCOUNTRY.COUNTRY_MST_PK(+)=CONSIGCUSTDTLS.ADM_COUNTRY_MST_FK");
                Strsql.Append(" AND   SHIPCOUNTRY.COUNTRY_MST_PK(+)=SHIPPERCUSTDTLS.ADM_COUNTRY_MST_FK ");
                Strsql.Append(" AND   AGENTMST.AGENT_MST_PK(+)=JSI.POL_AGENT_MST_FK");
                Strsql.Append(" AND   AGENTDTLS.AGENT_MST_FK(+)=AGENTMST.AGENT_MST_PK");
                Strsql.Append(" AND   AGENTCOUNTRY.COUNTRY_MST_PK(+)=AGENTDTLS.ADM_COUNTRY_MST_FK");
                Strsql.Append(" AND   DELMST.PLACE_PK(+)=JSI.DEL_PLACE_MST_FK");
                Strsql.Append(" AND CTMST.COMMODITY_GROUP_PK(+)=JSI.COMMODITY_GROUP_FK");
                Strsql.Append(" AND JTSIT.JOB_CARD_SEA_IMP_FK(+)=JSI.JOB_CARD_SEA_IMP_PK");
                Strsql.Append(" AND  nvl(JTSIT.ETA_DATE,sysdate)=(SELECT  nvl(MAX(JTT.ETA_DATE),sysdate) FROM JOB_TRN_SEA_IMP_TP JTT WHERE JTT.JOB_CARD_SEA_IMP_FK=JTSIT.JOB_CARD_SEA_IMP_FK)");
                Strsql.Append(" AND JSI.JOB_CARD_SEA_IMP_PK IN (" + jobcardpk + ")");
                Strsql.Append(" GROUP BY JSI.JOB_CARD_SEA_IMP_PK,");
                Strsql.Append(" JSI.JOBCARD_REF_NO ,");
                Strsql.Append(" JSI.UCR_NO  ,");
                Strsql.Append(" CONSIGCUST.CUSTOMER_NAME ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_1 ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_2 ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ADDRESS_3 ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_ZIP_CODE ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_PHONE_NO_1 ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_CITY ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_FAX_NO ,");
                Strsql.Append(" CONSIGCUSTDTLS.ADM_EMAIL_ID ,");
                Strsql.Append(" CONSCOUNTRY.COUNTRY_NAME ,");
                Strsql.Append(" SHIPPERCUST.CUSTOMER_NAME ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_1 ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ADDRESS_2 ,");
                Strsql.Append("  SHIPPERCUSTDTLS.ADM_ADDRESS_3 ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_ZIP_CODE ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_PHONE_NO_1 ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_CITY ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_FAX_NO ,");
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_EMAIL_ID ,");
                Strsql.Append(" SHIPCOUNTRY.COUNTRY_NAME,");
                Strsql.Append(" AGENTMST.AGENT_NAME ,");
                Strsql.Append(" AGENTDTLS.ADM_ADDRESS_1,");
                Strsql.Append(" AGENTDTLS.ADM_ADDRESS_2 ,");
                Strsql.Append(" AGENTDTLS.ADM_ADDRESS_3,");
                Strsql.Append(" AGENTDTLS.ADM_CITY,");
                Strsql.Append(" AGENTDTLS.ADM_ZIP_CODE ,");
                Strsql.Append("  AGENTDTLS.ADM_PHONE_NO_1,");
                Strsql.Append("  AGENTDTLS.ADM_FAX_NO ,");
                Strsql.Append("  AGENTDTLS.ADM_EMAIL_ID,");
                Strsql.Append(" AGENTCOUNTRY.COUNTRY_NAME,");
                Strsql.Append(" (CASE WHEN JSI.VOYAGE IS NOT NULL THEN");
                Strsql.Append(" JSI.VESSEL_NAME || '/' || JSI.VOYAGE");
                Strsql.Append(" ELSE");
                Strsql.Append(" JSI.VESSEL_NAME END ) ,");
                Strsql.Append(" CTMST.COMMODITY_GROUP_CODE,");
                Strsql.Append(" (CASE WHEN JSI.HBL_REF_NO IS NOT NULL THEN");
                Strsql.Append("  JSI.HBL_REF_NO");
                Strsql.Append("  ELSE");
                Strsql.Append("  JSI.MBL_REF_NO END ) ,");

                Strsql.Append(" POL.PORT_ID,POL.COUNTRY_MST_FK,");
                Strsql.Append(" POD.PORT_ID,POD.COUNTRY_MST_FK,");

                Strsql.Append(" DELMST.PLACE_NAME ,");
                Strsql.Append(" JSI.GOODS_DESCRIPTION,");
                Strsql.Append(" JSI.ETA_DATE ,");
                Strsql.Append(" JSI.ETD_DATE ,");
                Strsql.Append(" JSI.CLEARANCE_ADDRESS,");
                Strsql.Append(" JSI.MARKS_NUMBERS ,");
                Strsql.Append(" STMST.INCO_CODE,");
                Strsql.Append("  NVL(JSI.INSURANCE_AMT,0),");
                Strsql.Append(" JSI.CARGO_TYPE,");
                Strsql.Append(" JSI.PYMT_TYPE");
            }
            else if (process == 1)
            {
                Strsql.Append("select (select cus.customer_name from customer_mst_tbl cus where cus.customer_mst_pk=consg.customer_mst_fk ) CONSIGNAME,");
                Strsql.Append("consg.adm_address_1 CONSIGADD1,");
                Strsql.Append("consg.adm_address_2 CONSIGADD2,");
                Strsql.Append(" consg.adm_address_3 CONSIGADD3,");
                Strsql.Append("consg.adm_zip_code CONSIGZIP,");
                Strsql.Append(" consg.adm_city CONSIGCITY,");
                Strsql.Append(" ( select ctry.country_name from country_mst_tbl ctry where ctry.country_mst_pk=consg.adm_country_mst_fk) CONSCOUNTRY,");

                //modifying by thiyagarajan on 3/1/09 to display Port Id &  name in report
                Strsql.Append(" ((select pol.port_id  from port_mst_tbl pol where pol.port_mst_pk=booking.port_mst_pol_fk) || ','|| (select cont.country_name from country_mst_tbl cont where cont.country_mst_pk=(select pol.country_mst_fk from port_mst_tbl pol where pol.port_mst_pk=booking.port_mst_pol_fk))) POLNAME,");
                Strsql.Append(" ((select pod.port_id  from port_mst_tbl pod where pod.port_mst_pk=booking.port_mst_pod_fk) || ','|| (select cont.country_name from country_mst_tbl cont where cont.country_mst_pk=(select pod.country_mst_fk from port_mst_tbl poD where pod.port_mst_pk=booking.port_mst_pod_fk))) PODNAME,");
                //end

                Strsql.Append("(  CASE WHEN job.VOYAGE IS NOT NULL THEN");
                Strsql.Append(" Job.VESSEL_NAME || '/' || Job.VOYAGE");
                Strsql.Append(" Else ");
                Strsql.Append(" Job.VESSEL_NAME END ) VES_VOY,");

                //modifying by thiyagarajan on 3/1/09 to display Place name in report
                //Strsql.Append("'' as DEL_PLACE_NAME,")
                Strsql.Append("(select pl.place_name from place_mst_tbl pl where pl.place_pk=booking.DEL_PLACE_MST_FK) DEL_PLACE_NAME,");
                //end

                Strsql.Append("(CASE WHEN job.hbl_exp_tbl_fk IS NOT NULL THEN");
                Strsql.Append("(select hbl.hbl_ref_no from hbl_exp_tbl hbl where hbl.hbl_exp_tbl_pk=job.hbl_exp_tbl_fk)");
                Strsql.Append("Else");
                Strsql.Append("(select mbl.mbl_ref_no from mbl_exp_tbl mbl where mbl.mbl_exp_tbl_pk=job.mbl_exp_tbl_fk)");
                Strsql.Append(" END) BLREFNO,");
                Strsql.Append(" job.goods_description, job.marks_numbers MARKS,");
                Strsql.Append(" (select ship.inco_code from shipping_terms_mst_tbl ship where ship.shipping_terms_mst_pk=job.shipping_terms_mst_fk)");
                Strsql.Append(" TERMS,job.pymt_type,NVL(job.INSURANCE_AMT, 0) INSURANCE,");

                Strsql.Append("(select sum(jstc.gross_weight) from job_trn_sea_exp_cont jstc where jstc.job_card_sea_exp_fk=job.job_card_sea_exp_pk) GROSSWEIGHT,");

                Strsql.Append(" (select sum(jstc.volume_in_cbm) from job_trn_sea_exp_cont jstc where jstc.job_card_sea_exp_fk=job.job_card_sea_exp_pk) VOLUME,");
                Strsql.Append(" (select sum(jstc.chargeable_weight) from job_trn_sea_exp_cont jstc where jstc.job_card_sea_exp_fk=job.job_card_sea_exp_pk) CHARWT ");

                Strsql.Append(" from job_card_sea_exp_tbl job ,booking_sea_tbl booking, customer_contact_dtls consg");
                Strsql.Append(" where job.job_card_sea_exp_pk in (" + jobcardpk + ")");
                Strsql.Append(" and job.booking_sea_fk=booking.booking_sea_pk");
                Strsql.Append(" and job.consignee_cust_mst_fk=consg.customer_mst_fk");
            }
            try
            {
                return Objwk.GetDataSet(Strsql.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        #endregion "Fetch Job Card Sea/Air Details For Report"

        #region "FetchJobCardAirDetails"

        //created by thiyagarjan
        public DataSet FetchJobCardAirDetails(string JobCardPK, int process)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            if (process == 2)
            {
                Strsql = "select JAI.JOB_CARD_AIR_IMP_PK AS JOBCARDPK,";
                Strsql += "JAI.JOBCARD_REF_NO JOBCARDNO,";
                Strsql += "JAI.UCR_NO AS UCRNO,";
                Strsql += "CONSIGCUST.CUSTOMER_NAME CONSIGNAME,";
                Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_1 CONSIGADD1,";
                Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_2 CONSIGADD2,";
                Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_3 CONSIGADD3,";
                Strsql += "CONSIGCUSTDTLS.ADM_ZIP_CODE CONSIGZIP,";
                Strsql += "CONSIGCUSTDTLS.ADM_PHONE_NO_1 CONSIGPHONE,";
                Strsql += "CONSIGCUSTDTLS.ADM_CITY CONSIGCITY,";
                Strsql += "CONSIGCUSTDTLS.ADM_FAX_NO CONFAX,";
                Strsql += "CONSIGCUSTDTLS.ADM_EMAIL_ID CONEMAIL,";
                Strsql += "CONSCOUNTRY.COUNTRY_NAME CONSCOUNTRY,";
                Strsql += "SHIPPERCUST.CUSTOMER_NAME SHIPPERNAME,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_1 SHIPPERADD1,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_2 SHIPPERADD2,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_3 SHIPPERADD3,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ZIP_CODE SHIPPERZIP,";
                Strsql += "SHIPPERCUSTDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,";
                Strsql += "SHIPPERCUSTDTLS.ADM_CITY SHIPPERCITY,";
                Strsql += "SHIPPERCUSTDTLS.ADM_FAX_NO SHIPPERFAX,";
                Strsql += "SHIPPERCUSTDTLS.ADM_EMAIL_ID SHIPPEREMAIL,";
                Strsql += "SHIPCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
                Strsql += "AGENTMST.AGENT_NAME AGENTNAME,";
                Strsql += "AGENTDTLS.ADM_ADDRESS_1 AGENTADD1,";
                Strsql += "AGENTDTLS.ADM_ADDRESS_2 AGENTADD2,";
                Strsql += "AGENTDTLS.ADM_ADDRESS_3 AGENTADD3,";
                Strsql += "AGENTDTLS.ADM_CITY      AGENTCITY,";
                Strsql += "AGENTDTLS.ADM_ZIP_CODE  AGENTZIP,";
                Strsql += "AGENTDTLS.ADM_PHONE_NO_1 AGENTPHONE,";
                Strsql += "AGENTDTLS.ADM_FAX_NO    AGENTFAX,";
                Strsql += "AGENTDTLS.ADM_EMAIL_ID  AGENTEMAIL,";
                Strsql += "AGENTCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,";
                Strsql += "JAI.FLIGHT_NO  VES_VOY,";
                Strsql += "CGMST.COMMODITY_GROUP_DESC COMMTYPE,";
                Strsql += "(CASE WHEN JAI.HAWB_REF_NO IS NOT NULL THEN";
                Strsql += "JAI.HAWB_REF_NO";
                Strsql += " ELSE";
                Strsql += "JAI.MAWB_REF_NO END ) BLREFNO,";

                //modified by thiyagarjan

                //Strsql &= vbCrLf & "POL.PORT_NAME POLNAME,"
                //Strsql &= vbCrLf & "POD.PORT_NAME PODNAME,"

                Strsql += "(POL.PORT_ID || ','|| (select cont.country_name from country_mst_tbl cont where cont.country_mst_pk=POL.country_mst_fk)) POLNAME,";
                Strsql += "(POD.PORT_ID || ','|| (select cont1.country_name from country_mst_tbl cont1 where cont1.country_mst_pk=POD.country_mst_fk)) PODNAME,";

                Strsql += "DELMST.PLACE_NAME DEL_PLACE_NAME,";
                Strsql += "JAI.GOODS_DESCRIPTION,";
                Strsql += "JAI.ETD_DATE ETD,";
                Strsql += "JAI.ETA_DATE ETA,";
                Strsql += "JAI.CLEARANCE_ADDRESS CLEARANCEPOINT,";
                Strsql += "JAI.MARKS_NUMBERS MARKS,";
                Strsql += "STMST.INCO_CODE TERMS,";
                Strsql += "NVL(JAI.INSURANCE_AMT, 0) INSURANCE,";
                Strsql += "JAI.PYMT_TYPE PYMT_TYPE,";
                Strsql += "2 CARGO_TYPE,";
                Strsql += "SUM(JTSC.GROSS_WEIGHT) GROSSWEIGHT,";
                Strsql += " '' NETWEIGHT,";
                Strsql += " SUM(JTSC.CHARGEABLE_WEIGHT) CHARWT,";
                Strsql += "SUM(JTSC.VOLUME_IN_CBM) VOLUME";
                // Strsql &= vbCrLf & "MAX(JTAIT.ETA_DATE) ETA"
                Strsql += "from JOB_CARD_AIR_IMP_TBL JAI,";
                Strsql += "JOB_TRN_AIR_IMP_TP JTAIT,";
                Strsql += "CUSTOMER_MST_TBL CONSIGCUST,";
                Strsql += "CUSTOMER_CONTACT_DTLS CONSIGCUSTDTLS,";
                Strsql += "CUSTOMER_CONTACT_DTLS SHIPPERCUSTDTLS,";
                Strsql += "CUSTOMER_MST_TBL SHIPPERCUST,";
                Strsql += "AGENT_MST_TBL AGENTMST,";
                Strsql += "AGENT_CONTACT_DTLS AGENTDTLS,";
                Strsql += "PORT_MST_TBL POL,";
                Strsql += "PORT_MST_TBL POD,";
                Strsql += "JOB_TRN_AIR_IMP_CONT JTSC,";
                Strsql += "SHIPPING_TERMS_MST_TBL STMST,";
                Strsql += "COUNTRY_MST_TBL SHIPCOUNTRY,";
                Strsql += "COUNTRY_MST_TBL CONSCOUNTRY,";
                Strsql += "COUNTRY_MST_TBL AGENTCOUNTRY,";
                Strsql += "PLACE_MST_TBL DELMST,";
                Strsql += "COMMODITY_GROUP_MST_TBL CGMST";
                Strsql += "WHERE CONSIGCUST.CUSTOMER_MST_PK(+) = JAI.CONSIGNEE_CUST_MST_FK";
                Strsql += " AND   JTAIT.JOB_CARD_AIR_IMP_FK(+)= JAI.JOB_CARD_AIR_IMP_PK";
                Strsql += "AND  nvl(JTAIT.ETA_DATE,sysdate)=(SELECT  nvl(MAX(JTT.ETA_DATE),sysdate) FROM JOB_TRN_AIR_IMP_TP JTT WHERE JTT.JOB_CARD_AIR_IMP_FK=JTAIT.JOB_CARD_AIR_IMP_FK)";
                Strsql += "AND   SHIPPERCUST.CUSTOMER_MST_PK(+)=JAI.SHIPPER_CUST_MST_FK";
                Strsql += "AND   CONSIGCUSTDTLS.CUSTOMER_MST_FK(+)=CONSIGCUST.CUSTOMER_MST_PK";
                Strsql += "AND   SHIPPERCUSTDTLS.CUSTOMER_MST_FK(+)=SHIPPERCUST.CUSTOMER_MST_PK";
                Strsql += "AND   POL.PORT_MST_PK(+)=JAI.PORT_MST_POL_FK";
                Strsql += "AND   POD.PORT_MST_PK(+)=JAI.PORT_MST_POD_FK";
                Strsql += "AND   JTSC.JOB_CARD_AIR_IMP_FK(+)=JAI.JOB_CARD_AIR_IMP_PK";
                Strsql += "AND   STMST.SHIPPING_TERMS_MST_PK(+)=JAI.SHIPPING_TERMS_MST_FK";
                Strsql += "AND   CONSCOUNTRY.COUNTRY_MST_PK(+)=CONSIGCUSTDTLS.ADM_COUNTRY_MST_FK";
                Strsql += "AND   SHIPCOUNTRY.COUNTRY_MST_PK(+)=SHIPPERCUSTDTLS.ADM_COUNTRY_MST_FK ";
                Strsql += "AND   AGENTMST.AGENT_MST_PK(+)=JAI.POL_AGENT_MST_FK";
                Strsql += "AND   AGENTDTLS.AGENT_MST_FK(+)=AGENTMST.AGENT_MST_PK";
                Strsql += "AND   AGENTCOUNTRY.COUNTRY_MST_PK(+)=AGENTDTLS.ADM_COUNTRY_MST_FK";
                Strsql += "AND   DELMST.PLACE_PK(+)=JAI.DEL_PLACE_MST_FK";
                Strsql += "AND   CGMST.COMMODITY_GROUP_PK(+)=JAI.COMMODITY_GROUP_FK";
                Strsql += "AND   JAI.JOB_CARD_AIR_IMP_PK IN (" + JobCardPK + ")";
                Strsql += "GROUP BY JAI.JOB_CARD_AIR_IMP_PK,";
                Strsql += "JAI.JOBCARD_REF_NO ,";
                Strsql += "JAI.UCR_NO  ,";
                Strsql += "CONSIGCUST.CUSTOMER_NAME ,";
                Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_1 ,";
                Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_2 ,";
                Strsql += "CONSIGCUSTDTLS.ADM_ADDRESS_3 ,";
                Strsql += " CONSIGCUSTDTLS.ADM_ZIP_CODE ,";
                Strsql += "CONSIGCUSTDTLS.ADM_PHONE_NO_1 ,";
                Strsql += "CONSIGCUSTDTLS.ADM_CITY ,";
                Strsql += "CONSIGCUSTDTLS.ADM_FAX_NO ,";
                Strsql += "CONSIGCUSTDTLS.ADM_EMAIL_ID ,";
                Strsql += "CONSCOUNTRY.COUNTRY_NAME ,";
                Strsql += "SHIPPERCUST.CUSTOMER_NAME ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_1 ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ADDRESS_2 ,";
                Strsql += " SHIPPERCUSTDTLS.ADM_ADDRESS_3 ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_ZIP_CODE ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_PHONE_NO_1 ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_CITY ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_FAX_NO ,";
                Strsql += "SHIPPERCUSTDTLS.ADM_EMAIL_ID ,";
                Strsql += "SHIPCOUNTRY.COUNTRY_NAME,";
                Strsql += "AGENTMST.AGENT_NAME ,";
                Strsql += "AGENTDTLS.ADM_ADDRESS_1,";
                Strsql += "AGENTDTLS.ADM_ADDRESS_2 ,";
                Strsql += "AGENTDTLS.ADM_ADDRESS_3,";
                Strsql += "AGENTDTLS.ADM_CITY,";
                Strsql += "AGENTDTLS.ADM_ZIP_CODE ,";
                Strsql += " AGENTDTLS.ADM_PHONE_NO_1,";
                Strsql += " AGENTDTLS.ADM_FAX_NO ,";
                Strsql += " AGENTDTLS.ADM_EMAIL_ID,";
                Strsql += "AGENTCOUNTRY.COUNTRY_NAME,";
                Strsql += "JAI.FLIGHT_NO ,";
                Strsql += "CGMST.COMMODITY_GROUP_DESC,";
                Strsql += "(CASE WHEN JAI.HAWB_REF_NO IS NOT NULL THEN";
                Strsql += " JAI.HAWB_REF_NO";
                Strsql += " ELSE";
                Strsql += " JAI.MAWB_REF_NO END ) ,";

                Strsql += " POL.PORT_ID,POL.COUNTRY_MST_FK,";
                Strsql += " POD.PORT_ID,POD.COUNTRY_MST_FK,";

                Strsql += "DELMST.PLACE_NAME ,";
                Strsql += "JAI.GOODS_DESCRIPTION,";
                Strsql += "JAI.ETD_DATE ,";
                Strsql += "JAI.ETA_DATE ,";
                Strsql += "JAI.CLEARANCE_ADDRESS,";
                Strsql += "JAI.MARKS_NUMBERS ,";
                Strsql += "STMST.INCO_CODE,";
                Strsql += " NVL(JAI.INSURANCE_AMT,0),";
                Strsql += "JAI.PYMT_TYPE";
            }
            else if (process == 1)
            {
                Strsql += " select (select cus.customer_name from customer_mst_tbl cus where cus.customer_mst_pk=consg.customer_mst_fk ) CONSIGNAME,";
                Strsql += " consg.adm_address_1 CONSIGADD1,";
                Strsql += " consg.adm_address_2 CONSIGADD2,";
                Strsql += " consg.adm_address_3 CONSIGADD3,";
                Strsql += " consg.adm_zip_code CONSIGZIP,";
                Strsql += " consg.adm_city CONSIGCITY,";

                Strsql += "( select ctry.country_name from country_mst_tbl ctry where ctry.country_mst_pk=consg.adm_country_mst_fk) CONSCOUNTRY,";

                //modifying by thiyagarajan on 3/1/09 to display Port Id &  name in report
                Strsql += " ((select pol.port_id  from port_mst_tbl pol where pol.port_mst_pk=booking.port_mst_pol_fk) || ','|| (select cont.country_name from country_mst_tbl cont where cont.country_mst_pk=(select pol.country_mst_fk from port_mst_tbl pol where pol.port_mst_pk=booking.port_mst_pol_fk))) POLNAME,";
                Strsql += " ((select pod.port_id  from port_mst_tbl pod where pod.port_mst_pk=booking.port_mst_pod_fk) || ','|| (select cont.country_name from country_mst_tbl cont where cont.country_mst_pk=(select pod.country_mst_fk from port_mst_tbl poD where pod.port_mst_pk=booking.port_mst_pod_fk))) PODNAME,";
                //end

                Strsql += "job.flight_no VES_VOY,";

                //modifying by thiyagarajan on 3/1/09 to display Place name in report
                //Strsql &= "'' as DEL_PLACE_NAME, "
                Strsql += "(select pl.place_name from place_mst_tbl pl where pl.place_pk=booking.DEL_PLACE_MST_FK) DEL_PLACE_NAME,";
                //end
                Strsql += "(CASE WHEN job.hawb_exp_tbl_fk IS NOT NULL THEN";
                Strsql += "(select hawb.hawb_ref_no from hawb_exp_tbl hawb where hawb.hawb_exp_tbl_pk=job.hawb_exp_tbl_fk)";
                Strsql += "Else";
                Strsql += "(select mawb.mawb_ref_no from mawb_exp_tbl mawb where mawb.mawb_exp_tbl_pk=job.mawb_exp_tbl_fk)";
                Strsql += " END) BLREFNO,";
                Strsql += " job.goods_description, job.marks_numbers MARKS,";
                Strsql += " (select ship.inco_code from shipping_terms_mst_tbl ship where ship.shipping_terms_mst_pk=job.shipping_terms_mst_fk)";
                Strsql += " TERMS,job.pymt_type,NVL(job.INSURANCE_AMT, 0) INSURANCE,";

                Strsql += "(select sum(jstc.gross_weight) from job_trn_air_exp_cont jstc where jstc.job_card_air_exp_fk=job.job_card_air_exp_pk) GROSSWEIGHT,";

                Strsql += " (select sum(jstc.volume_in_cbm) from job_trn_air_exp_cont jstc where jstc.job_card_air_exp_fk=job.job_card_air_exp_pk) VOLUME,";
                Strsql += " (select sum(jstc.chargeable_weight) from job_trn_air_exp_cont jstc where jstc.job_card_air_exp_fk=job.job_card_air_exp_pk) CHARWT ";

                Strsql += " from job_card_air_exp_tbl job ,booking_air_tbl booking, customer_contact_dtls consg";
                Strsql += " where job.job_card_air_exp_pk in (" + JobCardPK + ")";
                Strsql += " and job.booking_air_fk=booking.booking_air_pk";
                Strsql += " and job.consignee_cust_mst_fk=consg.customer_mst_fk";
            }

            try
            {
                return Objwk.GetDataSet(Strsql);
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        #endregion "FetchJobCardAirDetails"

        #region "Fetch Consol invoice Custumer "

        public DataSet CONSOL_INV_CUST_PRINT(int Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with6 = objWK.MyCommand.Parameters;
                _with6.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with6.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with6.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with6.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_INV_CUST_RPT_PRINT");
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
        }

        #endregion "Fetch Consol invoice Custumer "

        #region "Fetch Consol invoice Details Report "

        public DataSet CONSOL_INV_DETAIL_MAIN_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with7 = objWK.MyCommand.Parameters;
                _with7.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with7.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with7.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with7.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_DETAILS_MAIN_RPT_PRINT");
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
        }

        #endregion "Fetch Consol invoice Details Report "

        #region "Fetch Consol invoice Sub Details Report "

        public DataSet CONSOL_INV_DETAIL_SUB_MAIN_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with8 = objWK.MyCommand.Parameters;
                _with8.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with8.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with8.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with8.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_SUB_MAIN_RPT_PRINT");
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
        }

        #endregion "Fetch Consol invoice Sub Details Report "

        #region "Fetch Consol invoice Curr Details Report "

        public DataSet CONSOL_INV_DETAIL_CURR_MAIN_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with9 = objWK.MyCommand.Parameters;
                _with9.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with9.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with9.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with9.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_CUR_MAIN_RPT_PRINT");
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
        }

        #endregion "Fetch Consol invoice Curr Details Report "

        #region "Fetch Location"

        public DataSet FetchLocation(long Loc)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
            StrSqlBuilder.Append("  COP.GST_NO,COP.COMPANY_REG_NO,COP.HOME_PAGE URL, ");
            StrSqlBuilder.Append("  L.LOCATION_ID , L.LOCATION_NAME, ");
            StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,L.E_MAIL_ID,");
            StrSqlBuilder.Append("  L.CITY,CMST.COUNTRY_NAME COUNTRY,L.ZIP, L.LOCATION_MST_PK");
            StrSqlBuilder.Append("  FROM CORPORATE_MST_TBL COP,LOCATION_MST_TBL L,COUNTRY_MST_TBL CMST");
            StrSqlBuilder.Append("  WHERE CMST.COUNTRY_MST_PK(+)=L.COUNTRY_MST_FK AND L.LOCATION_MST_PK = " + Loc + "");

            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "Fetch Location"

        #region "Fetch Barcode Manager Pk"

        public int FetchBarCodeManagerPk(int BizType, int ProcType)
        {
            string StrSql = null;
            DataSet DsBarManager = null;
            int strReturn = 0;
            StringBuilder strquery = new StringBuilder();

            WorkFlow objWF = new WorkFlow();
            //StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt where bdmt.config_id_fk='" & Configid & " '"
            try
            {
                strquery.Append(" Select a.bcd_mst_pk from barcode_data_mst_tbl a");
                strquery.Append("                  where a.config_id_fk='QFOR4078'");
                strquery.Append("                 and a.BCD_MST_FK= (select b.bcd_mst_pk from barcode_data_mst_tbl b ");

                //Sea & Export
                if (BizType == 2 & ProcType == 1)
                {
                    strquery.Append("                   where b.field_name='Export Documentation' ");
                    strquery.Append("                     and b.BCD_MST_FK=2 ) ");
                    //Air Export
                }
                else if (BizType == 1 & ProcType == 1)
                {
                    strquery.Append("                   where b.field_name='Export Documentation' ");
                    strquery.Append("                     and b.BCD_MST_FK=1 ) ");
                    //Air import
                }
                else if (BizType == 1 & ProcType == 2)
                {
                    strquery.Append("                   where b.field_name='Import Documentation' ");
                    strquery.Append("                     and b.BCD_MST_FK=1 ) ");

                    //sea import
                }
                else if (BizType == 2 & ProcType == 2)
                {
                    strquery.Append("                   where b.field_name='Import Documentation' ");
                    strquery.Append("                     and b.BCD_MST_FK=2 ) ");
                }

                DsBarManager = objWF.GetDataSet(strquery.ToString());
                if (DsBarManager.Tables[0].Rows.Count > 0)
                {
                    var _with10 = DsBarManager.Tables[0].Rows[0];
                    strReturn = Convert.ToInt32(_with10["bcd_mst_pk"]);
                }
                return strReturn;
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "Fetch Barcode Manager Pk"

        #region "Fetch Barcode Type"

        public DataSet FetchBarCodeField(int BarCodeManagerPk)
        {
            string StrSql = null;
            DataSet DsBarManager = null;
            int strReturn = 0;
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            try
            {
                strQuery.Append("select bdmt.bcd_mst_pk, bdmt.field_name, bdmt.default_value");
                strQuery.Append("  from barcode_data_mst_tbl bdmt, barcode_doc_data_tbl bddt");
                strQuery.Append(" where bdmt.bcd_mst_pk = bddt.bcd_mst_fk");
                strQuery.Append("   and bdmt.BCD_MST_FK= " + BarCodeManagerPk);
                strQuery.Append(" ORDER BY default_value desc");

                DsBarManager = objWF.GetDataSet(strQuery.ToString());
                return DsBarManager;
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "Fetch Barcode Type"

        #region "Enhance Search"

        public string FetchRemovalInvoice(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            string[] arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strProcessType = null;
            string strloc = "";

            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strProcessType = arr[3];
            strloc = arr[4];

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_REMOVAL_INVOICE_PKG.GET_REMOVAL_INVOICE";

                var _with11 = selectCommand.Parameters;
                _with11.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with11.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                //.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input
                //.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input
                _with11.Add("LOCFK_IN", strloc).Direction = ParameterDirection.Input;
                _with11.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "Enhance Search"

        #region "Fetch Custumer pk And Jobcadpk "

        public DataSet Fetch_CustumerPk(int ConsPk)
        {
            try
            {
                DataSet DsCustPk = null;
                int strReturn = 0;
                WorkFlow objWF = new WorkFlow();
                StringBuilder strQuery = new StringBuilder();
                strQuery.Append("select distinct vb.job_card_fk, cv.customer_mst_fk from REM_INVOICE_TRN_TBL vb ,REM_M_INVOICE_TBL  cv ");
                strQuery.Append("where vb.REMOVALS_INVOICE_FK =" + ConsPk);
                strQuery.Append(" and vb.REMOVALS_INVOICE_FK = cv.REMOVALS_INVOICE_PK ");
                DsCustPk = objWF.GetDataSet(strQuery.ToString());
                return DsCustPk;
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "Fetch Custumer pk And Jobcadpk "

        #region "fetch_Cutumer_pk "

        public DataSet fetch_Cust_pk(string pk)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                if ((pk != "0") & (!string.IsNullOrEmpty(pk)))
                {
                    strQuery.Append("select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name");

                    strQuery.Append("  from rem_m_job_card_mst_tbl j, customer_mst_tbl cmt");
                    strQuery.Append("    where j.JOB_CARD_PK = " + pk);
                    strQuery.Append(" and j.JOB_CARD_SHIPPER_FK = cmt.customer_mst_pk");
                }
                if ((!string.IsNullOrEmpty(strQuery.ToString())))
                {
                    return objWF.GetDataSet(strQuery.ToString());
                }
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
            return new DataSet();
        }

        #endregion "fetch_Cutumer_pk "

        #region "Export to XML"

        public DataSet Export2XML(string InvPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            DataTable dtInv = null;
            DataTable dtInvdet = null;
            DataTable dtJcHead = null;
            DataTable dtJcDet = null;
            DataSet MainDs = new DataSet();

            try
            {
                dtInv = getInvHeader(InvPK);
                dtInvdet = getInvDetails(InvPK);
                dtJcHead = getJcHeader(InvPK);
                dtJcDet = getJcDetails(InvPK);

                MainDs.Tables.Add(dtInv);
                MainDs.Tables.Add(dtInvdet);
                MainDs.Tables.Add(dtJcHead);
                MainDs.Tables.Add(dtJcDet);

                MainDs.Tables[0].TableName = "INV";
                MainDs.Tables[1].TableName = "INVDTL";
                MainDs.Tables[2].TableName = "JC";
                MainDs.Tables[3].TableName = "JCDTL";

                DataRelation relInv_InvDet = new DataRelation("relInv", new DataColumn[] {
                    MainDs.Tables["INV"].Columns["INVPK"],
                    MainDs.Tables["INV"].Columns["JCPK"]
                }, new DataColumn[] {
                    MainDs.Tables["INVDTL"].Columns["INVPK"],
                    MainDs.Tables["INVDTL"].Columns["JCPK"]
                });
                DataRelation relInv_Jc = new DataRelation("relInvJc", new DataColumn[] {
                    MainDs.Tables["INV"].Columns["INVPK"],
                    MainDs.Tables["INV"].Columns["JCPK"]
                }, new DataColumn[] {
                    MainDs.Tables["JC"].Columns["INVPK"],
                    MainDs.Tables["JC"].Columns["JCPK"]
                });
                DataRelation relJc_JcDet = new DataRelation("relJcJcDet", new DataColumn[] {
                    MainDs.Tables["JC"].Columns["INVPK"],
                    MainDs.Tables["JC"].Columns["JCPK"]
                }, new DataColumn[] {
                    MainDs.Tables["JCDTL"].Columns["INVPK"],
                    MainDs.Tables["JCDTL"].Columns["JCPK"]
                });

                relInv_InvDet.Nested = true;
                relInv_Jc.Nested = true;
                relJc_JcDet.Nested = true;

                MainDs.Relations.Add(relInv_InvDet);
                MainDs.Relations.Add(relInv_Jc);
                MainDs.Relations.Add(relJc_JcDet);

                MainDs.DataSetName = "INVOICEDETAILS";

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

        public DataTable getInvHeader(string InvPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            try
            {
                sb.Append("select distinct inv.REMOVALS_INVOICE_PK INVPK,");
                sb.Append("                INVtr.Job_Card_Fk JCPK,");
                sb.Append("                inv.invoice_ref_no invoice_nr,");
                sb.Append("                to_char(inv.invoice_date, 'dd-MON-yyyy') invoice_date,");
                //sb.Append("                to_char(nvl(jcse.departure_date, jcse.etd_date), 'dd-MON-yyyy') activity_date,")
                sb.Append("                to_char(inv.invoice_date, 'dd-MON-yyyy') invoice_actual_due_date,");
                sb.Append("                to_char(inv.invoice_date, 'dd-MON-yyyy') invoice_adjusted_due_date,");
                sb.Append("                'EXP' process_type,");
                sb.Append("                'SEA' business_type,");
                sb.Append("                ' ' ocr_no,");
                sb.Append("                cust.customer_id customer,");
                sb.Append("                ' ' AGENT,");
                sb.Append("                'CUSTOMER' PARTY_TYPE,");
                //sb.Append("                nvl(shmpt.cargo_move_code, ' ') shipping_terms,")
                sb.Append("                'AS PER CONTRACT' payment_terms,");
                sb.Append("                currcorp.currency_id base_currency,");
                sb.Append("                curr.currency_id invoice_currency,");
                sb.Append("                'CONTAINER' shipment,");
                //sb.Append("                nvl(inv.remarks, ' ') remarks,")
                //sb.Append("                jcse.vessel_name vsl,")
                //sb.Append("                jcse.voyage voyage,")
                sb.Append("                'GENERAL' roe_basis,");
                sb.Append("                'STANDARD' roe_type,");
                sb.Append("                get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                            currcorp.currency_mst_pk,");
                sb.Append("                            inv.invoice_date) roe_amount,");
                sb.Append("                '0' invoice_type,");
                sb.Append("                'RECORD1' ref_nr");
                sb.Append("   from rem_m_invoice_tbl     inv,");
                sb.Append("       rem_m_job_card_mst_tbl      jcse,");
                sb.Append("       currency_type_mst_tbl     curr,");
                sb.Append("       currency_type_mst_tbl     currcorp,");
                sb.Append("       rem_invoice_trn_tbl invtr,");
                sb.Append("       CUSTOMER_MST_TBL          cust");
                sb.Append("   where jcse.JOB_CARD_PK = invtr.job_card_fk");
                sb.Append("   and inv.currency_mst_fk = curr.currency_mst_pk");
                sb.Append("   and invtr.REMOVALS_INVOICE_fK = inv.REMOVALS_INVOICE_PK");
                sb.Append("   and cust.customer_mst_pk = inv.customer_mst_fk");
                sb.Append("   and  currcorp.currency_mst_pk=" + HttpContext.Current.Session["currency_mst_pk"]);
                sb.Append("   and inv.REMOVALS_INVOICE_PK = " + InvPK);
                return objWF.GetDataTable(sb.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        public DataTable getInvDetails(string InvPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            try
            {
                sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
                sb.Append("  FROM (select distinct inv.consol_invoice_pk INVPK,");
                sb.Append("                        INVTRN.JOB_CARD_FK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_NAME CHARGE_DESC,");
                sb.Append("                        cntr.container_type_mst_id CONTAINER_TYPE,");
                sb.Append("                        ' ' PACKAGE_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");
                sb.Append("                        QRY.QUANTITY,");
                sb.Append("                        ROUND(QRY.RATE,2) RATE,");
                sb.Append("                        ROUND(JOBFRT.FREIGHT_AMT, 2) AMOUNT,");
                sb.Append("                        'STANDARD' VAT_TYPE,");
                sb.Append("                        NVL((select Distinct (frtv.vat_percentage)");
                sb.Append("                              from frt_vat_country_tbl frtv,");
                sb.Append("                                   user_mst_tbl        umt,");
                sb.Append("                                   location_mst_tbl    loc");
                sb.Append("                             where umt.default_location_fk =");
                sb.Append("                                   loc.location_mst_pk");
                sb.Append("                               and loc.country_mst_fk = frtv.country_mst_fk");
                sb.Append("                               and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
                sb.Append("                               and FMT.freight_element_mst_pk =");
                sb.Append("                                   frtv.freight_element_mst_fk(+)),");
                sb.Append("                            CORP.VAT_PERCENTAGE) VAT_PERCENTAGE,");
                sb.Append("                        ROUND((NVL((select Distinct (frtv.vat_percentage)");
                sb.Append("                               from frt_vat_country_tbl frtv,");
                sb.Append("                                    user_mst_tbl        umt,");
                sb.Append("                                    location_mst_tbl    loc");
                sb.Append("                              where umt.default_location_fk =");
                sb.Append("                                    loc.location_mst_pk");
                sb.Append("                                and loc.country_mst_fk = frtv.country_mst_fk");
                sb.Append("                                and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
                sb.Append("                                and FMT.freight_element_mst_pk =");
                sb.Append("                                    frtv.freight_element_mst_fk(+)),");
                sb.Append("                             CORP.VAT_PERCENTAGE) *");
                sb.Append("                        (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) / 100),2) VAT_AMOUNT,");
                sb.Append("                        ROUND(((NVL((select Distinct (frtv.vat_percentage)");
                sb.Append("                                from frt_vat_country_tbl frtv,");
                sb.Append("                                     user_mst_tbl        umt,");
                sb.Append("                                     location_mst_tbl    loc");
                sb.Append("                               where umt.default_location_fk =");
                sb.Append("                                     loc.location_mst_pk");
                sb.Append("                                 and loc.country_mst_fk =");
                sb.Append("                                     frtv.country_mst_fk");
                sb.Append("                                 and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
                sb.Append("                                 and FMT.freight_element_mst_pk =");
                sb.Append("                                     frtv.freight_element_mst_fk(+)),");
                sb.Append("                              CORP.VAT_PERCENTAGE) *");
                sb.Append("                        (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) / 100) +");
                sb.Append("                        JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE),2) AMT_INCL_VAT,");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                    corp.currency_mst_fk,");
                sb.Append("                                    inv.invoice_date) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE, 2) AMT_IN_INV_CUR,");
                sb.Append("                        ROUND((ROUND(JOBFRT.FREIGHT_AMT *");
                sb.Append("                               GET_EX_RATE(CORP.CURRENCY_MST_FK,");
                sb.Append("                                           JOBFRT.CURRENCY_MST_FK,");
                sb.Append("                                           SYSDATE),");
                sb.Append("                               4) / 100),2) INV_AMT_IN_BASE_CUR,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        fmt.charge_basis CHARGE_BASIS,");
                sb.Append("                        'OPEN' COLLECT_STATUS,");
                sb.Append("                        'F' ADDITIONALCHARGES");
                sb.Append("          from consol_invoice_trn_tbl invtrn,");
                sb.Append("               consol_invoice_tbl inv,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("               job_card_sea_exp_tbl jcse,");
                sb.Append("               JOB_TRN_SEA_EXP_FD JOBFRT,");
                sb.Append("               container_type_mst_tbl cntr,");
                sb.Append("               job_trn_sea_exp_cont jccntr,");
                sb.Append("               CORPORATE_MST_TBL CORP,");
                sb.Append("               (SELECT JJ.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                       JJ.FREIGHT_AMT / SUM(JJJ.CONTAINER_TYPE_MST_FK) RATE,");
                sb.Append("                       JJ.FREIGHT_AMT,");
                sb.Append("                       (SELECT COUNT(JJJ.CONTAINER_TYPE_MST_FK)");
                sb.Append("                          FROM job_trn_sea_exp_cont      JJJ,");
                sb.Append("                               CONSOL_INVOICE_TRN_TBL I,");
                sb.Append("                               CONSOL_INVOICE_TBL     II");
                sb.Append("                         WHERE II.CONSOL_INVOICE_PK = " + InvPK);
                sb.Append("                           AND I.CONSOL_INVOICE_FK =");
                sb.Append("                               II.CONSOL_INVOICE_PK");
                sb.Append("                           AND JJJ.JOB_CARD_SEA_EXP_FK =");
                sb.Append("                               I.JOB_CARD_FK) QUANTITY");
                sb.Append("                  FROM JOB_TRN_SEA_EXP_FD        JJ,");
                sb.Append("                       job_trn_sea_exp_cont      JJJ,");
                sb.Append("                       CONSOL_INVOICE_TRN_TBL I,");
                sb.Append("                       CONSOL_INVOICE_TBL     II");
                sb.Append("                 WHERE II.CONSOL_INVOICE_PK = " + InvPK);
                sb.Append("                   AND I.CONSOL_INVOICE_FK = II.CONSOL_INVOICE_PK");
                sb.Append("                   AND JJJ.JOB_CARD_SEA_EXP_FK = I.JOB_CARD_FK");
                sb.Append("                   AND JJ.FREIGHT_TYPE IN (1, 2)");
                sb.Append("                   AND JJJ.JOB_CARD_SEA_EXP_FK = JJ.JOB_CARD_SEA_EXP_FK(+)");
                sb.Append("                 GROUP BY JJ.FREIGHT_AMT,");
                sb.Append("                          JJ.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                          JJJ.CONTAINER_TYPE_MST_FK) QRY");
                sb.Append("         where inv.consol_invoice_pk =  " + InvPK);
                sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
                sb.Append("           and jcse.job_card_sea_exp_pk = INVTRN.JOB_CARD_FK");
                sb.Append("           AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           and cntr.container_type_mst_pk = jobfrt.container_type_mst_fk");
                sb.Append("           AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND QRY.FREIGHT_ELEMENT_MST_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK");
                sb.Append("           and jccntr.job_card_sea_exp_fk = jcse.job_card_sea_exp_pk");
                sb.Append("           AND JOBFRT.FREIGHT_TYPE IN (1, 2)");
                sb.Append("           AND JOBFRT.JOB_CARD_SEA_EXP_FK = JCSE.JOB_CARD_SEA_EXP_PK");
                sb.Append("        union");
                sb.Append("        select distinct inv.consol_invoice_pk INVPK,");
                sb.Append("                        INVTRN.JOB_CARD_FK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_NAME CHARGE_DESC,");
                sb.Append("                        ' ' CONTAINER_TYPE,");
                sb.Append("                        ' ' PACKAGE_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");
                sb.Append("                        QRY.QUANTITY,");
                sb.Append("                        ROUND(QRY.RATE,2) RATE,");
                sb.Append("                        ROUND(JOBOTH.Amount, 2) AMOUNT,");
                sb.Append("                        'STANDARD' VAT_TYPE,");
                sb.Append("                        NVL((select Distinct (frtv.vat_percentage)");
                sb.Append("                              from frt_vat_country_tbl frtv,");
                sb.Append("                                   user_mst_tbl        umt,");
                sb.Append("                                   location_mst_tbl    loc");
                sb.Append("                             where umt.default_location_fk =");
                sb.Append("                                   loc.location_mst_pk");
                sb.Append("                               and loc.country_mst_fk = frtv.country_mst_fk");
                sb.Append("                               and umt.user_mst_pk =" + HttpContext.Current.Session["USER_PK"]);
                sb.Append("                               and FMT.freight_element_mst_pk =");
                sb.Append("                                   frtv.freight_element_mst_fk(+)),");
                sb.Append("                            CORP.VAT_PERCENTAGE) VAT_PERCENTAGE,");
                sb.Append("                        ROUND((NVL((select Distinct (frtv.vat_percentage)");
                sb.Append("                               from frt_vat_country_tbl frtv,");
                sb.Append("                                    user_mst_tbl        umt,");
                sb.Append("                                    location_mst_tbl    loc");
                sb.Append("                              where umt.default_location_fk =");
                sb.Append("                                    loc.location_mst_pk");
                sb.Append("                                and loc.country_mst_fk = frtv.country_mst_fk");
                sb.Append("                                and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
                sb.Append("                                and FMT.freight_element_mst_pk =");
                sb.Append("                                    frtv.freight_element_mst_fk(+)),");
                sb.Append("                             CORP.VAT_PERCENTAGE) *");
                sb.Append("                        (JOBOTH.Amount * JOBOTH.EXCHANGE_RATE) / 100),2) VAT_AMOUNT,");
                sb.Append("                        ROUND(((NVL((select Distinct (frtv.vat_percentage)");
                sb.Append("                                from frt_vat_country_tbl frtv,");
                sb.Append("                                     user_mst_tbl        umt,");
                sb.Append("                                     location_mst_tbl    loc");
                sb.Append("                               where umt.default_location_fk =");
                sb.Append("                                     loc.location_mst_pk");
                sb.Append("                                 and loc.country_mst_fk =");
                sb.Append("                                     frtv.country_mst_fk");
                sb.Append("                                 and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
                sb.Append("                                 and FMT.freight_element_mst_pk =");
                sb.Append("                                     frtv.freight_element_mst_fk(+)),");
                sb.Append("                              CORP.VAT_PERCENTAGE) *");
                sb.Append("                        (JOBOTH.Amount * JOBOTH.EXCHANGE_RATE) / 100) +");
                sb.Append("                        JOBOTH.Amount * JOBOTH.EXCHANGE_RATE),2) AMT_INCL_VAT,");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                    corp.currency_mst_fk,");
                sb.Append("                                    inv.invoice_date) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND(JOBOTH.Amount * JOBOTH.EXCHANGE_RATE, 2) AMT_IN_INV_CUR,");
                sb.Append("                        ROUND((ROUND(JOBOTH.Amount *");
                sb.Append("                               GET_EX_RATE(CORP.CURRENCY_MST_FK,");
                sb.Append("                                           JOBOTH.CURRENCY_MST_FK,");
                sb.Append("                                           SYSDATE),");
                sb.Append("                               4) / 100),2) INV_AMT_IN_BASE_CUR,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        fmt.charge_basis CHARGE_BASIS,");
                sb.Append("                        'OPEN' COLLECT_STATUS,");
                sb.Append("                        'T' ADDITIONALCHARGES");
                sb.Append("          from consol_invoice_trn_tbl invtrn,");
                sb.Append("               consol_invoice_tbl inv,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("               job_card_sea_exp_tbl jcse,");
                sb.Append("               JOB_TRN_SEA_EXP_OTH_CHRG joboth,");
                sb.Append("               container_type_mst_tbl cntr,");
                sb.Append("               job_trn_sea_exp_cont jccntr,");
                sb.Append("               (SELECT JJ.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                       JJ.Amount / SUM(JJJ.CONTAINER_TYPE_MST_FK) RATE,");
                sb.Append("                       JJ.Amount,");
                sb.Append("                       (SELECT COUNT(JJJ.CONTAINER_TYPE_MST_FK)");
                sb.Append("                          FROM job_trn_sea_exp_cont      JJJ,");
                sb.Append("                               consol_invoice_trn_tbl I,");
                sb.Append("                               consol_invoice_tbl     II");
                sb.Append("                         WHERE II.CONSOL_INVOICE_PK = " + InvPK);
                sb.Append("                           AND I.CONSOL_INVOICE_FK =");
                sb.Append("                               II.CONSOL_INVOICE_PK");
                sb.Append("                           AND JJJ.JOB_CARD_SEA_EXP_FK =");
                sb.Append("                               I.JOB_CARD_FK) QUANTITY");
                sb.Append("                  FROM JOB_TRN_SEA_EXP_OTH_CHRG  JJ,");
                sb.Append("                       job_trn_sea_exp_cont      JJJ,");
                sb.Append("                       consol_invoice_TRN_tbl I,");
                sb.Append("                       consol_invoice_tbl     II");
                sb.Append("                 WHERE II.CONSOL_INVOICE_PK  = " + InvPK);
                sb.Append("                   AND I.CONSOL_INVOICE_FK = II.CONSOL_INVOICE_PK");
                sb.Append("                   AND JJJ.JOB_CARD_SEA_EXP_FK = I.JOB_CARD_FK");
                sb.Append("                   AND JJ.FREIGHT_TYPE IN (1, 2)");
                sb.Append("                   AND JJJ.JOB_CARD_SEA_EXP_FK = JJ.JOB_CARD_SEA_EXP_FK(+)");
                sb.Append("                 GROUP BY JJ.Amount,");
                sb.Append("                          JJ.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                          JJJ.CONTAINER_TYPE_MST_FK) QRY,");
                sb.Append("               CORPORATE_MST_TBL CORP");
                sb.Append("         where inv.consol_invoice_pk = " + InvPK);
                sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
                sb.Append("           and jcse.job_card_sea_exp_pk = invTRN.Job_Card_Fk");
                sb.Append("           AND joboth.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND QRY.FREIGHT_ELEMENT_MST_FK = JOBOTH.FREIGHT_ELEMENT_MST_FK");
                sb.Append("           AND joboth.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND JOBOTH.INV_AGENT_TRN_SEA_EXP_FK IS NULL");
                sb.Append("           AND JOBOTH.INV_CUST_TRN_SEA_EXP_FK IS NULL");
                sb.Append("           AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("           AND JOBOTH.Freight_Type IN (1, 2)");
                sb.Append("           and jccntr.job_card_sea_exp_fk = jcse.job_card_sea_exp_pk");
                sb.Append("           AND JOBOTH.JOB_CARD_SEA_EXP_FK = JCSE.JOB_CARD_SEA_EXP_PK) Q");

                return objWF.GetDataTable(sb.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        public DataTable getJcHeader(string InvPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            try
            {
                sb.Append("SELECT distinct INV.CONSOL_INVOICE_PK INVPK,");
                sb.Append("       INVtrn.Job_Card_Fk JCPK,");
                sb.Append("       TO_CHAR(JCSE.JOBCARD_DATE,'DD-MON-YYYY') SALES_DATE,");
                sb.Append("       to_char(nvl(jcse.departure_date, jcse.etd_date), 'dd-MON-yyyy') SALES_ACT_DATE,");
                sb.Append("       'SEA' BUSINESS_TYPE,");
                sb.Append("       nvl(POR.PLACE_CODE, ' ') POR,");
                sb.Append("       nvl(POL.PORT_ID, ' ') POL,");
                sb.Append("       nvl(POD.PORT_ID, ' ') POD,");
                sb.Append("       nvl(PLD.PLACE_CODE, ' ') PFD,");
                sb.Append("       CUST.CUSTOMER_ID CUSTOMER,");
                sb.Append("       ' ' AGENT,");
                sb.Append("       'CUSTOMER' PARTY_TYPE,");
                sb.Append("       nvl(JCSE.VESSEL_NAME, ' ') VSL,");
                sb.Append("       nvl(JCSE.VOYAGE, ' ') VOYAGE,");
                sb.Append("       CURR.CURRENCY_ID BASE_CURRENCY,");
                sb.Append("       'SEA' PROCESS_TYPE,");
                sb.Append("       BKG.BOOKING_REF_NO BOOKING_REF_NO,");
                sb.Append("       to_char(BKG.BOOKING_DATE, 'DD-MON-YYYY') BOOKING_DATE,");
                sb.Append("       HBL.HBL_REF_NO BL_REF_NO,");
                sb.Append("       to_char(HBL.HBL_DATE, 'DD-MON-YYYY') BL_DATE,");
                sb.Append("       JCSE.JOBCARD_REF_NO JOBCARD_REF_NO,");
                sb.Append("       to_char(JCSE.JOBCARD_DATE, 'DD-MON-YYYY') JOB_CARD_DATE,");
                sb.Append("       'CONTAINER' SHIPMENT_TYPE,");
                sb.Append("       NVL(SHMT.CARGO_MOVE_CODE, ' ') SHIPPING_TERMS,");
                sb.Append("       'GENERAL' ROE_BASIS,");
                sb.Append("       'INVOICED' STATUS,");
                sb.Append("       'RECORD1' REF_NR");
                sb.Append("  FROM JOB_CARD_SEA_EXP_TBL   JCSE,");
                sb.Append("       consol_invoice_tbl   INV, consol_invoice_trn_tbl invtrn,");
                sb.Append("       BOOKING_SEA_TBL        BKG,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       PLACE_MST_TBL          POR,");
                sb.Append("       PLACE_MST_TBL          PLD,");
                sb.Append("       CURRENCY_TYPE_MST_TBL  CURR,");
                sb.Append("       HBL_EXP_TBL            HBL,");
                sb.Append("       CORPORATE_MST_TBL      CORP,");
                sb.Append("       cargo_move_mst_tbl SHMT,");
                sb.Append("       AGENT_MST_TBL          AGT,");
                sb.Append("       AGENT_MST_TBL          AGTDP,");
                sb.Append("       CUSTOMER_MST_TBL       CUST");
                sb.Append(" WHERE INVtrn.Job_Card_Fk = JCSE.JOB_CARD_SEA_EXP_PK and inv.consol_invoice_pk = invtrn.consol_invoice_fk");
                sb.Append("   AND INV.Consol_Invoice_Pk = " + InvPK);
                sb.Append("   AND CORP.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
                sb.Append("   AND JCSE.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                sb.Append("   AND HBL.JOB_CARD_SEA_EXP_FK(+) = JCSE.JOB_CARD_SEA_EXP_PK");
                sb.Append("   AND SHMT.CARGO_MOVE_PK(+) = JCSE.Cargo_Move_Fk");
                sb.Append("   AND JCSE.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                sb.Append("   AND BKG.COL_PLACE_MST_FK = POR.PLACE_PK(+)");
                sb.Append("   AND BKG.DEL_PLACE_MST_FK = PLD.PLACE_PK(+)");
                sb.Append("   AND BKG.PORT_MST_POD_FK = POL.PORT_MST_PK");
                sb.Append("   AND BKG.PORT_MST_POL_FK = POD.PORT_MST_PK");
                sb.Append("   AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                sb.Append("   AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");

                return objWF.GetDataTable(sb.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        public DataTable getJcDetails(string InvPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            try
            {
                sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
                sb.Append("  FROM (select distinct INV.CONSOL_INVOICE_PK INVPK, ");
                sb.Append("                        JCSE.JOB_CARD_SEA_EXP_PK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        cntr.container_type_mst_id CONTAINER_TYPE,");
                sb.Append("                        ' ' PACKAGE_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");
                sb.Append("                        ROUND(QRY.RATE,2) RATE,");
                sb.Append("                        QRY.QUANTITY,");
                sb.Append("                        ROUND(JOBFRT.FREIGHT_AMT, 2) TRANSACTION_AMOUNT,");
                sb.Append("                        NVL((select Distinct (frtv.vat_percentage)");
                sb.Append("                              from frt_vat_country_tbl frtv,");
                sb.Append("                                   user_mst_tbl        umt,");
                sb.Append("                                   location_mst_tbl    loc");
                sb.Append("                             where umt.default_location_fk =");
                sb.Append("                                   loc.location_mst_pk");
                sb.Append("                               and loc.country_mst_fk = frtv.country_mst_fk");
                sb.Append("                               and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
                sb.Append("                               and FMT.freight_element_mst_pk =");
                sb.Append("                                   frtv.freight_element_mst_fk(+)),");
                sb.Append("                            CORP.VAT_PERCENTAGE) VAT_PERCENTAGE,");
                sb.Append("                        'STANDARD' VAT_TYPE,");
                sb.Append("                        ROUND((NVL((select Distinct (frtv.vat_percentage)");
                sb.Append("                               from frt_vat_country_tbl frtv,");
                sb.Append("                                    user_mst_tbl        umt,");
                sb.Append("                                    location_mst_tbl    loc");
                sb.Append("                              where umt.default_location_fk =");
                sb.Append("                                    loc.location_mst_pk");
                sb.Append("                                and loc.country_mst_fk = frtv.country_mst_fk");
                sb.Append("                                and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
                sb.Append("                                and FMT.freight_element_mst_pk =");
                sb.Append("                                    frtv.freight_element_mst_fk(+)),");
                sb.Append("                             CORP.VAT_PERCENTAGE) *");
                sb.Append("                        (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) / 100),2) VAT_AMOUNT,");
                sb.Append("                        ROUND(((NVL((select Distinct (frtv.vat_percentage)");
                sb.Append("                                from frt_vat_country_tbl frtv,");
                sb.Append("                                     user_mst_tbl        umt,");
                sb.Append("                                     location_mst_tbl    loc");
                sb.Append("                               where umt.default_location_fk =");
                sb.Append("                                     loc.location_mst_pk");
                sb.Append("                                 and loc.country_mst_fk =");
                sb.Append("                                     frtv.country_mst_fk");
                sb.Append("                                 and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
                sb.Append("                                 and FMT.freight_element_mst_pk =");
                sb.Append("                                     frtv.freight_element_mst_fk(+)),");
                sb.Append("                              CORP.VAT_PERCENTAGE) *");
                sb.Append("                        (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) / 100) +");
                sb.Append("                        JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE),2) AMOUNT_INCL_VAT,");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                    corp.currency_mst_fk,");
                sb.Append("                                    inv.invoice_date) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND((ROUND(JOBFRT.FREIGHT_AMT *");
                sb.Append("                               GET_EX_RATE(CORP.CURRENCY_MST_FK,");
                sb.Append("                                           JOBFRT.CURRENCY_MST_FK,");
                sb.Append("                                           SYSDATE),");
                sb.Append("                               4) / 100),2) AMOUNT_IN_BASECURRENCY,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        CUST.CUSTOMER_ID CUSTOMER,");
                sb.Append("                        ' ' AGENT,");
                sb.Append("                        'CUSTOMER' PARTY_TYPE,");
                sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
                sb.Append("          from CONSOL_INVOICE_TRN_TBL  invtrn,");
                sb.Append("               CONSOL_INVOICE_TBL  inv,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("               job_card_sea_exp_tbl jcse,");
                sb.Append("               JOB_TRN_SEA_EXP_FD JOBFRT,");
                sb.Append("               container_type_mst_tbl cntr,");
                sb.Append("               job_trn_sea_exp_cont jccntr,");
                sb.Append("               CORPORATE_MST_TBL CORP,");
                sb.Append("               CUSTOMER_MST_TBL CUST,");
                sb.Append("               AGENT_MST_TBL AGT,");
                sb.Append("               AGENT_MST_TBL AGTDP,");
                sb.Append("               LOCATION_MST_TBL LOC,");
                sb.Append("               USER_MST_TBL USR,");
                sb.Append("               (SELECT JJ.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                       JJ.FREIGHT_AMT / SUM(JJJ.CONTAINER_TYPE_MST_FK) RATE,");
                sb.Append("                       JJ.FREIGHT_AMT,");
                sb.Append("                       (SELECT COUNT(JJJ.CONTAINER_TYPE_MST_FK)");
                sb.Append("                          FROM job_trn_sea_exp_cont      JJJ,");
                sb.Append("                               CONSOL_INVOICE_TRN_TBL  I,");
                sb.Append("                               CONSOL_INVOICE_TBL          II");
                sb.Append("                         WHERE II.CONSOL_INVOICE_PK  = " + InvPK);
                sb.Append("                           AND I.CONSOL_INVOICE_FK =");
                sb.Append("                               II.CONSOL_INVOICE_PK");
                sb.Append("                           AND JJJ.JOB_CARD_SEA_EXP_FK =");
                sb.Append("                                I.JOB_CARD_FK) QUANTITY");
                sb.Append("                  FROM JOB_TRN_SEA_EXP_FD        JJ,");
                sb.Append("                       job_trn_sea_exp_cont      JJJ,");
                sb.Append("                       CONSOL_INVOICE_TRN_TBL  I,");
                sb.Append("                       CONSOL_INVOICE_TBL          II");
                sb.Append("                 WHERE II.CONSOL_INVOICE_PK = " + InvPK);
                sb.Append("                    AND I.CONSOL_INVOICE_FK = II.CONSOL_INVOICE_PK");
                sb.Append("                   AND JJJ.JOB_CARD_SEA_EXP_FK = I.JOB_CARD_FK");
                sb.Append("                   AND JJ.FREIGHT_TYPE IN (1, 2)");
                sb.Append("                   AND JJJ.JOB_CARD_SEA_EXP_FK = JJ.JOB_CARD_SEA_EXP_FK(+)");
                sb.Append("                 GROUP BY JJ.FREIGHT_AMT,");
                sb.Append("                          JJ.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                          JJJ.CONTAINER_TYPE_MST_FK) QRY");
                sb.Append("         where inv.consol_invoice_pk = " + InvPK);
                sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
                sb.Append("           and jcse.job_card_sea_exp_pk = invTRN.Job_Card_Fk");
                sb.Append("           AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           and cntr.container_type_mst_pk = jobfrt.container_type_mst_fk");
                sb.Append("           AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND QRY.FREIGHT_ELEMENT_MST_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK");
                sb.Append("           and jccntr.job_card_sea_exp_fk = jcse.job_card_sea_exp_pk");
                sb.Append("           AND JOBFRT.FREIGHT_TYPE IN (1, 2)");
                sb.Append("           AND JOBFRT.JOB_CARD_SEA_EXP_FK = JCSE.JOB_CARD_SEA_EXP_PK");
                sb.Append("           AND JCSE.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
                sb.Append("           AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                sb.Append("           AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)");
                sb.Append("           AND USR.USER_MST_PK = JCSE.CREATED_BY_FK");
                sb.Append("        union");
                sb.Append("        select distinct INV.CONSOL_INVOICE_PK INVPK,");
                sb.Append("                        JCSE.JOB_CARD_SEA_EXP_PK JCPK,");
                sb.Append("                        FMT.FREIGHT_ELEMENT_ID CHARGE_CODE,");
                sb.Append("                        ' ' CONTAINER_TYPE,");
                sb.Append("                        ' ' PACKAGE_TYPE,");
                sb.Append("                        CUMT.CURRENCY_ID TRANSACTION_CURRENCY,");
                sb.Append("                        ROUND(QRY.RATE,2) RATE,");
                sb.Append("                        QRY.QUANTITY,");
                sb.Append("                        ROUND(JOBOTH.Amount, 2) TRANSACTION_AMOUNT,");
                sb.Append("                        NVL((select Distinct (frtv.vat_percentage)");
                sb.Append("                              from frt_vat_country_tbl frtv,");
                sb.Append("                                   user_mst_tbl        umt,");
                sb.Append("                                   location_mst_tbl    loc");
                sb.Append("                             where umt.default_location_fk =");
                sb.Append("                                   loc.location_mst_pk");
                sb.Append("                               and loc.country_mst_fk = frtv.country_mst_fk");
                sb.Append("                               and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
                sb.Append("                               and FMT.freight_element_mst_pk =");
                sb.Append("                                   frtv.freight_element_mst_fk(+)),");
                sb.Append("                            CORP.VAT_PERCENTAGE) VAT_PERCENTAGE,");
                sb.Append("                        'STANDARD' VAT_TYPE,");
                sb.Append("                        ROUND((NVL((select Distinct (frtv.vat_percentage)");
                sb.Append("                               from frt_vat_country_tbl frtv,");
                sb.Append("                                    user_mst_tbl        umt,");
                sb.Append("                                    location_mst_tbl    loc");
                sb.Append("                              where umt.default_location_fk =");
                sb.Append("                                    loc.location_mst_pk");
                sb.Append("                                and loc.country_mst_fk = frtv.country_mst_fk");
                sb.Append("                                and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
                sb.Append("                                and FMT.freight_element_mst_pk =");
                sb.Append("                                    frtv.freight_element_mst_fk(+)),");
                sb.Append("                             CORP.VAT_PERCENTAGE) *");
                sb.Append("                        (JOBOTH.Amount * JOBOTH.EXCHANGE_RATE) / 100),2) VAT_AMOUNT,");
                sb.Append("                        ROUND(((NVL((select Distinct (frtv.vat_percentage)");
                sb.Append("                                from frt_vat_country_tbl frtv,");
                sb.Append("                                     user_mst_tbl        umt,");
                sb.Append("                                     location_mst_tbl    loc");
                sb.Append("                               where umt.default_location_fk =");
                sb.Append("                                     loc.location_mst_pk");
                sb.Append("                                 and loc.country_mst_fk =");
                sb.Append("                                     frtv.country_mst_fk");
                sb.Append("                                 and umt.user_mst_pk = " + HttpContext.Current.Session["USER_PK"]);
                sb.Append("                                 and FMT.freight_element_mst_pk =");
                sb.Append("                                     frtv.freight_element_mst_fk(+)),");
                sb.Append("                              CORP.VAT_PERCENTAGE) *");
                sb.Append("                        (JOBOTH.Amount * JOBOTH.EXCHANGE_RATE) / 100) +");
                sb.Append("                        JOBOTH.Amount * JOBOTH.EXCHANGE_RATE),2) AMOUNT_INCL_VAT,");
                sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
                sb.Append("                                    corp.currency_mst_fk,");
                sb.Append("                                    inv.invoice_date) ROE,");
                sb.Append("                        'STANDARD' ROE_TYPE,");
                sb.Append("                        ROUND((ROUND(JOBOTH.Amount *");
                sb.Append("                               GET_EX_RATE(CORP.CURRENCY_MST_FK,");
                sb.Append("                                           JOBOTH.CURRENCY_MST_FK,");
                sb.Append("                                           SYSDATE),");
                sb.Append("                               4) / 100),2) AMOUNT_IN_BASECURRENCY,");
                sb.Append("                        (CASE");
                sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
                sb.Append("                           'P'");
                sb.Append("                          ELSE");
                sb.Append("                           'C'");
                sb.Append("                        END) PREPAID_COLLECT,");
                sb.Append("                        CUST.CUSTOMER_ID CUSTOMER,");
                sb.Append("                        ' ' AGENT,");
                sb.Append("                        'CUSTOMER' PARTY_TYPE,");
                sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
                sb.Append("          from CONSOL_INVOICE_TRN_TBL  invtrn,");
                sb.Append("               CONSOL_INVOICE_TBL  inv,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL CUMT,");
                sb.Append("               job_card_sea_exp_tbl jcse,");
                sb.Append("               JOB_TRN_SEA_EXP_OTH_CHRG joboth,");
                sb.Append("               container_type_mst_tbl cntr,");
                sb.Append("               job_trn_sea_exp_cont jccntr,");
                sb.Append("               CUSTOMER_MST_TBL CUST,");
                sb.Append("               CORPORATE_MST_TBL CORP,");
                sb.Append("               AGENT_MST_TBL AGT,");
                sb.Append("               AGENT_MST_TBL AGTDP,");
                sb.Append("               LOCATION_MST_TBL LOC,");
                sb.Append("               USER_MST_TBL USR,");
                sb.Append("               (SELECT JJ.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                       JJ.Amount / SUM(JJJ.CONTAINER_TYPE_MST_FK) RATE,");
                sb.Append("                       JJ.Amount,");
                sb.Append("                       (SELECT COUNT(JJJ.CONTAINER_TYPE_MST_FK)");
                sb.Append("                          FROM job_trn_sea_exp_cont      JJJ,");
                sb.Append("                               CONSOL_INVOICE_TRN_TBL I,");
                sb.Append("                               CONSOL_INVOICE_TBL     II");
                sb.Append("                         WHERE II.CONSOL_INVOICE_PK = " + InvPK);
                sb.Append("                           AND I.CONSOL_INVOICE_FK =");
                sb.Append("                               II.CONSOL_INVOICE_PK");
                sb.Append("                           AND JJJ.JOB_CARD_SEA_EXP_FK =");
                sb.Append("                               I.JOB_CARD_FK) QUANTITY");
                sb.Append("                  FROM JOB_TRN_SEA_EXP_OTH_CHRG  JJ,");
                sb.Append("                       job_trn_sea_exp_cont      JJJ,");
                sb.Append("                       CONSOL_INVOICE_TRN_TBL  I,");
                sb.Append("                       CONSOL_INVOICE_TBL          II");
                sb.Append("                 WHERE II.CONSOL_INVOICE_PK = " + InvPK);
                sb.Append("                   AND I.CONSOL_INVOICE_FK = II.CONSOL_INVOICE_PK");
                sb.Append("                   AND JJJ.JOB_CARD_SEA_EXP_FK = I.JOB_CARD_FK");
                sb.Append("                   AND JJ.FREIGHT_TYPE IN (1, 2)");
                sb.Append("                   AND JJJ.JOB_CARD_SEA_EXP_FK = JJ.JOB_CARD_SEA_EXP_FK(+)");
                sb.Append("                 GROUP BY JJ.Amount,");
                sb.Append("                          JJ.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                          JJJ.CONTAINER_TYPE_MST_FK) QRY");
                sb.Append("         where inv.consol_invoice_pk  = " + InvPK);
                sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
                sb.Append("           and jcse.job_card_sea_exp_pk = INVTRN.JOB_CARD_FK");
                sb.Append("           AND joboth.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("           AND QRY.FREIGHT_ELEMENT_MST_FK = JOBOTH.FREIGHT_ELEMENT_MST_FK");
                sb.Append("           AND joboth.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
                sb.Append("           AND JOBOTH.INV_AGENT_TRN_SEA_EXP_FK IS NULL");
                sb.Append("           AND JOBOTH.INV_CUST_TRN_SEA_EXP_FK IS NULL");
                sb.Append("           AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("           AND JOBOTH.Freight_Type = 2");
                sb.Append("           and jccntr.job_card_sea_exp_fk = jcse.job_card_sea_exp_pk");
                sb.Append("           AND JOBOTH.JOB_CARD_SEA_EXP_FK = JCSE.JOB_CARD_SEA_EXP_PK");
                sb.Append("           AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
                sb.Append("           AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
                sb.Append("           AND USR.USER_MST_PK = JCSE.CREATED_BY_FK");
                sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)) Q");

                return objWF.GetDataTable(sb.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
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

        #endregion "Export to XML"
    }
}