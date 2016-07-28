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
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_BBInvoiceDetails : CommonFeatures
    {
        // Fetch Functionality For fetching JobCards whose freight payers are matching with selected customer for consolidated invoice // Added By jitendra on 22/05/07

        #region "Property"
        long lngInvPk;
        //Snigdharani - 08/01/2009 - Unique Invoice Reference Number(VEK Client)
        public string uniqueReferenceNr;
        public long ReturnSavePk
        {
            get { return lngInvPk; }
            set { lngInvPk = value; }
        }
        #endregion

        #region "FetchCredteDays"
        //adding by thiyagarajan on 4/12/08 for introducing "Payment Due" facility in report
        public DataSet GetDtVat(Int32 invpk)
        {
            System.Text.StringBuilder strquery = new System.Text.StringBuilder();
            WorkFlow objWf = new WorkFlow();
            try
            {
                strquery.Append(" select c.invoice_date,cust.vat_no from consol_invoice_tbl c , customer_mst_tbl cust where ");
                strquery.Append(" c.customer_mst_fk=cust.customer_mst_pk and  c.consol_invoice_pk=" + invpk);
                return objWf.GetDataSet(strquery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
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
                strquery.Append(" select J.BOOKING_MST_FK,cust.customer_id,cust.credit_days from JOB_CARD_TRN j,");
                strquery.Append(" CUSTOMER_MST_TBL CUST where j.job_card_trn_pk in ");
                strquery.Append(" (select c.job_card_fk  from consol_invoice_trn_tbl c where c.consol_invoice_fk= " + invpk);
                strquery.Append(" group by c.job_card_fk) and j.shipper_cust_mst_fk=cust.customer_mst_pk ");
                dsinv = objWf.GetDataSet(strquery.ToString());
                if (dsinv.Tables[0].Rows.Count > 1)
                {
                    strquery = strquery.Remove(0, strquery.Length - 1);
                    //strquery.Append("select to_char('" & invdate & "' + '" & dsinv.Tables(0).Rows(0)["credit_days") & "' 'DD/MM/YYYY') from dual ")
                    strquery.Append("  select to_char(c1.invoice_date" + dsinv.Tables[0].Rows[0]["credit_days"] + " ,'dd/mm/yyyy') from consol_invoice_tbl c1 where c1.consol_invoice_pk= " + invpk);
                    return objWf.ExecuteScaler(strquery.ToString());
                }

                string strSQL = null;

                strSQL = " select b.credit_days,to_CHAR(con.invoice_date,'dd/mm/yyyy')invdt  , (case when b.credit_days>0 then ";
                strSQL += " to_CHAR(con.invoice_date+b.credit_days,'dd/mm/yyyy') end )crdate ";
                strSQL += " from JOB_CARD_TRN j,booking_mst_tbl b,consol_invoice_tbl con,consol_invoice_trn_tbl inv ";

                if (Process == 2)
                {
                    if (biztype == 2)
                    {
                        strSQL += " , JOB_CARD_TRN impj ";
                    }
                    else
                    {
                        strSQL += " , JOB_CARD_TRN impj ";
                    }
                }
                strSQL += " where inv.consol_invoice_fk= " + invpk;
                strSQL += "  and con.consol_invoice_pk=inv.consol_invoice_fk ";
                if (Process == 1)
                {
                    strSQL += " and inv.job_card_fk=j.job_card_trn_pk and j.booking_mst_fk=b.booking_mst_pk";
                }
                else
                {
                    strSQL += " and inv.job_card_fk=impj.JOB_CARD_TRN_PK and j.booking_mst_fk=b.booking_mst_pk";
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
                        strqry += "  JOB_CARD_TRN   impj,   customer_mst_tbl cust  where inv.consol_invoice_fk = " + invpk;
                        strqry += "  and con.consol_invoice_pk = inv.consol_invoice_fk and inv.job_card_fk = impj.JOB_CARD_TRN_PK ";
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
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
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
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //end
        #endregion

        #region "Fetch Records"
        public DataSet FetchAllJCsForFreightPayers(bool blnFetch, string strCustomer, string JCdate, string strJobNo, string strBLNo, short BizType, short Process, string VoyPK_OR_FlightNo)
        {

            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWf = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder strSelectionQuery = new System.Text.StringBuilder();

            //'SELECTION QUERY
            if (strCustomer.Length > 0)
            {
                strSelectionQuery.Append("AND FRTPYR.CUSTOMER_ID = '" + strCustomer + "' ");
            }
            else
            {
                //Added by purnanand for Not ISPostback Event 25-jan-08
                strSelectionQuery.Append("AND FRTPYR.CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM CUSTOMER_MST_TBL) ");
            }

            if (JCdate.Trim().Length > 0 && Process == 1)
            {
                strSelectionQuery.Append(" and TO_DATE(TO_CHAR(JOB.JOBCARD_DATE,DATEFORMAT),DATEFORMAT) = TO_DATE('" + JCdate + "' ,'" + dateFormat + "')");
                //" &  & "'")
            }

            if (strJobNo.Length > 0)
            {
                strSelectionQuery.Append(" and job.jobcard_ref_no = '" + strJobNo + "'");
            }

            if (strBLNo.Length > 0)
            {
                //'Export
                if (Process == 1)
                {
                    strSelectionQuery.Append(" AND HBL.HBL_REF_NO = '" + strBLNo + "'");
                }
                else
                {
                    strSelectionQuery.Append(" AND JOB.HBL_REF_NO = '" + strBLNo + "'");
                }
            }

            if (VoyPK_OR_FlightNo.Length > 0)
            {
                //'SEA
                if (BizType == 2)
                {
                    strSelectionQuery.Append(" AND JOB.VOYAGE_TRN_FK = " + VoyPK_OR_FlightNo + "");
                    //'AIR
                }
                else
                {
                    strSelectionQuery.Append(" AND JOB.FLIGHT_NO = '" + VoyPK_OR_FlightNo + "'");
                }
            }
            //********************************************
            //By Purnanand for fetching data even when blnfetch is false 25-Jan-08
            // If blnFetch = False Then
            //strSelectionQuery.Append(" and   1=2 ")
            //End If
            //*********************************************
            //By Amit Singh on 06-Sep-07
            //At Invoice Generation screen, on fetch button click, instead of showing 2 separate records for 
            //Freight and Other charges against the selected JC, it should be only a single record per JC.

            strQuery.Append("SELECT JOBPK,JOBCARD,JOBDATE,SHIPPER,CONSIGNEE,");
            strQuery.Append("POL,POD,SUM(FRAMT) FRAMT,CUST,CUST_PK,SEL FROM ");
            strQuery.Append("(SELECT JOB.JOB_CARD_TRN_PK JOBPK,");
            strQuery.Append("MAX(JOB.JOBCARD_REF_NO) JOBCARD,");
            strQuery.Append("MAX(JOB.JOBCARD_DATE) JOBDATE,");
            strQuery.Append("MAX(CUST.CUSTOMER_ID) SHIPPER,");
            strQuery.Append("MAX(CNSGN.CUSTOMER_ID) CONSIGNEE,");
            strQuery.Append("MAX(POL.PORT_ID) POL,");
            strQuery.Append("MAX(POD.PORT_ID) POD,");
            strQuery.Append("SUM( NVL(JFD.FREIGHT_AMT,0) * JFD.EXCHANGE_RATE) FRAMT,");
            strQuery.Append("MAX(FRTPYR.CUSTOMER_NAME) CUST,");
            strQuery.Append("MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK, ");
            strQuery.Append(" '' SEL");

            strQuery.Append("FROM JOB_TRN_SEA_EXP_FD JFD, ");
            strQuery.Append("JOB_CARD_TRN JOB,");
            //'Export
            if (Process == 1)
            {
                strQuery.Append("HBL_EXP_TBL HBL,");
                strQuery.Append("booking_mst_tbl BKG,");
            }
            strQuery.Append("PORT_MST_TBL POL,");
            strQuery.Append("PORT_MST_TBL POD,");
            strQuery.Append("CUSTOMER_MST_TBL CUST,");
            strQuery.Append("CUSTOMER_MST_TBL CNSGN,");
            strQuery.Append("USER_MST_TBL UMT,");
            strQuery.Append("CUSTOMER_MST_TBL FRTPYR");

            strQuery.Append("WHERE 1 = 1");
            strQuery.Append("AND JFD.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK");
            strQuery.Append("AND (NVL(JFD.FREIGHT_AMT,0) * JFD.EXCHANGE_RATE)>0");
            //strQuery.Append("AND JFD.INVOICE_SEA_TBL_FK IS NULL " & vbCrLf)
            //strQuery.Append("AND JFD.INV_AGENT_TRN_SEA_EXP_FK IS NULL " & vbCrLf)
            //strQuery.Append("AND JFD.CONSOL_INVOICE_TRN_FK IS NULL" & vbCrLf)
            strQuery.Append("AND JFD.FRTPAYER_CUST_MST_FK = FRTPYR.CUSTOMER_MST_PK(+)");
            strQuery.Append("AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK");
            strQuery.Append("AND JOB.CONSIGNEE_CUST_MST_FK = CNSGN.CUSTOMER_MST_PK(+)");
            //modifying by thiyagarajan on 13/10/08 to make consignee is optional while fetching
            strQuery.Append("AND UMT.USER_MST_PK = JOB.CREATED_BY_FK");

            //if the invoice generated for all freight elements in a Jobcard , no need to display it in grid
            //by thiyagarajan

            strQuery.Append("AND jfd.consol_invoice_trn_fk is null");

            //end

            // strQuery.Append("and FRTPYR.CUSTOMER_MST_PK is not null" & vbCrLf)
            //'Export
            if (Process == 1)
            {
                strQuery.Append("AND JOB.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK(+)");
                strQuery.Append("AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                strQuery.Append("AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
                strQuery.Append("AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK");
                strQuery.Append("AND UMT.DEFAULT_LOCATION_FK = " + LoggedIn_Loc_FK);
                strQuery.Append("AND JFD.FREIGHT_TYPE = 1");
                //'Prepaid elements
            }
            else
            {
                strQuery.Append("AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
                strQuery.Append("AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
                strQuery.Append("AND JFD.FREIGHT_TYPE = 2");
                //'collect elements
            }
            //'SELECTION QUERY
            strQuery.Append(strSelectionQuery);
            //'collect elements
            strQuery.Append(" GROUP BY JOB.JOB_CARD_TRN_PK,JFD.FRTPAYER_CUST_MST_FK ");

            strQuery.Append("");
            strQuery.Append("UNION");
            strQuery.Append("");

            strQuery.Append("SELECT JOB.JOB_CARD_TRN_PK JOBPK,");
            strQuery.Append("MAX(JOB.JOBCARD_REF_NO) JOBCARD,");
            strQuery.Append("MAX(JOB.JOBCARD_DATE) JOBDATE,");
            strQuery.Append("MAX(CUST.CUSTOMER_ID) SHIPPER,");
            strQuery.Append("MAX(CNSGN.CUSTOMER_ID) CONSIGNEE,");
            strQuery.Append("MAX(POL.PORT_ID) POL,");
            strQuery.Append("MAX(POD.PORT_ID) POD,");
            strQuery.Append("SUM( NVL(JOTH.AMOUNT,0) * JOTH.EXCHANGE_RATE) FRAMT,");
            strQuery.Append("MAX(FRTPYR.CUSTOMER_NAME) CUST,");
            strQuery.Append("MAX(FRTPYR.CUSTOMER_MST_PK) CUST_PK, ");
            strQuery.Append(" '' SEL");

            strQuery.Append("FROM JOB_TRN_SEA_EXP_OTH_CHRG JOTH, ");
            strQuery.Append("JOB_CARD_TRN JOB,");
            //'Export
            if (Process == 1)
            {
                strQuery.Append("HBL_EXP_TBL HBL,");
                strQuery.Append("booking_mst_tbl BKG,");
            }
            strQuery.Append("PORT_MST_TBL POL,");
            strQuery.Append("PORT_MST_TBL POD,");
            strQuery.Append("CUSTOMER_MST_TBL CUST,");
            strQuery.Append("CUSTOMER_MST_TBL CNSGN,");
            strQuery.Append("USER_MST_TBL UMT,");
            strQuery.Append("CUSTOMER_MST_TBL FRTPYR");

            strQuery.Append("WHERE 1 = 1");
            strQuery.Append("AND JOTH.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK");
            strQuery.Append("AND (NVL(JOTH.AMOUNT,0) * JOTH.EXCHANGE_RATE)>0");
            //strQuery.Append("AND JOTH.INV_CUST_TRN_SEA_EXP_FK IS NULL " & vbCrLf)
            //strQuery.Append("AND JOTH.INV_AGENT_TRN_SEA_EXP_FK IS NULL " & vbCrLf)
            //strQuery.Append("AND JOTH.CONSOL_INVOICE_TRN_FK IS NULL" & vbCrLf)
            strQuery.Append("AND JOTH.FRTPAYER_CUST_MST_FK = FRTPYR.CUSTOMER_MST_PK(+)");
            strQuery.Append("AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK");
            strQuery.Append("AND JOB.CONSIGNEE_CUST_MST_FK = CNSGN.CUSTOMER_MST_PK(+)");
            //modifying by thiyagarajan on 13/10/08 to make consignee is optional while fetching
            strQuery.Append("AND UMT.USER_MST_PK = JOB.CREATED_BY_FK");

            //if the invoice generated for all freight elements in a Jobcard , no need to display it in grid
            //by thiyagarajan

            strQuery.Append("AND joth.consol_invoice_trn_fk is null");

            //end

            // strQuery.Append("and FRTPYR.CUSTOMER_MST_PK is not null" & vbCrLf)
            //and FRTPYR.CUSTOMER_MST_PK is not null
            //'Export
            if (Process == 1)
            {
                strQuery.Append("AND JOB.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK(+)");
                strQuery.Append("AND JOB.BOOKING_SEA_FK = BKG.BOOKING_SEA_PK");
                strQuery.Append("AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
                strQuery.Append("AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK");
                strQuery.Append("AND UMT.DEFAULT_LOCATION_FK = " + LoggedIn_Loc_FK);
                strQuery.Append("AND JOTH.FREIGHT_TYPE = 1");
                //'Prepaid elements
            }
            else
            {
                strQuery.Append("AND JOB.PORT_MST_POL_FK = POL.PORT_MST_PK");
                strQuery.Append("AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
                strQuery.Append("AND JOTH.FREIGHT_TYPE = 2");
                //'collect elements
            }
            //'SELECTION QUERY
            strQuery.Append(strSelectionQuery);
            //'collect elements
            strQuery.Append(" GROUP BY JOB.JOB_CARD_TRN_PK,JOTH.FRTPAYER_CUST_MST_FK ) ");
            strQuery.Append(" GROUP BY JOBPK,JOBCARD,JOBDATE,SHIPPER,CONSIGNEE,POL,POD,SEL,CUST,CUST_PK  ");

            //Air Export
            if (BizType == 1 & Process == 1)
            {
                strQuery.Replace("SEA", "AIR");
                strQuery.Replace("INVOICE_AIR_TBL_FK", "INV_CUST_TRN_AIR_EXP_FK");
                strQuery.Replace("HBL", "HAWB");
                //Air Import
            }
            else if (BizType == 1 & Process == 2)
            {
                strQuery.Replace("SEA", "AIR");
                strQuery.Replace("INVOICE_AIR_TBL_FK", "INV_CUST_TRN_AIR_IMP_FK");
                strQuery.Replace("EXP", "IMP");
                strQuery.Replace("HBL", "HAWB");
                // Sea Import 
            }
            else if (BizType == 2 & Process == 2)
            {
                strQuery.Replace("EXP", "IMP");
            }
            try
            {
                return objWf.GetDataSet(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //created by thiyagarajan on 24/11/08 for location based currency task
        public DataSet FetchCurrId(string invpk)
        {
            System.Text.StringBuilder strsql = new System.Text.StringBuilder();
            WorkFlow objWf = new WorkFlow();
            try
            {
                strsql.Append(" select curr.currency_id CURRID ,curr.currency_mst_pk CURRPK  from consol_invoice_tbl con , currency_type_mst_tbl curr ");
                strsql.Append(" where con.currency_mst_fk=curr.currency_mst_pk");
                strsql.Append(" and con.consol_invoice_pk=" + invpk);
                return objWf.GetDataSet(strsql.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        // Fetch Functionality For fetching JobCards made for selected customer for consolidated invoice
        public DataSet FetchAll(bool blnFetch, string strCustomer, string JCdate, string strJobNo, string strBLNo, short BizType, short Process)
        {

            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWf = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            strQuery.Append("SELECT JOB.JOB_CARD_TRN_PK JOBPK,");
            strQuery.Append("       JOB.JOBCARD_REF_NO JOBCARD,");
            strQuery.Append("       JOB.JOBCARD_DATE JOBDATE,");
            strQuery.Append("       CUST.CUSTOMER_ID SHIPPER,");
            strQuery.Append("       CNSGN.CUSTOMER_ID CONSIGNEE,");
            strQuery.Append("       POL.PORT_ID POL,");
            strQuery.Append("       POD.PORT_ID POD,");
            strQuery.Append("       NVL((NVL((SELECT SUM(NVL(JOBFD.FREIGHT_AMT, 0)*JOBFD.EXCHANGE_RATE)");
            strQuery.Append("           FROM JOB_TRN_SEA_EXP_FD JOBFD");
            strQuery.Append("          WHERE JOBFD.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK");
            //'surya 20Nov06
            //'Export
            if (Process == 1)
            {
                strQuery.Append("            AND JOBFD.FREIGHT_TYPE = 1");
                //'Prepaid elements
                //'Import
            }
            else
            {
                strQuery.Append("            AND JOBFD.FREIGHT_TYPE = 2");
                //'Collect elements
            }
            strQuery.Append("            AND (JOBFD.INVOICE_SEA_TBL_FK IS NULL AND");
            strQuery.Append("                JOBFD.CONSOL_INVOICE_TRN_FK IS NULL AND");
            strQuery.Append("                JOBFD.INV_AGENT_TRN_SEA_EXP_FK IS NULL)),0) +");
            strQuery.Append("       NVL((SELECT SUM(NVL(JOBOTH.AMOUNT, 0)*JOBOTH.exchange_rate)");
            strQuery.Append("              FROM JOB_TRN_SEA_EXP_OTH_CHRG JOBOTH");
            strQuery.Append("             WHERE JOBOTH.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK");
            strQuery.Append("               AND (JOBOTH.INV_CUST_TRN_SEA_EXP_FK IS NULL AND");
            strQuery.Append("                   JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL AND");
            strQuery.Append("                   JOBOTH.INV_AGENT_TRN_SEA_EXP_FK IS NULL)),");
            strQuery.Append("            0)),0) FRAMT,");
            strQuery.Append("       0 SEL,");
            //'Export
            if (Process == 1)
            {
                strQuery.Append("       CUST.CUSTOMER_NAME CUST,");
                strQuery.Append("       CUST.CUSTOMER_MST_PK CUST_PK");
                //'Import
            }
            else
            {
                strQuery.Append("       CNSGN.CUSTOMER_NAME CUST,");
                strQuery.Append("       CNSGN.CUSTOMER_MST_PK CUST_PK");

            }
            strQuery.Append("  FROM JOB_CARD_TRN JOB,");
            //'Export
            if (Process == 1)
            {
                strQuery.Append("       booking_mst_tbl      BKG,");
            }
            strQuery.Append("       PORT_MST_TBL         POL,");
            strQuery.Append("       PORT_MST_TBL         POD,");
            strQuery.Append("       CUSTOMER_MST_TBL     CUST,");
            strQuery.Append("       CUSTOMER_MST_TBL     CNSGN,");
            strQuery.Append("       USER_MST_TBL        UMT");
            strQuery.Append(" WHERE 1 = 1");
            //'Export
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
            strQuery.Append("          WHERE JOBFD.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK");
            //'Export
            if (Process == 1)
            {
                strQuery.Append("            AND JOBFD.FREIGHT_TYPE = 1");
                //'Prepaid
                //'Import
            }
            else
            {
                strQuery.Append("            AND JOBFD.FREIGHT_TYPE = 2");
                //'Collect
            }
            strQuery.Append("            AND (JOBFD.INVOICE_SEA_TBL_FK IS NULL AND");
            strQuery.Append("                JOBFD.CONSOL_INVOICE_TRN_FK IS NULL AND");
            strQuery.Append("                JOBFD.INV_AGENT_TRN_SEA_EXP_FK IS NULL)),0) +");
            strQuery.Append("       NVL((SELECT SUM(NVL(JOBOTH.AMOUNT, 0)*JOBOTH.exchange_rate)");
            strQuery.Append("              FROM JOB_TRN_SEA_EXP_OTH_CHRG JOBOTH");
            strQuery.Append("             WHERE JOBOTH.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK");
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
            try
            {
                return objWf.GetDataSet(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        #region "Find CargoType"
        //finding cargotype
        //by thiyagarajan
        public int FetchCargoType(string strJobpk, string strCBJCpk, string strTPTpk, string strDemPKList, int Biztype, int process)
        {
            WorkFlow objwf = new WorkFlow();
            int cargotype = 0;
            string StrSql = null;
            StrSql = string.Empty;
            DataSet Ds = new DataSet();
            try
            {
                if (string.IsNullOrEmpty(strJobpk) & !string.IsNullOrEmpty(strCBJCpk))
                {
                    strJobpk = strCBJCpk;
                }
                else if (string.IsNullOrEmpty(strJobpk) & !string.IsNullOrEmpty(strTPTpk))
                {
                    strJobpk = strTPTpk;
                }
                if (!string.IsNullOrEmpty(strCBJCpk))
                {
                    StrSql += " select NVL(C.CARGO_TYPE,0) from CBJC_TBL C ";
                    StrSql += " where C.CBJC_PK in ( " + strCBJCpk + " ) ";
                }
                else if (!string.IsNullOrEmpty(strTPTpk))
                {
                    StrSql += " select NVL(T.CARGO_TYPE,0) from TRANSPORT_INST_SEA_TBL T ";
                    StrSql += " where T.TRANSPORT_INST_SEA_PK in ( " + strTPTpk + " ) ";
                }
                else if (!string.IsNullOrEmpty(strJobpk))
                {
                    StrSql += " select NVL(J.cargo_type,0) from JOB_CARD_TRN j ";
                    StrSql += " where j.JOB_CARD_TRN_PK in ( " + strJobpk + " ) ";
                }
                else if (!string.IsNullOrEmpty(strDemPKList))
                {
                    StrSql += " SELECT DCH.CARGO_TYPE FROM DEM_CALC_HDR DCH ";
                    StrSql += " WHERE DCH.DEM_CALC_HDR_PK IN (" + strDemPKList + ")";
                }
                Ds = objwf.GetDataSet(StrSql);
                if (Ds.Tables[0].Rows.Count > 0)
                {
                    return Convert.ToInt32( Ds.Tables[0].Rows[0][0]);
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return 0;
        }
        #endregion




        //Fetch for Consolidatable Grid
        public DataSet FetchConsolidatable(short BizType, short Process, string CustPk, bool Edit = false, int ExType = 1, string strJobPks = "", string strCBJCPks = "", string strTPTPks = "", string strDemPKList = "")
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWk = new WorkFlow();
            string frtType = Convert.ToString((Process == 1 ? "1" : "2"));
            if (string.IsNullOrEmpty(strJobPks))
            {
                strJobPks = "0";
            }
            if (string.IsNullOrEmpty(strCBJCPks))
            {
                strCBJCPks = "0";
            }
            if (string.IsNullOrEmpty(strTPTPks))
            {
                strTPTPks = "0";
            }
            if (string.IsNullOrEmpty(strDemPKList))
            {
                strDemPKList = "0";
            }
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT CFD.FREIGHT_ELEMENT_MST_FK,");
            sb.Append("                CT.CBJC_NO,");
            sb.Append("                CT.CBJC_DATE,");
            sb.Append("                0 COMMODITY_MST_PK,");
            sb.Append(" (SELECT ROWTOCOL('SELECT C.COMMODITY_NAME FROM commodity_mst_tbl c,CBJC_TRN_CONT CNT");
            sb.Append(" WHERE cnt.cbjc_fk in (' || ");
            sb.Append("  ct.cbjc_pk || ') ");
            sb.Append("  and C.COMMODITY_MST_PK = CNT.COMMODITY_MST_FK') ");
            sb.Append("  FROM DUAL) COMMODITY_NAME,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN CFD.CONTAINER_TYPE_MST_FK IS NOT NULL THEN");
            sb.Append("                   CON.CONTAINER_TYPE_MST_ID");
            sb.Append("                  ELSE");
            sb.Append("                   '1'");
            sb.Append("                END) UNIT,");
            sb.Append("                CFD.CBJC_TRN_FD_PK,");
            sb.Append("                CFD.CBJC_FK JOBFK,");
            sb.Append("                CFD.CURRENCY_MST_FK,");
            sb.Append("                FMT.FREIGHT_ELEMENT_NAME,");
            sb.Append("                DECODE(CFD.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') AS PC,");
            sb.Append("                CFD.RATEPERBASIS FREIGHT_AMT,");
            sb.Append("                CUMT.CURRENCY_ID,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN (CFD.CONSOL_INVOICE_TRN_FK IS NULL) THEN");
            sb.Append("                   NULL");
            sb.Append("                  WHEN CFD.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN");
            sb.Append("                   (SELECT DISTINCT ROUND((TRN.TOT_AMT / TRN.EXCHANGE_RATE),");
            sb.Append("                                          2)");
            sb.Append("                      FROM CONSOL_INVOICE_TRN_TBL TRN");
            sb.Append("                     WHERE TRN.FRT_OTH_ELEMENT_FK =");
            sb.Append("                           CFD.FREIGHT_ELEMENT_MST_FK");
            sb.Append("                       AND TRN.FRT_OTH_ELEMENT = 1");
            sb.Append("                       AND TRN.CONSOL_INVOICE_TRN_PK =");
            sb.Append("                           CFD.CONSOL_INVOICE_TRN_FK");
            sb.Append("                       AND TRN.JOB_CARD_FK = CT.CBJC_PK)");
            sb.Append("                ");
            sb.Append("                END) INV_AMT,");
            sb.Append("                (CASE");
            sb.Append("                  WHEN (CFD.CONSOL_INVOICE_TRN_FK IS NULL) THEN");
            sb.Append("                   'False'");
            sb.Append("                  ELSE");
            sb.Append("                   'True'");
            sb.Append("                END) CHK,");
            sb.Append("                PAR.FRT_BOF_FK,FMT.PREFERENCE");
            sb.Append("  FROM CBJC_TBL                CT,");
            sb.Append("       CBJC_TRN_FD             CFD,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CON,");
            sb.Append("       PARAMETERS_TBL          PAR");
            sb.Append(" WHERE CFD.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND CFD.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            sb.Append("   AND CT.CBJC_PK = CFD.CBJC_FK");
            sb.Append("   AND (CFD.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK OR");
            sb.Append("       CFD.CONTAINER_TYPE_MST_FK IS NULL)");
            sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
            sb.Append("   AND CFD.FREIGHT_TYPE = " + frtType);
            sb.Append("   AND CFD.CBJC_FK IN (" + strCBJCPks + ")");
            sb.Append("   AND CFD.FRTPAYER_CUST_MST_FK IN (" + CustPk + ")");

            System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);
            sb1.Append("SELECT DISTINCT TFD.FREIGHT_ELEMENT_MST_FK,");
            sb1.Append("                TIST.TRANS_INST_REF_NO,");
            sb1.Append("                TIST.TRANS_INST_DATE,");
            sb1.Append("                0 COMMODITY_MST_PK,");
            sb1.Append(" (SELECT ROWTOCOL('SELECT C.COMMODITY_NAME FROM commodity_mst_tbl c,TRANSPORT_TRN_CONT CNT");
            sb1.Append(" WHERE CNT.TRANSPORT_INST_FK in (' || ");
            sb1.Append("  TIST.TRANSPORT_INST_SEA_PK || ') ");
            sb1.Append("  and C.COMMODITY_MST_PK = CNT.COMMODITY_MST_FK') ");
            sb1.Append("  FROM DUAL) COMMODITY_NAME,");
            sb1.Append("                (CASE");
            sb1.Append("                  WHEN TFD.CONTAINER_TYPE_MST_FK IS NOT NULL THEN");
            sb1.Append("                   CON.CONTAINER_TYPE_MST_ID");
            sb1.Append("                  ELSE");
            sb1.Append("                   '1'");
            sb1.Append("                END) UNIT,");
            sb1.Append("                TFD.TRANSPORT_TRN_FD_PK,");
            sb1.Append("                TFD.TRANSPORT_INST_FK JOBFK,");
            sb1.Append("                TFD.CURRENCY_MST_FK,");
            sb1.Append("                FMT.FREIGHT_ELEMENT_NAME,");
            sb1.Append("                DECODE(TFD.FREIGHT_TYPE, 1, 'Prepaid', 2, 'Collect') AS PC,");
            sb1.Append("                TFD.RATEPERBASIS FREIGHT_AMT,");
            sb1.Append("                CUMT.CURRENCY_ID,");
            sb1.Append("                (CASE");
            sb1.Append("                  WHEN (TFD.CONSOL_INVOICE_TRN_FK IS NULL) THEN");
            sb1.Append("                   NULL");
            sb1.Append("                  WHEN TFD.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN");
            sb1.Append("                   (SELECT DISTINCT ROUND((TRN.TOT_AMT / TRN.EXCHANGE_RATE),");
            sb1.Append("                                          2)");
            sb1.Append("                      FROM CONSOL_INVOICE_TRN_TBL TRN");
            sb1.Append("                     WHERE TRN.FRT_OTH_ELEMENT_FK =");
            sb1.Append("                           TFD.FREIGHT_ELEMENT_MST_FK");
            sb1.Append("                       AND TRN.FRT_OTH_ELEMENT = 1");
            sb1.Append("                       AND TRN.CONSOL_INVOICE_TRN_PK =");
            sb1.Append("                           TFD.CONSOL_INVOICE_TRN_FK");
            sb1.Append("                       AND TRN.JOB_CARD_FK = TIST.TRANSPORT_INST_SEA_PK)");
            sb1.Append("                ");
            sb1.Append("                END) INV_AMT,");
            sb1.Append("                (CASE");
            sb1.Append("                  WHEN (TFD.CONSOL_INVOICE_TRN_FK IS NULL) THEN");
            sb1.Append("                   'False'");
            sb1.Append("                  ELSE");
            sb1.Append("                   'True'");
            sb1.Append("                END) CHK,");
            sb1.Append("                PAR.FRT_BOF_FK,FMT.PREFERENCE");
            sb1.Append("  FROM TRANSPORT_INST_SEA_TBL  TIST,");
            sb1.Append("       TRANSPORT_TRN_FD        TFD,");
            sb1.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
            sb1.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
            sb1.Append("       CONTAINER_TYPE_MST_TBL  CON,");
            sb1.Append("       PARAMETERS_TBL          PAR");
            sb1.Append(" WHERE TFD.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
            sb1.Append("   AND TFD.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            sb1.Append("   AND TIST.TRANSPORT_INST_SEA_PK = TFD.TRANSPORT_INST_FK");
            sb1.Append("   AND (TFD.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK OR");
            sb1.Append("       TFD.CONTAINER_TYPE_MST_FK IS NULL)");
            sb1.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
            sb1.Append("   AND TFD.FREIGHT_TYPE = " + frtType);
            sb1.Append("   AND TFD.TRANSPORT_INST_FK IN (" + strTPTPks + ")");
            sb1.Append("   AND TFD.FRTPAYER_CUST_MST_FK IN (" + CustPk + ")");

            strBuilder.Append(" select JOBCARD_REF_NO ,JOBCARD_DATE,COMMODITY_MST_PK,COMMODITY_NAME,unit,JOB_TRN_FD_PK,JOBFK,FREIGHT_ELEMENT_MST_FK,");
            strBuilder.Append(" CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PC,FREIGHT_AMT, CURRENCY_ID,INV_AMT, CHK,FRT_BOF_FK,PREFERENCE from ");
            strBuilder.Append(" ( SELECT distinct JOBFRT.FREIGHT_ELEMENT_MST_FK, ");
            strBuilder.Append(" JOB.JOBCARD_REF_NO,");
            strBuilder.Append(" JOB.JOBCARD_DATE,COMM.COMMODITY_MST_PK,COMM.COMMODITY_NAME,");
            strBuilder.Append(" ( case when jobfrt.container_type_mst_fk is not null then ");
            strBuilder.Append(" con.container_type_mst_id Else '1' end) UNIT,");

            strBuilder.Append(" JOBFRT.JOB_TRN_FD_PK,");
            strBuilder.Append(" JOBFRT.JOB_CARD_TRN_FK JOBFK,");
            strBuilder.Append(" JOBFRT.CURRENCY_MST_FK,");
            strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,");
            strBuilder.Append(" DECODE(JOBFRT.FREIGHT_TYPE,1,'Prepaid',2,'Collect') AS PC,");
            strBuilder.Append(" JOBFRT.Rateperbasis FREIGHT_AMT,");
            strBuilder.Append(" CUMT.CURRENCY_ID,");

            //Invoice Amount:
            strBuilder.Append(" (CASE ");
            strBuilder.Append(" WHEN (JOBFRT.INVOICE_TBL_FK IS NULL ");
            strBuilder.Append("   AND JOBFRT.INV_AGENT_TRN_FK IS NULL");
            strBuilder.Append("   AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ");
            strBuilder.Append(" ) THEN NULL ");
            //        If Consol. Invoice exists Fetch Invoice Amount from its Transaction 
            strBuilder.Append(" WHEN JOBFRT.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ");
            strBuilder.Append("   (SELECT distinct ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2)  FROM CONSOL_INVOICE_TRN_TBL TRN ");
            strBuilder.Append("   WHERE TRN.FRT_OTH_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
            strBuilder.Append("   AND TRN.FRT_OTH_ELEMENT=1  ");
            strBuilder.Append("   and TRN.CONSOL_INVOICE_TRN_PK=JOBFRT.CONSOL_INVOICE_TRN_FK ");
            strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_TRN_PK)");

            //       If Agent Invoice exists Fetch Invoice Amount from its Transaction   
            strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN");
            strBuilder.Append("    WHERE TRN.COST_FRT_ELEMENT_FK=JOBFRT.FREIGHT_ELEMENT_MST_FK ");
            strBuilder.Append("    AND TRN.COST_FRT_ELEMENT=2 ");
            strBuilder.Append("    AND TRN.INV_AGENT_TRN_PK=JOBFRT.INV_AGENT_TRN_FK) END) INV_AMT,");

            strBuilder.Append(" (CASE WHEN (JOBFRT.INVOICE_TBL_FK IS NULL ");
            strBuilder.Append("            AND JOBFRT.INV_AGENT_TRN_FK IS NULL");
            strBuilder.Append("            AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL)");
            strBuilder.Append("            THEN 'False' ELSE 'True' END) CHK,PAR.FRT_BOF_FK,FMT.PREFERENCE");

            strBuilder.Append(" FROM ");
            strBuilder.Append(" JOB_CARD_TRN JOB, ");
            strBuilder.Append(" JOB_TRN_FD JOBFRT,");
            strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,");
            strBuilder.Append(" JOB_TRN_CONT CNT,COMMODITY_MST_TBL COMM,");
            strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT,CONTAINER_TYPE_MST_TBL CON,PARAMETERS_TBL PAR");
            strBuilder.Append(" WHERE");
            strBuilder.Append(" JOBFRT.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK");
            strBuilder.Append(" AND (CNT.JOB_TRN_CONT_PK = JOBFRT.JOB_TRN_CONT_FK ");
            strBuilder.Append("     OR CNT.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK) ");
            strBuilder.Append(" AND COMM.COMMODITY_MST_PK=CNT.COMMODITY_MST_FK ");
            strBuilder.Append(" AND JOBFRT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK");
            strBuilder.Append(" AND JOB.JOB_CARD_TRN_PK=JOBFRT.JOB_CARD_TRN_FK AND ( JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK or JOBFRT.CONTAINER_TYPE_MST_FK is null ) AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+) ");
            if (Edit == false)
            {
                strBuilder.Append(" AND JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL ");
            }

            strBuilder.Append(" AND JOBFRT.FREIGHT_TYPE=1 ");

            strBuilder.Append(" AND JOBFRT.JOB_CARD_TRN_FK IN (" + strJobPks + ")");
            strBuilder.Append(" AND JOBFRT.frtpayer_cust_mst_fk in ( " + CustPk + ")");
            // added by jitendra on 22/05/07
            strBuilder.Append(" UNION");
            strBuilder.Append(" " + sb.ToString() + "");
            strBuilder.Append(" UNION");
            strBuilder.Append(" " + sb1.ToString() + "");
            strBuilder.Append(" )");
            strBuilder.Append(" UNION");
            //Other(Charges)
            strBuilder.Append("  SELECT ");
            strBuilder.Append(" JOB.JOBCARD_REF_NO, ");
            strBuilder.Append(" JOB.JOBCARD_DATE,0 COMMODITY_MST_PK,'Other Charges' COMMODITY_NAME,'Oth.Chrg' UNIT,");
            strBuilder.Append(" JOBOTH.JOB_TRN_OTH_PK,");
            strBuilder.Append(" JOBOTH.JOB_CARD_TRN_FK JOBFK,");
            strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK,");
            strBuilder.Append(" JOBOTH.CURRENCY_MST_FK,");
            strBuilder.Append(" FMT.FREIGHT_ELEMENT_NAME,");
            strBuilder.Append(" 'Prepaid' AS PC,");
            strBuilder.Append(" JOBOTH.AMOUNT,");
            strBuilder.Append(" CUMT.CURRENCY_ID,");


            strBuilder.Append(" (CASE ");
            //If No Invoice then Invoice Amount is null  
            strBuilder.Append(" WHEN (JOBOTH.INV_CUST_TRN_FK IS NULL ");
            strBuilder.Append("    AND JOBOTH.INV_AGENT_TRN_FK IS NULL ");
            strBuilder.Append("    AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL )THEN NULL ");

            //If Consolidated Invoice exists then : Inv Amount -> Select from Consol. Inv. Transaction table   
            strBuilder.Append(" WHEN JOBOTH.CONSOL_INVOICE_TRN_FK IS NOT NULL THEN ");
            strBuilder.Append("   (SELECT ROUND((TRN.TOT_AMT/TRN.EXCHANGE_RATE),2)  FROM CONSOL_INVOICE_TRN_TBL TRN ");
            strBuilder.Append("    WHERE TRN.FRT_OTH_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
            strBuilder.Append("    AND TRN.FRT_OTH_ELEMENT=2  ");
            strBuilder.Append("   and TRN.CONSOL_INVOICE_TRN_PK=JOBOTH.CONSOL_INVOICE_TRN_FK ");
            strBuilder.Append("    AND trn.job_card_fk=JOB.JOB_CARD_TRN_PK)");

            //If Customer Invoice exists then : Inv Amount -> Select from Customer. Inv. Transaction table   
            strBuilder.Append(" ELSE (SELECT TRN.TOT_AMT FROM INV_AGENT_TRN_TBL TRN");
            strBuilder.Append("      WHERE TRN.COST_FRT_ELEMENT_FK=JOBOTH.FREIGHT_ELEMENT_MST_FK ");
            strBuilder.Append("      AND TRN.COST_FRT_ELEMENT=3 ");
            strBuilder.Append("      AND TRN.INV_AGENT_TRN_PK=JOBOTH.INV_AGENT_TRN_FK) END) INV_AMT,");

            strBuilder.Append("(CASE WHEN (JOBOTH.INV_CUST_TRN_FK IS NULL ");
            strBuilder.Append(" AND JOBOTH.INV_AGENT_TRN_FK IS NULL AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL)");
            strBuilder.Append(" THEN 'False' ELSE 'True' END) CHK,");
            if (BizType == 2)
            {
                strBuilder.Append(" PAR.FRT_BOF_FK,FMT.PREFERENCE ");
            }
            else
            {
                strBuilder.Append(" PAR.FRT_AFC_FK,FMT.PREFERENCE ");
            }
            strBuilder.Append(" FROM ");
            strBuilder.Append(" JOB_CARD_TRN JOB,");
            strBuilder.Append(" JOB_TRN_OTH_CHRG JOBOTH,");
            strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL FMT,");
            strBuilder.Append(" CURRENCY_TYPE_MST_TBL CUMT, PARAMETERS_TBL PAR");
            strBuilder.Append(" WHERE");
            strBuilder.Append(" JOBOTH.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK");
            strBuilder.Append(" AND JOBOTH.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK");
            strBuilder.Append(" AND JOB.JOB_CARD_TRN_PK= JOBOTH.JOB_CARD_TRN_FK");
            // added by jitendra
            strBuilder.Append(" AND JOBOTH.Freight_Type=1");
            if (BizType == 2)
            {
                strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+) ");
            }
            else
            {
                strBuilder.Append(" AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_AFC_FK(+) ");
            }
            strBuilder.Append(" AND JOBOTH.JOB_CARD_TRN_FK IN(" + strJobPks + ")");
            strBuilder.Append("   AND JOBOTH.frtpayer_cust_mst_fk in ( " + CustPk + ")");

            strBuilder.Append(" UNION SELECT * FROM (SELECT DCH.DEM_CALC_ID JOBCARD_REF_NO,");
            strBuilder.Append("       DCH.DEM_CALC_DATE JOBCARD_DATE, 0 COMMODITY_MST_PK, 'Freight Charges' COMMODITY_NAME, ");
            strBuilder.Append("       'DET' UNIT,");
            strBuilder.Append("       TO_NUMBER(NULL) JOB_TRN_SEA_IMP_OTH_PK,");
            strBuilder.Append("       DCH.DEM_CALC_HDR_PK JOBFK,");
            strBuilder.Append("       FMT.FREIGHT_ELEMENT_MST_PK,");
            strBuilder.Append("       CUMT.CURRENCY_MST_PK,");
            strBuilder.Append("       FMT.FREIGHT_ELEMENT_NAME,");
            strBuilder.Append("       'Collect' PC,");
            strBuilder.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ");
            strBuilder.Append("          FROM DEM_CALC_DTL DCD");
            strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) FREIGHT_AMT,");
            strBuilder.Append("       CUMT.CURRENCY_ID,");

            strBuilder.Append("       CASE WHEN DCH.DET_INVOICE_TRN_FK IS NOT NULL THEN ");
            strBuilder.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ");
            strBuilder.Append("          FROM DEM_CALC_DTL DCD");
            strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) ");
            strBuilder.Append("       ELSE NULL END INV_AMT,");

            strBuilder.Append("       'True' CHK,");
            strBuilder.Append("       PAR.FRT_BOF_FK,FMT.PREFERENCE ");
            strBuilder.Append("  FROM DEM_CALC_HDR            DCH,");
            strBuilder.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
            strBuilder.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
            strBuilder.Append("       PARAMETERS_TBL          PAR");
            strBuilder.Append(" WHERE CUMT.CURRENCY_MST_PK = DCH.CURRENCY_MST_FK");
            strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_ID = 'DET'");
            strBuilder.Append("   AND FMT.ACTIVE_FLAG = 1");
            strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
            strBuilder.Append("   AND DCH.DEM_CALC_HDR_PK IN (" + strDemPKList + ") ");

            strBuilder.Append(" UNION SELECT DCH.DEM_CALC_ID JOBCARD_REF_NO,");
            strBuilder.Append("       DCH.DEM_CALC_DATE JOBCARD_DATE,0 COMMODITY_MST_PK, 'Freight Charges' COMMODITY_NAME,");
            strBuilder.Append("       'DEM' UNIT,");
            strBuilder.Append("       TO_NUMBER(NULL) JOB_TRN_SEA_IMP_OTH_PK,");
            strBuilder.Append("       DCH.DEM_CALC_HDR_PK JOBFK,");
            strBuilder.Append("       FMT.FREIGHT_ELEMENT_MST_PK,");
            strBuilder.Append("       CUMT.CURRENCY_MST_PK,");
            strBuilder.Append("       FMT.FREIGHT_ELEMENT_NAME,");
            strBuilder.Append("       'Collect' PC,");
            strBuilder.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ");
            strBuilder.Append("          FROM DEM_CALC_DTL DCD");
            strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) FREIGHT_AMT,");
            strBuilder.Append("       CUMT.CURRENCY_ID,");

            strBuilder.Append("       CASE WHEN DCH.DEM_INVOICE_TRN_FK IS NOT NULL THEN ");
            strBuilder.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ");
            strBuilder.Append("          FROM DEM_CALC_DTL DCD");
            strBuilder.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) ");
            strBuilder.Append("       ELSE NULL END INV_AMT,");

            strBuilder.Append("       'True' CHK,");
            strBuilder.Append("       PAR.FRT_BOF_FK,FMT.PREFERENCE ");
            strBuilder.Append("  FROM DEM_CALC_HDR            DCH,");
            strBuilder.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
            strBuilder.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
            strBuilder.Append("       PARAMETERS_TBL          PAR");
            strBuilder.Append(" WHERE CUMT.CURRENCY_MST_PK = DCH.CURRENCY_MST_FK");
            strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_ID = 'DEM'");
            strBuilder.Append("   AND FMT.ACTIVE_FLAG = 1");
            strBuilder.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
            strBuilder.Append("   AND DCH.DEM_CALC_HDR_PK IN (" + strDemPKList + ")) WHERE FREIGHT_AMT > 0 ");

            try
            {
                if (BizType == 2)
                {
                    //Return objWk.GetDataSet("SELECT Q.* FROM ( " & strBuilder.ToString & " )Q ORDER BY FRT_BOF_FK,UNIT DESC")                    
                    return objWk.GetDataSet("SELECT JOBCARD_REF_NO ,JOBCARD_DATE,COMMODITY_MST_PK,COMMODITY_NAME,unit,JOB_TRN_FD_PK,JOBFK,FREIGHT_ELEMENT_MST_FK,CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PC,FREIGHT_AMT, CURRENCY_ID,INV_AMT, CHK,FRT_BOF_FK FROM ( " + strBuilder.ToString() + " )Q ORDER BY UNIT,COMMODITY_MST_PK,PREFERENCE");
                }
                else
                {
                    return objWk.GetDataSet("SELECT JOBCARD_REF_NO ,JOBCARD_DATE,COMMODITY_MST_PK,COMMODITY_NAME,unit,JOB_TRN_FD_PK,JOBFK,FREIGHT_ELEMENT_MST_FK,CURRENCY_MST_FK, FREIGHT_ELEMENT_NAME, PC,FREIGHT_AMT, CURRENCY_ID,INV_AMT, CHK,FRT_BOF_FK FROM ( " + strBuilder.ToString() + " )Q ORDER BY UNIT,COMMODITY_MST_PK,PREFERENCE");
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //Added By Prakash Chandra on 26/06/2008
        public string FetchCreditLimit(string CustPk)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            //Dim credit As Integer
            WorkFlow objWF = new WorkFlow();
            strQuery.Append(" select distinct nvl(cmt.credit_limit,0) from ");
            strQuery.Append("  customer_mst_tbl cmt,");
            strQuery.Append(" JOB_TRN_FD JOBFRT");
            strQuery.Append(" where  cmt.customer_mst_pk=JOBFRT.FRTPAYER_CUST_MST_FK ");
            strQuery.Append(" and JOBFRT.frtpayer_cust_mst_fk in ( " + CustPk + ")");
            try
            {
                return objWF.ExecuteScaler(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchInvoiceData(string strJobPks, string strCBJCPks, string strTPTPks, string strDemPKList, int intInvPk, int nBaseCurrPK, short BizType, short Process, int UserPk, string CustPk,
        string CreditLimit, string amount, int ExType = 1)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWK = new WorkFlow();
            DataSet DS = null;
            string strsql = null;
            string vatcode = null;
            //adding by thiyagarajan on 3/1/09 :VEK Gap analysis
            Int32 rowcunt = 0;
            Int32 Contpk = 0;
            //end
            string frtType = Convert.ToString((Process == 1 ? "1" : "2"));
            if (string.IsNullOrEmpty(strJobPks))
            {
                strJobPks = "0";
            }
            if (string.IsNullOrEmpty(strCBJCPks))
            {
                strCBJCPks = "0";
            }
            if (string.IsNullOrEmpty(strTPTPks))
            {
                strTPTPks = "0";
            }
            if (string.IsNullOrEmpty(strDemPKList))
            {
                strDemPKList = "0";
            }

            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT DISTINCT CFD.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,");
                sb.Append("                'FREIGHT' AS TYPE,");
                sb.Append("                CT.CBJC_NO,");
                sb.Append("                0 COMMODITY_MST_PK,");
                sb.Append(" (SELECT ROWTOCOL('SELECT C.COMMODITY_NAME FROM commodity_mst_tbl c,CBJC_TRN_CONT CNT");
                sb.Append(" WHERE cnt.cbjc_fk in (' || ");
                sb.Append("  ct.cbjc_pk || ') ");
                sb.Append("  and C.COMMODITY_MST_PK = CNT.COMMODITY_MST_FK') ");
                sb.Append("  FROM DUAL) COMMODITY_NAME,");
                sb.Append("                (CASE");
                sb.Append("                  WHEN CFD.CONTAINER_TYPE_MST_FK IS NOT NULL THEN");
                sb.Append("                   CON.CONTAINER_TYPE_MST_ID");
                sb.Append("                  ELSE");
                sb.Append("                   '1'");
                sb.Append("                END) UNIT,");
                sb.Append("                CFD.CBJC_TRN_FD_PK AS PK,");
                sb.Append("                CFD.CBJC_FK AS JOBCARD_FK,");
                sb.Append("                1 FREIGHT_OR_OTH,");
                sb.Append("                CFD.CURRENCY_MST_FK,");
                sb.Append("                FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,");
                sb.Append("                '' AS ELEMENT,");
                sb.Append("                CUMT.CURRENCY_ID,");
                sb.Append("                '' AS CURR,");
                sb.Append("                CFD.FREIGHT_AMT AS AMOUNT,");
                sb.Append("                GET_EX_RATE(CFD.CURRENCY_MST_FK, " + nBaseCurrPK + ", SYSDATE) EXCHANGE_RATE,");
                sb.Append("                CFD.FREIGHT_AMT *");
                sb.Append("                GET_EX_RATE(CFD.CURRENCY_MST_FK, " + nBaseCurrPK + ", SYSDATE) AS INV_AMOUNT,");
                sb.Append("                (SELECT FETCH_VAT((SELECT BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) FROM DUAL),");
                sb.Append("                                  CFD.FRTPAYER_CUST_MST_FK,");
                sb.Append("                                  " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb.Append("                                  CFD.FREIGHT_TYPE,");
                sb.Append("                                  CFD.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                                  1)");
                sb.Append("                   FROM DUAL) VAT_CODE,");
                sb.Append("                (SELECT FETCH_VAT((SELECT BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) FROM DUAL),");
                sb.Append("                                  CFD.FRTPAYER_CUST_MST_FK,");
                sb.Append("                                  " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb.Append("                                  CFD.FREIGHT_TYPE,");
                sb.Append("                                  CFD.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                                  2)");
                sb.Append("                   FROM DUAL) VAT_PERCENT,");
                sb.Append("                ((SELECT FETCH_VAT((SELECT BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) FROM DUAL),");
                sb.Append("                                   CFD.FRTPAYER_CUST_MST_FK,");
                sb.Append("                                   " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb.Append("                                   CFD.FREIGHT_TYPE,");
                sb.Append("                                   CFD.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                                   2)");
                sb.Append("                    FROM DUAL) *");
                sb.Append("                (CFD.FREIGHT_AMT * CFD.EXCHANGE_RATE) / 100) TAX_AMOUNT,");
                sb.Append("                (((SELECT FETCH_VAT((SELECT BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) FROM DUAL),");
                sb.Append("                                    CFD.FRTPAYER_CUST_MST_FK,");
                sb.Append("                                    " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb.Append("                                    CFD.FREIGHT_TYPE,");
                sb.Append("                                    CFD.FREIGHT_ELEMENT_MST_FK,");
                sb.Append("                                    2)");
                sb.Append("                     FROM DUAL) *");
                sb.Append("                (CFD.FREIGHT_AMT * CFD.EXCHANGE_RATE) / 100) +");
                sb.Append("                CFD.FREIGHT_AMT * CFD.EXCHANGE_RATE) TOTAL_AMOUNT,");
                sb.Append("                '' AS REMARKS,");
                sb.Append("                'NEW' AS \"MODE1\",");
                sb.Append("                'FALSE' AS CHK,");
                sb.Append("                PAR.FRT_BOF_FK, '2' JOB_TYPE,FMT.PREFERENCE");
                sb.Append("  FROM CBJC_TRN_FD             CFD,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                sb.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                sb.Append("       CORPORATE_MST_TBL       CORP,");
                sb.Append("       CBJC_TBL    CT,");
                sb.Append("       CONTAINER_TYPE_MST_TBL  CON,");
                sb.Append("       PARAMETERS_TBL          PAR");
                sb.Append(" WHERE CFD.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("   AND CFD.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb.Append("   AND CT.CBJC_PK = CFD.CBJC_FK");
                sb.Append("   AND (CFD.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK OR");
                sb.Append("       CFD.CONTAINER_TYPE_MST_FK IS NULL)");
                sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
                sb.Append("   AND CFD.CBJC_FK IN (" + strCBJCPks + ")");
                sb.Append("   AND CFD.CONSOL_INVOICE_TRN_FK IS NULL");
                sb.Append("   AND CFD.FREIGHT_TYPE = " + frtType);
                sb.Append("   AND CFD.FREIGHT_AMT > 0");
                sb.Append("   AND CFD.FRTPAYER_CUST_MST_FK IN ( " + CustPk + ")");

                System.Text.StringBuilder sb1 = new System.Text.StringBuilder(5000);
                sb1.Append("SELECT DISTINCT TFD.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,");
                sb1.Append("                'FREIGHT' AS TYPE,");
                sb1.Append("                TIST.TRANS_INST_REF_NO,");
                sb1.Append("                0 COMMODITY_MST_PK,");
                sb1.Append(" (SELECT ROWTOCOL('SELECT C.COMMODITY_NAME FROM commodity_mst_tbl c,TRANSPORT_TRN_CONT CNT");
                sb1.Append(" WHERE CNT.TRANSPORT_INST_FK in (' || ");
                sb1.Append("  TIST.TRANSPORT_INST_SEA_PK || ') ");
                sb1.Append("  and C.COMMODITY_MST_PK = CNT.COMMODITY_MST_FK') ");
                sb1.Append("  FROM DUAL) COMMODITY_NAME,");
                sb1.Append("                (CASE");
                sb1.Append("                  WHEN TFD.CONTAINER_TYPE_MST_FK IS NOT NULL THEN");
                sb1.Append("                   CON.CONTAINER_TYPE_MST_ID");
                sb1.Append("                  ELSE");
                sb1.Append("                   '1'");
                sb1.Append("                END) UNIT,");
                sb1.Append("                TFD.TRANSPORT_TRN_FD_PK AS PK,");
                sb1.Append("                TFD.TRANSPORT_INST_FK AS JOBCARD_FK,");
                sb1.Append("                1 FREIGHT_OR_OTH,");
                sb1.Append("                TFD.CURRENCY_MST_FK,");
                sb1.Append("                FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,");
                sb1.Append("                '' AS ELEMENT,");
                sb1.Append("                CUMT.CURRENCY_ID,");
                sb1.Append("                '' AS CURR,");
                sb1.Append("                TFD.FREIGHT_AMT AS AMOUNT,");
                sb1.Append("                GET_EX_RATE(TFD.CURRENCY_MST_FK, " + nBaseCurrPK + ", SYSDATE) EXCHANGE_RATE,");
                sb1.Append("                TFD.FREIGHT_AMT *");
                sb1.Append("                GET_EX_RATE(TFD.CURRENCY_MST_FK, " + nBaseCurrPK + ", SYSDATE) AS INV_AMOUNT,");
                sb1.Append("                (SELECT FETCH_VAT((SELECT BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) FROM DUAL),");
                sb1.Append("                                  TFD.FRTPAYER_CUST_MST_FK,");
                sb1.Append("                                  " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb1.Append("                                  TFD.FREIGHT_TYPE,");
                sb1.Append("                                  TFD.FREIGHT_ELEMENT_MST_FK,");
                sb1.Append("                                  1)");
                sb1.Append("                   FROM DUAL) VAT_CODE,");
                sb1.Append("                (SELECT FETCH_VAT((SELECT BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) FROM DUAL),");
                sb1.Append("                                  TFD.FRTPAYER_CUST_MST_FK,");
                sb1.Append("                                  " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb1.Append("                                  TFD.FREIGHT_TYPE,");
                sb1.Append("                                  TFD.FREIGHT_ELEMENT_MST_FK,");
                sb1.Append("                                  2)");
                sb1.Append("                   FROM DUAL) VAT_PERCENT,");
                sb1.Append("                ((SELECT FETCH_VAT((SELECT BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) FROM DUAL),");
                sb1.Append("                                   TFD.FRTPAYER_CUST_MST_FK,");
                sb1.Append("                                   " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb1.Append("                                   TFD.FREIGHT_TYPE,");
                sb1.Append("                                   TFD.FREIGHT_ELEMENT_MST_FK,");
                sb1.Append("                                   2)");
                sb1.Append("                    FROM DUAL) * (TFD.FREIGHT_AMT * TFD.EXCHANGE_RATE) / 100) TAX_AMOUNT,");
                sb1.Append("                (((SELECT FETCH_VAT((SELECT BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) FROM DUAL),");
                sb1.Append("                                    TFD.FRTPAYER_CUST_MST_FK,");
                sb1.Append("                                    " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",");
                sb1.Append("                                    TFD.FREIGHT_TYPE,");
                sb1.Append("                                    TFD.FREIGHT_ELEMENT_MST_FK,");
                sb1.Append("                                    2)");
                sb1.Append("                     FROM DUAL) * (TFD.FREIGHT_AMT * TFD.EXCHANGE_RATE) / 100) +");
                sb1.Append("                TFD.FREIGHT_AMT * TFD.EXCHANGE_RATE) TOTAL_AMOUNT,");
                sb1.Append("                '' AS REMARKS,");
                sb1.Append("                'NEW' AS \"MODE1\",");
                sb1.Append("                'FALSE' AS CHK,");
                sb1.Append("                PAR.FRT_BOF_FK, '3' JOB_TYPE,FMT.PREFERENCE");
                sb1.Append("  FROM TRANSPORT_TRN_FD        TFD,");
                sb1.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                sb1.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                sb1.Append("       CORPORATE_MST_TBL       CORP,");
                sb1.Append("       TRANSPORT_INST_SEA_TBL  TIST,");
                sb1.Append("       CONTAINER_TYPE_MST_TBL  CON,");
                sb1.Append("       PARAMETERS_TBL          PAR");
                sb1.Append(" WHERE TFD.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb1.Append("   AND TFD.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb1.Append("   AND TIST.TRANSPORT_INST_SEA_PK = TFD.TRANSPORT_INST_FK");
                sb1.Append("   AND (TFD.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK OR");
                sb1.Append("       TFD.CONTAINER_TYPE_MST_FK IS NULL)");
                sb1.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
                sb1.Append("   AND TFD.TRANSPORT_INST_FK IN (" + strTPTPks + ")");
                sb1.Append("   AND TFD.CONSOL_INVOICE_TRN_FK IS NULL");
                sb1.Append("   AND TFD.FREIGHT_TYPE = " + frtType);
                sb1.Append("   AND TFD.FREIGHT_AMT > 0");
                sb1.Append("   AND TFD.FRTPAYER_CUST_MST_FK IN (" + CustPk + ")");

                System.Text.StringBuilder sb2 = new System.Text.StringBuilder(5000);
                sb2.Append("SELECT DISTINCT TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK,");
                sb2.Append("                DECODE(TRN.FRT_OTH_ELEMENT_FK,");
                sb2.Append("                       1,");
                sb2.Append("                       'COST',");
                sb2.Append("                       2,");
                sb2.Append("                       'FREIGHT',");
                sb2.Append("                       3,");
                sb2.Append("                       'OTHER') AS TYPE,");
                sb2.Append("                CT.CBJC_NO,");
                sb2.Append("                0 COMMODITY_MST_PK,");
                sb2.Append(" (SELECT ROWTOCOL('SELECT C.COMMODITY_NAME FROM commodity_mst_tbl c,CBJC_TRN_CONT CNT");
                sb2.Append(" WHERE cnt.cbjc_fk in (' || ");
                sb2.Append("  ct.cbjc_pk || ') ");
                sb2.Append("  and C.COMMODITY_MST_PK = CNT.COMMODITY_MST_FK') ");
                sb2.Append("  FROM DUAL) COMMODITY_NAME,");
                sb2.Append("                (CASE");
                sb2.Append("                  WHEN CFD.CONTAINER_TYPE_MST_FK IS NOT NULL THEN");
                sb2.Append("                   CON.CONTAINER_TYPE_MST_ID");
                sb2.Append("                  ELSE");
                sb2.Append("                   '1'");
                sb2.Append("                END) UNIT,");
                sb2.Append("                CFD.CBJC_TRN_FD_PK AS PK,");
                sb2.Append("                TRN.JOB_CARD_FK AS JOBCARD_FK,");
                sb2.Append("                TRN.FRT_OTH_ELEMENT FREIGHT_OR_OTH,");
                sb2.Append("                TRN.CURRENCY_MST_FK,");
                sb2.Append("                TRN.FRT_DESC AS ELEMENT_NAME,");
                sb2.Append("                '' AS ELEMENT,");
                sb2.Append("                CUMT.CURRENCY_ID,");
                sb2.Append("                '' AS CURR,");
                sb2.Append("                TRN.ELEMENT_AMT AS AMOUNT,");
                sb2.Append("                ROUND((CASE TRN.ELEMENT_AMT");
                sb2.Append("                        WHEN 0 THEN");
                sb2.Append("                         1");
                sb2.Append("                        ELSE");
                sb2.Append("                         TRN.AMT_IN_INV_CURR / TRN.ELEMENT_AMT");
                sb2.Append("                      END),");
                sb2.Append("                      6) AS EXCHANGE_RATE,");
                sb2.Append("                TRN.AMT_IN_INV_CURR AS INV_AMOUNT,");
                sb2.Append("                TRN.VAT_CODE AS VAT_CODE,");
                sb2.Append("                TRN.TAX_PCNT AS VAT_PERCENT,");
                sb2.Append("                TRN.TAX_AMT AS TAX_AMOUNT,");
                sb2.Append("                TRN.TOT_AMT AS TOTAL_AMOUNT,");
                sb2.Append("                TRN.REMARKS,");
                sb2.Append("                'EDIT' AS \"MODE1\",");
                sb2.Append("                'TRUE' AS CHK,");
                sb2.Append("                PAR.FRT_BOF_FK,");
                sb2.Append("                '2' JOB_TYPE,FMT.PREFERENCE");
                sb2.Append("  FROM CONSOL_INVOICE_TRN_TBL  TRN,");
                sb2.Append("       CONSOL_INVOICE_TBL      HDR,");
                sb2.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                sb2.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                sb2.Append("       CBJC_TRN_FD             CFD,");
                sb2.Append("       CONTAINER_TYPE_MST_TBL  CON,");
                sb2.Append("       PARAMETERS_TBL          PAR,");
                sb2.Append("       CBJC_TBL                CT");
                sb2.Append(" WHERE TRN.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK");
                sb2.Append("   AND TRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb2.Append("   AND CT.CBJC_PK = TRN.JOB_CARD_FK");
                sb2.Append("   AND CFD.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb2.Append("   AND CFD.CONSOL_INVOICE_TRN_FK = TRN.CONSOL_INVOICE_TRN_PK");
                sb2.Append("   AND TRN.JOB_CARD_FK = CFD.CBJC_FK");
                sb2.Append("   AND (CFD.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK OR");
                sb2.Append("       CFD.CONTAINER_TYPE_MST_FK IS NULL)");
                sb2.Append("   AND CFD.FREIGHT_ELEMENT_MST_FK = TRN.FRT_OTH_ELEMENT_FK");
                sb2.Append("   AND TRN.FRT_OTH_ELEMENT_FK = PAR.FRT_BOF_FK(+)");
                sb2.Append("   AND HDR.CONSOL_INVOICE_PK = " + intInvPk);

                System.Text.StringBuilder sb3 = new System.Text.StringBuilder(5000);
                sb3.Append("SELECT DISTINCT TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK,");
                sb3.Append("                DECODE(TRN.FRT_OTH_ELEMENT_FK,");
                sb3.Append("                       1,");
                sb3.Append("                       'COST',");
                sb3.Append("                       2,");
                sb3.Append("                       'FREIGHT',");
                sb3.Append("                       3,");
                sb3.Append("                       'OTHER') AS TYPE,");
                sb3.Append("                TIST.TRANS_INST_REF_NO,");
                sb3.Append("                0 COMMODITY_MST_PK,");
                sb3.Append(" (SELECT ROWTOCOL('SELECT C.COMMODITY_NAME FROM commodity_mst_tbl c,TRANSPORT_TRN_CONT CNT");
                sb3.Append(" WHERE CNT.TRANSPORT_INST_FK in (' || ");
                sb3.Append("  TIST.TRANSPORT_INST_SEA_PK || ') ");
                sb3.Append("  and C.COMMODITY_MST_PK = CNT.COMMODITY_MST_FK') ");
                sb3.Append("  FROM DUAL) COMMODITY_NAME,");
                sb3.Append("                (CASE");
                sb3.Append("                  WHEN TFD.CONTAINER_TYPE_MST_FK IS NOT NULL THEN");
                sb3.Append("                   CON.CONTAINER_TYPE_MST_ID");
                sb3.Append("                  ELSE");
                sb3.Append("                   '1'");
                sb3.Append("                END) UNIT,");
                sb3.Append("                TFD.TRANSPORT_TRN_FD_PK AS PK,");
                sb3.Append("                TRN.JOB_CARD_FK AS JOBCARD_FK,");
                sb3.Append("                TRN.FRT_OTH_ELEMENT FREIGHT_OR_OTH,");
                sb3.Append("                TRN.CURRENCY_MST_FK,");
                sb3.Append("                TRN.FRT_DESC AS ELEMENT_NAME,");
                sb3.Append("                '' AS ELEMENT,");
                sb3.Append("                CUMT.CURRENCY_ID,");
                sb3.Append("                '' AS CURR,");
                sb3.Append("                TRN.ELEMENT_AMT AS AMOUNT,");
                sb3.Append("                ROUND((CASE TRN.ELEMENT_AMT");
                sb3.Append("                        WHEN 0 THEN");
                sb3.Append("                         1");
                sb3.Append("                        ELSE");
                sb3.Append("                         TRN.AMT_IN_INV_CURR / TRN.ELEMENT_AMT");
                sb3.Append("                      END),");
                sb3.Append("                      6) AS EXCHANGE_RATE,");
                sb3.Append("                TRN.AMT_IN_INV_CURR AS INV_AMOUNT,");
                sb3.Append("                TRN.VAT_CODE AS VAT_CODE,");
                sb3.Append("                TRN.TAX_PCNT AS VAT_PERCENT,");
                sb3.Append("                TRN.TAX_AMT AS TAX_AMOUNT,");
                sb3.Append("                TRN.TOT_AMT AS TOTAL_AMOUNT,");
                sb3.Append("                TRN.REMARKS,");
                sb3.Append("                'EDIT' AS \"MODE1\",");
                sb3.Append("                'TRUE' AS CHK,");
                sb3.Append("                PAR.FRT_BOF_FK,");
                sb3.Append("                '3' JOB_TYPE,FMT.PREFERENCE");
                sb3.Append("  FROM CONSOL_INVOICE_TRN_TBL  TRN,");
                sb3.Append("       CONSOL_INVOICE_TBL      HDR,");
                sb3.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                sb3.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                sb3.Append("       TRANSPORT_TRN_FD        TFD,");
                sb3.Append("       CONTAINER_TYPE_MST_TBL  CON,");
                sb3.Append("       PARAMETERS_TBL          PAR,");
                sb3.Append("       TRANSPORT_INST_SEA_TBL  TIST");
                sb3.Append(" WHERE TRN.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK");
                sb3.Append("   AND TRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                sb3.Append("   AND TIST.TRANSPORT_INST_SEA_PK = TRN.JOB_CARD_FK");
                sb3.Append("   AND TFD.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                sb3.Append("   AND TFD.CONSOL_INVOICE_TRN_FK = TRN.CONSOL_INVOICE_TRN_PK");
                sb3.Append("   AND TRN.JOB_CARD_FK = TFD.TRANSPORT_INST_FK");
                sb3.Append("   AND (TFD.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK OR");
                sb3.Append("       TFD.CONTAINER_TYPE_MST_FK IS NULL)");
                sb3.Append("   AND TFD.FREIGHT_ELEMENT_MST_FK = TRN.FRT_OTH_ELEMENT_FK");
                sb3.Append("   AND TRN.FRT_OTH_ELEMENT_FK = PAR.FRT_BOF_FK(+)");
                sb3.Append("   AND HDR.CONSOL_INVOICE_PK = " + intInvPk);

                //adding by thiyagarajan on 3/1/09 as "no.of palattes" col.introduced in Detention form :VEK Gap analysis
                if (intInvPk > 0 & Convert.ToInt32(strDemPKList) > 0)
                {
                    strQuery.Append("SELECT 'FREIGHT' AS TYPE, J.DEM_CALC_ID Jobcard_ref_no, 0 COMMODITY_MST_PK, 'Freight Charges' COMMODITY_NAME, '' unit , TRN.CONSOL_INVOICE_TRN_PK PK,J.DEM_CALC_HDR_PK jobcard_fk, ");
                    strQuery.Append(" TRN.FRT_OTH_ELEMENT freight_or_oth ,TRN.FRT_OTH_ELEMENT_FK element_fk,TRN.CURRENCY_MST_FK currency_mst_fk,TRN.FRT_DESC element_name, ");
                    strQuery.Append(" '' AS ELEMENT,cur.currency_id,  '' AS CURR, TRN.ELEMENT_AMT AS AMOUNT, ROUND((CASE TRN.ELEMENT_AMT ");
                    strQuery.Append(" WHEN 0 THEN 1 ELSE  TRN.AMT_IN_INV_CURR /TRN.ELEMENT_AMT END),6) AS EXCHANGE_RATE,       TRN.AMT_IN_INV_CURR AS INV_AMOUNT, ");
                    strQuery.Append(" TRN.VAT_CODE AS VAT_CODE,TRN.TAX_PCNT AS VAT_PERCENT,TRN.TAX_AMT AS TAX_AMOUNT,TRN.TOT_AMT AS TOTAL_AMOUNT,TRN.REMARKS, ");
                    strQuery.Append(" 'EDIT' AS \"MODE\",'TRUE' AS CHK, ");
                    if (BizType == 1)
                    {
                        strQuery.Append(" '' FRT_AFC_FK, ");
                    }
                    else
                    {
                        strQuery.Append(" '' FRT_BOF_FK, ");
                    }
                    strQuery.Append(" TRN.JOB_TYPE FROM CONSOL_INVOICE_TBL MAS,CONSOL_INVOICE_TRN_TBL TRN,DEM_CALC_HDR J,currency_type_mst_tbl cur ");
                    strQuery.Append(" ,freight_element_mst_tbl femt");
                    strQuery.Append(" where cur.currency_mst_pk=trn.currency_mst_fk and MAS.CONSOL_INVOICE_PK=" + intInvPk + " AND TRN.CONSOL_INVOICE_FK=MAS.CONSOL_INVOICE_PK AND trn.job_card_fk=j.DEM_CALC_HDR_PK ");
                    strQuery.Append("   AND femt.freight_element_name=trn.frt_desc  ORDER BY unit,femt.preference ");

                    return objWK.GetDataSet(strQuery.ToString());
                }
                else if (BizType == 2 & Process == 2 & intInvPk > 0)
                {
                    rowcunt = FetchDetFrt(intInvPk);
                    if (rowcunt > 0)
                    {
                        Contpk = FetchDetContPk(Convert.ToInt32(strJobPks), intInvPk);
                        //modifying by thiyagarajan on 7/2/09 as "no.of palattes" col.introduced in Detention form :VEK Gap analysis
                        if (Contpk == 0)
                        {
                            strQuery.Append("SELECT 0 PK, J.JOBCARD_REF_NO Jobcard_ref_no, 0 COMMODITY_MST_PK, 'Freight Charges' COMMODITY_NAME, '' unit , TRN.CONSOL_INVOICE_TRN_PK PK,J.JOB_CARD_TRN_PK jobcard_fk, ");
                            strQuery.Append(" TRN.FRT_OTH_ELEMENT freight_or_oth ,TRN.FRT_OTH_ELEMENT_FK element_fk,TRN.CURRENCY_MST_FK currency_mst_fk,TRN.FRT_DESC element_name, ");
                            strQuery.Append(" '' AS ELEMENT,cur.currency_id,  '' AS CURR, TRN.ELEMENT_AMT AS AMOUNT, ROUND((CASE TRN.ELEMENT_AMT ");
                            strQuery.Append(" WHEN 0 THEN 1 ELSE  TRN.AMT_IN_INV_CURR /TRN.ELEMENT_AMT END),6) AS EXCHANGE_RATE,       TRN.AMT_IN_INV_CURR AS INV_AMOUNT, ");
                            strQuery.Append(" TRN.VAT_CODE AS VAT_CODE,TRN.TAX_PCNT AS VAT_PERCENT,TRN.TAX_AMT AS TAX_AMOUNT,TRN.TOT_AMT AS TOTAL_AMOUNT,TRN.REMARKS, ");
                            strQuery.Append(" 'EDIT' AS \"MODE\",'TRUE' AS CHK, ");
                            if (BizType == 1)
                            {
                                strQuery.Append(" '' FRT_AFC_FK, ");
                            }
                            else
                            {
                                strQuery.Append(" '' FRT_BOF_FK, ");
                            }
                            strQuery.Append(" TRN.JOB_TYPE FROM CONSOL_INVOICE_TBL MAS,CONSOL_INVOICE_TRN_TBL TRN,JOB_CARD_TRN J,currency_type_mst_tbl cur ");
                            strQuery.Append(" ,freight_element_mst_tbl femt");
                            strQuery.Append(" where cur.currency_mst_pk=trn.currency_mst_fk and MAS.CONSOL_INVOICE_PK=" + intInvPk + " AND TRN.CONSOL_INVOICE_FK=MAS.CONSOL_INVOICE_PK AND trn.job_card_fk=j.JOB_CARD_TRN_PK ");
                            strQuery.Append("   AND femt.freight_element_name=trn.frt_desc  ORDER BY unit,femt.preference ");
                            return objWK.GetDataSet(strQuery.ToString());
                        }
                    }
                }
                //end


                if (intInvPk == 0)
                {
                    strQuery.Append(" SELECT type,jobcard_ref_no,COMMODITY_MST_PK,COMMODITY_NAME,unit,pk,jobcard_fk,freight_or_oth,element_fk,currency_mst_fk,element_name,element,currency_id, ");
                    strQuery.Append(" curr,amount,exchange_rate,inv_amount,vat_code,vat_percent VAT_PERCENT,tax_amount,total_amount,remarks,mode1 as \"MODE\",chk,FRT_BOF_FK,JOB_TYPE from ( ");
                    strQuery.Append(" select type,jobcard_ref_no,COMMODITY_MST_PK,COMMODITY_NAME,unit,pk,jobcard_fk,freight_or_oth,element_fk,currency_mst_fk,element_name,element,currency_id,");
                    strQuery.Append(" curr,amount,exchange_rate,inv_amount,vat_code,vat_percent,tax_amount,total_amount,remarks,mode1,chk,FRT_BOF_FK,JOB_TYPE,PREFERENCE from ( ");

                    if (BizType == 1)
                    {
                        strQuery.Append(" SELECT 'FREIGHT' AS TYPE,");
                    }
                    else
                    {
                        strQuery.Append(" SELECT distinct JOBFRT.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,'FREIGHT' AS TYPE,");
                    }
                    strQuery.Append(" JOB.JOBCARD_REF_NO,COMM.COMMODITY_MST_PK,COMM.COMMODITY_NAME,");
                    if (BizType == 1)
                    {
                        strQuery.Append(" CON.CONTAINER_TYPE_MST_ID AS UNIT,");
                    }
                    else
                    {
                        strQuery.Append(" (case when jobfrt.container_type_mst_fk is not null then  con.container_type_mst_id  else '1' end) UNIT, ");
                    }

                    strQuery.Append("       JOBFRT.JOB_TRN_FD_PK AS PK,");
                    strQuery.Append("       JOBFRT.JOB_CARD_TRN_FK AS JOBCARD_FK,");
                    strQuery.Append("       1 FREIGHT_OR_OTH,");
                    if (BizType == 1)
                    {
                        strQuery.Append("       JOBFRT.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,");
                    }
                    strQuery.Append("       JOBFRT.CURRENCY_MST_FK,");
                    strQuery.Append("       FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,");
                    strQuery.Append("       '' AS ELEMENT,");
                    strQuery.Append("       CUMT.CURRENCY_ID,");
                    strQuery.Append("       '' AS CURR,");
                    strQuery.Append("       JOBFRT.FREIGHT_AMT AS AMOUNT,");
                    strQuery.Append("       GET_EX_RATE(JOBFRT.CURRENCY_MST_FK, " + nBaseCurrPK + ",SYSDATE) EXCHANGE_RATE,");
                    strQuery.Append("       JOBFRT.FREIGHT_AMT * GET_EX_RATE(JOBFRT.CURRENCY_MST_FK, " + nBaseCurrPK + ",SYSDATE) AS INV_AMOUNT,");
                    strQuery.Append("(select FETCH_VAT((select BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) from dual )" + ",JOBFRT.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,1) from dual) VAT_CODE,");

                    strQuery.Append("(select FETCH_VAT ((select BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) from dual )" + ",JOBFRT.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2) from dual) VAT_PERCENT,");
                    strQuery.Append("((select FETCH_VAT ((select BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) from dual )" + ",JOBFRT.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2) from dual)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) TAX_AMOUNT,");
                    strQuery.Append("(((select FETCH_VAT ((select BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) from dual )" + ",JOBFRT.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBFRT.FREIGHT_TYPE " + ",JOBFRT.FREIGHT_ELEMENT_MST_FK,2) from dual)* (JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE)/100) + JOBFRT.FREIGHT_AMT * JOBFRT.EXCHANGE_RATE) TOTAL_AMOUNT,");

                    strQuery.Append("       '' AS REMARKS,");
                    strQuery.Append("       'NEW' AS \"MODE1\",");
                    strQuery.Append("       'FALSE' AS CHK,PAR.FRT_BOF_FK, '1' JOB_TYPE,FMT.PREFERENCE");
                    strQuery.Append("  FROM JOB_TRN_FD      JOBFRT,");
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                    strQuery.Append("       CORPORATE_MST_TBL       CORP,");

                    strQuery.Append("        JOB_CARD_TRN JOB,CONTAINER_TYPE_MST_TBL  CON,PARAMETERS_TBL   PAR");
                    strQuery.Append("       ,JOB_TRN_CONT    CNT, COMMODITY_MST_TBL  COMM ");
                    strQuery.Append("        WHERE JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                    strQuery.Append("        AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                    if (BizType == 1)
                    {
                        strQuery.Append("  AND JOB.JOB_CARD_TRN_PK=JOBFRT.JOB_CARD_TRN_FK AND JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK");
                    }
                    else if (BizType == 2)
                    {
                        strQuery.Append("  AND JOB.JOB_CARD_TRN_PK=JOBFRT.JOB_CARD_TRN_FK AND ( JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK or JOBFRT.CONTAINER_TYPE_MST_FK is null ) ");
                    }
                    strQuery.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+) AND JOBFRT.JOB_CARD_TRN_FK IN (" + strJobPks + ")");
                    strQuery.Append("   AND JOBFRT.INVOICE_TBL_FK IS NULL");

                    strQuery.Append("   AND (CNT.JOB_TRN_CONT_PK=JOBFRT.JOB_CARD_TRN_FK  ");
                    strQuery.Append("       OR CNT.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK)  ");
                    strQuery.Append("   AND COMM.COMMODITY_MST_PK = CNT.COMMODITY_MST_FK ");
                    strQuery.Append("   AND (JOBFRT.CONSOL_INVOICE_TRN_FK IS NULL OR  JOBFRT.CONSOL_INVOICE_TRN_FK IN (SELECT cit.consol_invoice_trn_pk FROM consol_invoice_trn_tbl cit WHERE cit.consol_invoice_fk IN( select t.consol_invoice_pk from CONSOL_INVOICE_TBL t WHERE t.chk_invoice =2))) ");
                    strQuery.Append("   AND JOBFRT.FREIGHT_TYPE = " + frtType);

                    strQuery.Append("   AND JOBFRT.INV_AGENT_TRN_FK IS NULL");
                    strQuery.Append("   and JOBFRT.FREIGHT_AMT>0");
                    strQuery.Append("   AND JOBFRT.frtpayer_cust_mst_fk in ( " + CustPk + ")");
                    // added by jitendra on 22/05/07
                    strQuery.Append(" UNION");
                    strQuery.Append(" " + sb.ToString() + "");
                    strQuery.Append(" UNION");
                    strQuery.Append(" " + sb1.ToString() + "");
                    strQuery.Append(" )");
                    strQuery.Append("UNION");

                    strQuery.Append("SELECT 'OTHER' AS TYPE,");
                    strQuery.Append("JOB.JOBCARD_REF_NO ,0 COMMODITY_MST_PK,'Other Charges' COMMODITY_NAME,'Oth.Chrg' AS UNIT,");
                    strQuery.Append("       JOBOTH.JOB_TRN_OTH_PK AS PK,");
                    strQuery.Append("       JOBOTH.JOB_CARD_TRN_FK AS JOBCARD_FK,");
                    strQuery.Append("       2 FREIGHT_OR_OTH,");
                    strQuery.Append("       JOBOTH.FREIGHT_ELEMENT_MST_FK AS ELEMENT_FK,");
                    strQuery.Append("       JOBOTH.CURRENCY_MST_FK,");
                    strQuery.Append("       FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,");
                    strQuery.Append("       '' AS ELEMENT_SEARCH,");
                    strQuery.Append("       CUMT.CURRENCY_ID,");
                    strQuery.Append("       '' AS CURR_SEARCH,");
                    strQuery.Append("       JOBOTH.AMOUNT AS AMOUNT,");
                    strQuery.Append("       GET_EX_RATE(JOBOTH.CURRENCY_MST_FK, " + nBaseCurrPK + ",SYSDATE) EXCHANGE_RATE,");
                    strQuery.Append("       ROUND(JOBOTH.AMOUNT * GET_EX_RATE(JOBOTH.CURRENCY_MST_FK, " + nBaseCurrPK + ",SYSDATE),2) AS INV_AMOUNT,");

                    strQuery.Append("(select FETCH_VAT ((select  BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) from dual )" + ",JOBOTH.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,1) from dual) VAT_CODE,");

                    strQuery.Append("(select FETCH_VAT ((select BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) from dual )" + ",JOBOTH.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2) from dual) VAT_PERCENT,");

                    strQuery.Append("((select FETCH_VAT((select BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) from dual )" + ",JOBOTH.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2) from dual)* (JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100) TAX_AMOUNT,");

                    strQuery.Append("(((select FETCH_VAT((select BB_FETCH_EU(" + Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]) + "," + BizType + "," + Process + " ) from dual )" + ",JOBOTH.FRTPAYER_CUST_MST_FK," + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + ",JOBOTH.FREIGHT_TYPE " + ",JOBOTH.FREIGHT_ELEMENT_MST_FK,2) from dual)* (JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE)/100) + JOBOTH.AMOUNT * JOBOTH.EXCHANGE_RATE) TOTAL_AMOUNT,");

                    strQuery.Append("       '' AS REMARKS,");
                    strQuery.Append("       'NEW' AS \"MODE\",");
                    strQuery.Append("       'FALSE' AS CHK,PAR.FRT_BOF_FK, '1' JOB_TYPE,FMT.PREFERENCE");
                    strQuery.Append("  FROM JOB_TRN_OTH_CHRG JOBOTH,");
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL  FMT,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL    CUMT,");
                    strQuery.Append("       CORPORATE_MST_TBL        CORP,");
                    strQuery.Append("       JOB_CARD_TRN JOB,PARAMETERS_TBL  PAR");
                    strQuery.Append(" WHERE JOBOTH.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK");
                    strQuery.Append("   AND JOBOTH.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                    strQuery.Append("   AND JOB.JOB_CARD_TRN_PK = JOBOTH.JOB_CARD_TRN_FK AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_AFC_FK(+)");
                    strQuery.Append("   AND JOBOTH.JOB_CARD_TRN_FK IN (" + strJobPks + ")");
                    strQuery.Append("   AND JOBOTH.INV_CUST_TRN_FK IS NULL");
                    strQuery.Append("   AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL");
                    strQuery.Append("   AND JOBOTH.FREIGHT_TYPE = 1 ");
                    strQuery.Append("   AND JOBOTH.AMOUNT>0 ");
                    strQuery.Append("   AND JOBOTH.frtpayer_cust_mst_fk in (" + CustPk + ")");

                    strQuery.Append(" UNION SELECT * FROM (SELECT 'FREIGHT' AS TYPE,");
                    strQuery.Append("       DCH.DEM_CALC_ID JOBCARD_REF_NO, 0 COMMODITY_MST_PK, 'Freight Charges' COMMODITY_NAME, ");
                    strQuery.Append("       'DET' UNIT,");
                    strQuery.Append("       TO_NUMBER(NULL) AS PK,");
                    strQuery.Append("       DCH.DEM_CALC_HDR_PK AS JOBCARD_FK,");
                    strQuery.Append("       1 FREIGHT_OR_OTH,");
                    strQuery.Append("       FMT.FREIGHT_ELEMENT_MST_PK AS ELEMENT_FK,");
                    strQuery.Append("       CUMT.CURRENCY_MST_PK,");
                    strQuery.Append("       FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,");
                    strQuery.Append("       '' AS ELEMENT_SEARCH,");
                    strQuery.Append("       CUMT.CURRENCY_ID,");
                    strQuery.Append("       '' AS CURR_SEARCH,");
                    strQuery.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ");
                    strQuery.Append("          FROM DEM_CALC_DTL DCD");
                    strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) AS AMOUNT,");
                    strQuery.Append("       GET_EX_RATE(DCH.CURRENCY_MST_FK,");
                    strQuery.Append("                   " + nBaseCurrPK + ",");
                    strQuery.Append("                   TO_DATE(DCH.DEM_CALC_DATE, 'DD/MM/YYYY')) EXCHANGE_RATE,");
                    strQuery.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ");
                    strQuery.Append("          FROM DEM_CALC_DTL DCD");
                    strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) * ");
                    strQuery.Append("       GET_EX_RATE(DCH.CURRENCY_MST_FK,");
                    strQuery.Append("                   " + nBaseCurrPK + ",");
                    strQuery.Append("                   TO_DATE(DCH.DEM_CALC_DATE, 'DD/MM/YYYY')) AS INV_AMOUNT,");
                    strQuery.Append("       TO_CHAR(NULL) VAT_CODE,");
                    strQuery.Append("       TO_CHAR(NULL) VAT_PERCENT,");
                    strQuery.Append("       TO_NUMBER(NULL) TAX_AMOUNT,");
                    strQuery.Append("       (SELECT SUM(NVL(DCD.DET_AMOUNT, 0) - NVL(DCD.DET_WAIVER, 0)) ");
                    strQuery.Append("          FROM DEM_CALC_DTL DCD");
                    strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) *");
                    strQuery.Append("       GET_EX_RATE(DCH.CURRENCY_MST_FK,");
                    strQuery.Append("                   " + nBaseCurrPK + ",");
                    strQuery.Append("                   TO_DATE(DCH.DEM_CALC_DATE, 'DD/MM/YYYY')) TOTAL_AMOUNT,");
                    strQuery.Append("       '' AS REMARKS,");
                    strQuery.Append("       'NEW' AS \"MODE\",");
                    strQuery.Append("       'FALSE' AS CHK,");
                    strQuery.Append("       PAR.FRT_BOF_FK,");
                    strQuery.Append("       '4' JOB_TYPE,FMT.PREFERENCE");
                    strQuery.Append("  FROM DEM_CALC_HDR            DCH,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                    strQuery.Append("       PARAMETERS_TBL          PAR");
                    strQuery.Append(" WHERE CUMT.CURRENCY_MST_PK = DCH.CURRENCY_MST_FK");
                    strQuery.Append("   AND FMT.FREIGHT_ELEMENT_ID = 'DET'");
                    strQuery.Append("   AND FMT.ACTIVE_FLAG = 1");
                    strQuery.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
                    strQuery.Append("   AND DCH.DET_INVOICE_TRN_FK IS NULL ");
                    strQuery.Append("   AND DCH.DEM_CALC_HDR_PK = (" + strDemPKList + ")");

                    strQuery.Append(" UNION SELECT 'FREIGHT' AS TYPE,");
                    strQuery.Append("       DCH.DEM_CALC_ID JOBCARD_REF_NO, 0 COMMODITY_MST_PK, 'Freight Charges' COMMODITY_NAME, ");
                    strQuery.Append("       'DEM' UNIT,");
                    strQuery.Append("       TO_NUMBER(NULL) AS PK,");
                    strQuery.Append("       DCH.DEM_CALC_HDR_PK AS JOBCARD_FK,");
                    strQuery.Append("       1 FREIGHT_OR_OTH,");
                    strQuery.Append("       FMT.FREIGHT_ELEMENT_MST_PK AS ELEMENT_FK,");
                    strQuery.Append("       CUMT.CURRENCY_MST_PK,");
                    strQuery.Append("       FMT.FREIGHT_ELEMENT_NAME AS ELEMENT_NAME,");
                    strQuery.Append("       '' AS ELEMENT_SEARCH,");
                    strQuery.Append("       CUMT.CURRENCY_ID,");
                    strQuery.Append("       '' AS CURR_SEARCH,");
                    strQuery.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ");
                    strQuery.Append("          FROM DEM_CALC_DTL DCD");
                    strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) AS AMOUNT,");
                    strQuery.Append("       GET_EX_RATE(DCH.CURRENCY_MST_FK,");
                    strQuery.Append("                   " + nBaseCurrPK + ",");
                    strQuery.Append("                   TO_DATE(DCH.DEM_CALC_DATE, 'DD/MM/YYYY')) EXCHANGE_RATE,");
                    strQuery.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ");
                    strQuery.Append("          FROM DEM_CALC_DTL DCD");
                    strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) * ");
                    strQuery.Append("       GET_EX_RATE(DCH.CURRENCY_MST_FK,");
                    strQuery.Append("                   " + nBaseCurrPK + ",");
                    strQuery.Append("                   TO_DATE(DCH.DEM_CALC_DATE, 'DD/MM/YYYY')) AS INV_AMOUNT,");
                    strQuery.Append("       TO_CHAR(NULL) VAT_CODE,");
                    strQuery.Append("       TO_CHAR(NULL) VAT_PERCENT,");
                    strQuery.Append("       TO_NUMBER(NULL) TAX_AMOUNT,");
                    strQuery.Append("       (SELECT SUM(NVL(DCD.DEM_AMOUNT, 0) - NVL(DCD.DEM_WAIVER, 0)) ");
                    strQuery.Append("          FROM DEM_CALC_DTL DCD");
                    strQuery.Append("         WHERE DCH.DEM_CALC_HDR_PK = DCD.DEM_CALC_HDR_FK) *");
                    strQuery.Append("       GET_EX_RATE(DCH.CURRENCY_MST_FK,");
                    strQuery.Append("                   " + nBaseCurrPK + ",");
                    strQuery.Append("                   TO_DATE(DCH.DEM_CALC_DATE, 'DD/MM/YYYY')) TOTAL_AMOUNT,");
                    strQuery.Append("       '' AS REMARKS,");
                    strQuery.Append("       'NEW' AS \"MODE\",");
                    strQuery.Append("       'FALSE' AS CHK,");
                    strQuery.Append("       PAR.FRT_BOF_FK,");
                    strQuery.Append("       '4' JOB_TYPE,FMT.PREFERENCE");
                    strQuery.Append("  FROM DEM_CALC_HDR            DCH,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   CUMT,");
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL FMT,");
                    strQuery.Append("       PARAMETERS_TBL          PAR");
                    strQuery.Append(" WHERE CUMT.CURRENCY_MST_PK = DCH.CURRENCY_MST_FK");
                    strQuery.Append("   AND FMT.FREIGHT_ELEMENT_ID = 'DEM'");
                    strQuery.Append("   AND FMT.ACTIVE_FLAG = 1");
                    strQuery.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = PAR.FRT_BOF_FK(+)");
                    strQuery.Append("   AND DCH.DEM_INVOICE_TRN_FK IS NULL ");
                    strQuery.Append("   AND DCH.DEM_CALC_HDR_PK = (" + strDemPKList + ")) WHERE AMOUNT > 0 ");

                    strQuery.Append(")Q     ORDER BY TYPE,COMMODITY_MST_PK,PREFERENCE ");

                }
                else
                {
                    strQuery.Append(" select type,jobcard_ref_no,COMMODITY_MST_PK,COMMODITY_NAME,unit,pk,jobcard_fk,freight_or_oth,element_fk,currency_mst_fk,element_name,element,currency_id, ");
                    strQuery.Append(" curr,amount,exchange_rate,inv_amount,vat_code,VAT_PERCENT,tax_amount,total_amount,remarks,mode1 as \"MODE\",chk,FRT_BOF_FK,JOB_TYPE from ( ");
                    strQuery.Append(" select type,jobcard_ref_no,COMMODITY_MST_PK,COMMODITY_NAME,unit,pk,jobcard_fk,freight_or_oth,element_fk,currency_mst_fk,element_name,element,currency_id, ");
                    strQuery.Append(" curr,amount,exchange_rate,inv_amount,vat_code,VAT_PERCENT,tax_amount,total_amount,remarks,mode1,chk,FRT_BOF_FK,JOB_TYPE,PREFERENCE from ( ");

                    if (BizType == 1)
                    {
                        strQuery.Append(" SELECT 'FREIGHT' AS TYPE,");
                    }
                    else
                    {
                        strQuery.Append(" SELECT  distinct TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK, DECODE(TRN.FRT_OTH_ELEMENT_FK, 1, 'COST', 2, 'FREIGHT', 3, 'OTHER') AS TYPE,");
                    }

                    strQuery.Append("     JOB.JOBCARD_REF_NO,COMM.COMMODITY_MST_PK,COMM.COMMODITY_NAME,");

                    if (BizType == 1)
                    {
                        strQuery.Append("   CON.CONTAINER_TYPE_MST_ID AS UNIT, ");
                    }
                    else
                    {
                        strQuery.Append("  (case when jobfrt.container_type_mst_fk is not null then  con.container_type_mst_id  else '1' end) UNIT, ");
                    }

                    strQuery.Append(" JOBFRT.JOB_TRN_FD_PK AS PK,");
                    strQuery.Append("       TRN.JOB_CARD_FK AS JOBCARD_FK,");
                    strQuery.Append("       TRN.FRT_OTH_ELEMENT FREIGHT_OR_OTH,");

                    if (BizType == 1)
                    {
                        strQuery.Append("       TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK,");
                    }

                    strQuery.Append("       TRN.CURRENCY_MST_FK,");
                    strQuery.Append("    trn.frt_desc AS ELEMENT_NAME,");
                    //Added By jitendra 
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
                    //Added by Venkata 
                    strQuery.Append("       TRN.TAX_PCNT AS VAT_PERCENT,");
                    strQuery.Append("       TRN.TAX_AMT AS TAX_AMOUNT,");
                    strQuery.Append("       TRN.TOT_AMT AS TOTAL_AMOUNT,");
                    strQuery.Append("       TRN.REMARKS,");
                    strQuery.Append("       'EDIT' AS \"MODE1\",");
                    strQuery.Append("       'TRUE' AS CHK,PAR.FRT_BOF_FK, '1' JOB_TYPE,FMT.PREFERENCE");
                    strQuery.Append("  FROM CONSOL_INVOICE_TRN_TBL TRN,");
                    strQuery.Append("       CONSOL_INVOICE_TBL     HDR,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL  CUMT,freight_element_mst_tbl fmt,JOB_TRN_FD JOBFRT,CONTAINER_TYPE_MST_TBL CON,PARAMETERS_TBL PAR, ");
                    strQuery.Append("        JOB_CARD_TRN JOB,COMMODITY_MST_TBL COMM");
                    strQuery.Append(" WHERE TRN.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK");
                    strQuery.Append("   AND TRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                    strQuery.Append("   AND JOB.JOB_CARD_TRN_PK=TRN.JOB_CARD_FK");
                    strQuery.Append("   AND COMM.COMMODITY_MST_PK(+) = TRN.COMMODITY_MST_FK");
                    strQuery.Append("   AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK and jobfrt.consol_invoice_trn_fk = trn.consol_invoice_trn_pk ");
                    //
                    strQuery.Append(" AND TRN.JOB_CARD_FK  = JOBFRT.JOB_CARD_TRN_FK ");

                    if (BizType == 1)
                    {
                        strQuery.Append(" AND JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK(+) ");
                    }
                    else
                    {
                        strQuery.Append("  AND ( JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK or JOBFRT.CONTAINER_TYPE_MST_FK is null ) ");
                    }
                    if (rowcunt <= 0)
                    {
                        strQuery.Append(" AND JOBFRT.FREIGHT_ELEMENT_MST_FK = TRN.FRT_OTH_ELEMENT_FK ");
                    }
                    else
                    {
                        if (Contpk > 0)
                        {
                            strQuery.Append("  and jobfrt.JOB_TRN_FD_PK in ( " + Contpk + " )");
                        }
                    }
                    strQuery.Append(" AND TRN.FRT_OTH_ELEMENT_FK = PAR.FRT_BOF_FK(+) ");

                    strQuery.Append("  AND HDR.CONSOL_INVOICE_PK = " + intInvPk + " ");
                    strQuery.Append(" UNION");
                    strQuery.Append(" " + sb2.ToString() + "");
                    strQuery.Append(" UNION");
                    strQuery.Append(" " + sb3.ToString() + "");
                    strQuery.Append(" )");
                    strQuery.Append(" UNION ");
                    strQuery.Append("  SELECT 'OTHER' AS TYPE,");
                    strQuery.Append("       JOB.JOBCARD_REF_NO,0 COMMODITY_MST_PK,'' COMMODITY_NAME,");
                    strQuery.Append("       'Oth.Chrg' AS UNIT, JOBOTH.JOB_TRN_OTH_PK AS PK,");
                    strQuery.Append("       TRN.JOB_CARD_FK AS JOBCARD_FK,");
                    strQuery.Append("      TRN.FRT_OTH_ELEMENT ,");
                    strQuery.Append("       TRN.FRT_OTH_ELEMENT_FK AS ELEMENT_FK,");
                    strQuery.Append("       TRN.CURRENCY_MST_FK,");
                    strQuery.Append("      trn.frt_desc AS ELEMENT_NAME,");
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
                    strQuery.Append("       TRN.TAX_PCNT AS VAT_PERCENT,");
                    strQuery.Append("       TRN.TAX_AMT AS TAX_AMOUNT,");
                    strQuery.Append("       TRN.TOT_AMT AS TOTAL_AMOUNT,");
                    strQuery.Append("       TRN.REMARKS,");
                    strQuery.Append("       'EDIT' AS \"MODE\",");
                    strQuery.Append("       'TRUE' AS CHK,PAR.FRT_BOF_FK, '1' JOB_TYPE,FEMT.PREFERENCE");
                    strQuery.Append("  FROM CONSOL_INVOICE_TRN_TBL TRN,");
                    strQuery.Append("       CONSOL_INVOICE_TBL     HDR,");
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL  CUMT,JOB_TRN_OTH_CHRG JOBoth,PARAMETERS_TBL PAR, ");
                    strQuery.Append("       JOB_CARD_TRN JOB,FREIGHT_ELEMENT_MST_TBL      FEMT ");
                    strQuery.Append(" WHERE TRN.CONSOL_INVOICE_FK = HDR.CONSOL_INVOICE_PK");
                    strQuery.Append("   AND TRN.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
                    strQuery.Append("   AND JOB.JOB_CARD_TRN_PK=TRN.JOB_CARD_FK");
                    strQuery.Append("   AND TRN.JOB_CARD_FK  = JOBoth.JOB_CARD_TRN_FK ");
                    strQuery.Append("   AND JOBOTH.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK ");
                    if (rowcunt <= 0)
                    {
                        strQuery.Append(" AND JOBoth.FREIGHT_ELEMENT_MST_FK = TRN.FRT_OTH_ELEMENT_FK ");
                    }
                    else
                    {
                        if (Contpk > 0)
                        {
                            strQuery.Append("  and JOBoth.JOB_TRN_OTH_PK in (" + Contpk + " ) ");
                        }
                    }
                    //end
                    strQuery.Append(" AND TRN.FRT_OTH_ELEMENT_FK = PAR.FRT_BOF_FK(+) ");
                    strQuery.Append("   AND HDR.CONSOL_INVOICE_PK = " + intInvPk + "");
                    strQuery.Append("  ) Q order by TYPE,COMMODITY_MST_PK,PREFERENCE ");
                }
                if (BizType == 1)
                {
                    strQuery.Replace("CON.CONTAINER_TYPE_MST_ID", "upper(JOBFRT.QUANTITY)");
                    strQuery.Replace("FRT_BOF_FK", "FRT_AFC_FK");
                    strQuery.Replace("CONTAINER_TYPE_MST_TBL CON,", " ");
                    strQuery.Replace("AND JOBFRT.CONTAINER_TYPE_MST_FK = CON.CONTAINER_TYPE_MST_PK", " ");
                }
                DS = objWK.GetDataSet(strQuery.ToString());
                return DS;
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
        //adding by thiyagarajan on 3/1/09 as "no.of palattes" col.introduced in Detention form :VEK Gap analysis
        public Int32 FetchDetFrt(int invPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            try
            {
                strBuilder.Append(" select count(*) from consol_invoice_trn_tbl contran,parameters_tbl pmt ");
                strBuilder.Append(" where contran.consol_invoice_fk=" + invPk);
                strBuilder.Append(" and contran.frt_oth_element_fk=pmt.frt_det_charge_fk");
                return Convert.ToInt32(objWK.ExecuteScaler(strBuilder.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //modifying by thiyagarajan on 3/1/09 as "no.of palattes" col.introduced in Detention form :VEK Gap analysis
        public Int32 FetchDetContPk(int Jobpk, Int32 invPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            DataSet dspk = null;
            Int32 pk = default(Int32);
            Int32 i = default(Int32);
            string strpk = "";
            try
            {
                strBuilder.Append(" SELECT max(oth.JOB_TRN_OTH_PK) FROM JOB_TRN_OTH_CHRG OTH WHERE  ");
                strBuilder.Append(" oth.JOB_CARD_TRN_FK in  (" + Jobpk + ")");
                pk = Convert.ToInt32(getDefault(objWK.ExecuteScaler(strBuilder.ToString()), 0));
                if (pk == 0)
                {
                    strBuilder.Remove(0, strBuilder.Length);
                    strBuilder.Append(" SELECT MAX(FD.JOB_TRN_FD_PK) FROM JOB_TRN_FD FD WHERE  ");
                    strBuilder.Append(" FD.JOB_CARD_TRN_FK IN (" + Jobpk + ")");
                    pk = Convert.ToInt32(getDefault(objWK.ExecuteScaler(strBuilder.ToString()), 0));
                }
                return pk;
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
        //end
        public DataSet FetchHeader(int invPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            strBuilder.Append(" select cm.customer_name cust, ct.currency_id Cur,");
            strBuilder.Append(" ci.invoice_ref_no REFNO, ");
            strBuilder.Append(" ci.invoice_date CDATE, ");
            strBuilder.Append(" ci.invoice_amt INVAMT,");
            strBuilder.Append(" ci.discount_amt DISAMT, ");
            strBuilder.Append(" ci.net_receivable NET,ci.INV_UNIQUE_REF_NR, ");
            //Snigdharani - 08/01/2009 - Unique Invoice Reference Number(VEK)
            strBuilder.Append(" ci.remarks,ci.tds_remarks,ci.aif,");
            strBuilder.Append(" BMT.BANK_MST_PK, BMT.BANK_ID, BMT.BANK_NAME, ");
            strBuilder.Append("   UMTCRT.USER_NAME    AS CREATED_BY, ");
            strBuilder.Append("   UMTUPD.USER_NAME    AS LAST_MODIFIED_BY, ");
            strBuilder.Append("   UMTAPP.USER_NAME    AS APPROVED_BY, ");
            strBuilder.Append("   TO_DATE(ci.CREATED_DT) CREATED_BY_DT, ");
            strBuilder.Append("   TO_DATE(ci.LAST_MODIFIED_DT) LAST_MODIFIEDBY_DT, ");
            strBuilder.Append("   TO_DATE(ci.LAST_MODIFIED_DT) APPROVED_DT,ci.last_modified_by_fk,ci.chk_invoice ,ci.created_by_fk ");
            strBuilder.Append(" from consol_invoice_tbl ci,customer_mst_tbl cm, currency_type_mst_tbl ct,BANK_MST_TBL BMT, ");
            strBuilder.Append("  USER_MST_TBL UMTCRT, ");
            strBuilder.Append("  USER_MST_TBL UMTUPD, ");
            strBuilder.Append("  USER_MST_TBL UMTAPP ");
            strBuilder.Append(" where ci.consol_invoice_pk = " + invPk + "");
            strBuilder.Append(" and cm.customer_mst_pk = ci.customer_mst_fk and");
            strBuilder.Append(" ct.currency_mst_pk = ci.currency_mst_fk");
            strBuilder.Append(" AND BMT.BANK_MST_PK(+) = CI.BANK_MST_FK");
            strBuilder.Append(" AND UMTCRT.USER_MST_PK(+) = ci.CREATED_BY_FK ");
            strBuilder.Append(" AND UMTUPD.USER_MST_PK(+) = ci.LAST_MODIFIED_BY_FK  ");
            strBuilder.Append(" AND UMTAPP.USER_MST_PK(+) = ci.LAST_MODIFIED_BY_FK  ");
            try
            {
                return objWK.GetDataSet(strBuilder.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchHeaderFAC(int invPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            strBuilder.Append(" select OPR.OPERATOR_NAME cust, ct.currency_id Cur,");
            strBuilder.Append(" ci.invoice_ref_no REFNO, ");
            strBuilder.Append(" ci.invoice_date CDATE, ");
            strBuilder.Append(" ci.invoice_amt INVAMT,");
            strBuilder.Append(" ci.discount_amt DISAMT, ");
            strBuilder.Append(" ci.net_receivable NET,ci.INV_UNIQUE_REF_NR, ");
            //Snigdharani - 08/01/2009 - Unique Invoice Reference Number(VEK)
            strBuilder.Append(" ci.remarks,ci.tds_remarks,ci.aif, ");
            strBuilder.Append(" NULL BANK_MST_PK, '' BANK_ID, '' BANK_NAME ");
            strBuilder.Append("   UMTCRT.USER_NAME    AS CREATED_BY, ");
            strBuilder.Append("   UMTUPD.USER_NAME    AS LAST_MODIFIED_BY, ");
            strBuilder.Append("   UMTAPP.USER_NAME    AS APPROVED_BY, ");
            strBuilder.Append("   TO_DATE(ci.CREATED_DT) CREATED_BY_DT, ");
            strBuilder.Append("   TO_DATE(ci.LAST_MODIFIED_DT) LAST_MODIFIEDBY_DT, ");
            strBuilder.Append("   TO_DATE(ci.LAST_MODIFIED_DT) APPROVED_DT,ci.last_modified_by_fk,ci.chk_invoice ,ci.created_by_fk ");
            strBuilder.Append(" from consol_invoice_tbl ci,OPERATOR_MST_TBL OPR, currency_type_mst_tbl ct, ");
            strBuilder.Append("  USER_MST_TBL UMTCRT, ");
            strBuilder.Append("  USER_MST_TBL UMTUPD, ");
            strBuilder.Append("  USER_MST_TBL UMTAPP ");
            strBuilder.Append(" where ci.consol_invoice_pk = " + invPk + "");
            strBuilder.Append(" AND CT.CURRENCY_MST_PK = CI.CURRENCY_MST_FK and");
            strBuilder.Append(" ct.currency_mst_pk = ci.currency_mst_fk");
            strBuilder.Append(" AND UMTCRT.USER_MST_PK(+) = ci.CREATED_BY_FK ");
            strBuilder.Append(" AND UMTUPD.USER_MST_PK(+) = ci.LAST_MODIFIED_BY_FK  ");
            strBuilder.Append(" AND UMTAPP.USER_MST_PK(+) = ci.LAST_MODIFIED_BY_FK  ");
            strBuilder.Append(" AND CI.IS_FAC_INV = 1");
            strBuilder.Append(" ");
            try
            {
                return objWK.GetDataSet(strBuilder.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

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
                sqlinv.Append(" select sum(cc.tax_amt) taxamt ,sum(cc.amt_in_inv_curr) invamt, c.inv_unique_ref_nr from consol_invoice_trn_tbl cc,consol_invoice_tbl c where ");
                sqlinv.Append(" c.invoice_ref_no like '" + refno + "' and c.consol_invoice_pk=cc.consol_invoice_fk group by c.inv_unique_ref_nr");
                dsinvoice.Tables.Add(objWK.GetDataTable(sqlinv.ToString()));
                return dsinvoice;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //Added new parameter Snigdharani - 08/01/2009 - Unique Invoice Reference Number(VEK Client)
        public int SaveData(DataSet dsSave, object InvRefNo, long nLocationPk, string CREATED_BY_FK, string BizType, string Process, long nEmpId, double CrLimit, string Customer, double NetAmt,
        double CrLimitUsed, int extype = 1, string uniqueRefNr = "", int InvType = 0, int IsFac = 0, int InvStatus = 0)
        {

            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();

            int intSaveSucceeded = 0;
            OracleTransaction TRAN = null;
            int intPkValue = 0;
            int intChldCnt = 0;
            Int32 cargotype = default(Int32);
            try
            {
                if (string.IsNullOrEmpty(Convert.ToString(InvRefNo)))
                {
                    InvRefNo = GenerateInvoiceNo(nLocationPk, nEmpId, Convert.ToInt64(dsSave.Tables[0].Rows[0]["CREATED_BY_FK_IN"]), objWK);
                    if (Convert.ToString(InvRefNo) == "Protocol Not Defined.")
                    {
                        InvRefNo = "";
                        return -1;
                    }
                }
                TRAN = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = TRAN;
                //Snigdharani - 08/01/2009 - Unique Invoice Reference Number(VEK)
                int UNIQUE = 0;
                if (string.IsNullOrEmpty(uniqueRefNr))
                {
                    System.DateTime dt = default(System.DateTime);
                    dt = System.DateTime.Now;
                    string st = null;
                    st = Convert.ToString(dt.Day + dt.Month + dt.Year + dt.Hour + dt.Minute + dt.Second + dt.Millisecond);

                    uniqueRefNr = GetVEKInvoiceRef(0, 0, st);
                    //While IsUniqueRefNr(uniqueRefNr, objWK.MyCommand) > 0
                    //    uniqueRefNr = GetVEKInvoiceRef(10000000, 99999999)
                    //End While
                }
                uniqueReferenceNr = uniqueRefNr;
                //End Snigdharani
                var _with1 = dsSave.Tables[0].Rows[0];
                objWK.MyCommand.Connection = objWK.MyConnection;
                objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                objWK.MyCommand.CommandText = objWK.MyUserName + ".CONSOL_INV_PKG.CONSOL_INV_HDR_INS";
                objWK.MyCommand.Parameters.Clear();
                objWK.MyCommand.Parameters.Add("PROCESS_TYPE_IN", Convert.ToInt32(_with1["PROCESS_TYPE_IN"]));
                objWK.MyCommand.Parameters.Add("BUSINESS_TYPE_IN", Convert.ToInt32(_with1["BUSINESS_TYPE_IN"]));
                objWK.MyCommand.Parameters.Add("CHK_INVOICE_IN", InvStatus);
                objWK.MyCommand.Parameters.Add("CUSTOMER_MST_FK_IN", (string.IsNullOrEmpty(_with1["CUSTOMER_MST_FK_IN"].ToString()) ? DBNull.Value : _with1["CUSTOMER_MST_FK_IN"]));
                objWK.MyCommand.Parameters.Add("INVOICE_REF_NO_IN", Convert.ToString(InvRefNo));
                objWK.MyCommand.Parameters.Add("INVOICE_DATE_IN", _with1["INVOICE_DATE_IN"]);
                objWK.MyCommand.Parameters.Add("INVOICE_DUE_DATE_IN", _with1["INVOICE_DATE_IN"]);
                objWK.MyCommand.Parameters.Add("CURRENCY_MST_FK_IN", _with1["CURRENCY_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("BANK_MST_FK_IN", _with1["BANK_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("INVOICE_AMT_IN", _with1["INVOICE_AMT_IN"]);
                objWK.MyCommand.Parameters.Add("DISCOUNT_AMT_IN", _with1["DISCOUNT_AMT_IN"]);
                objWK.MyCommand.Parameters.Add("NET_RECEIVABLE_IN", _with1["NET_RECEIVABLE_IN"]);
                objWK.MyCommand.Parameters.Add("REMARKS_IN", _with1["REMARKS_IN"]);
                objWK.MyCommand.Parameters.Add("CREATED_BY_FK_IN", _with1["CREATED_BY_FK_IN"]);
                objWK.MyCommand.Parameters.Add("EXCH_RATE_TYPE_FK_IN", getDefault(extype, DBNull.Value));
                objWK.MyCommand.Parameters.Add("INV_UNIQUE_REF_NR_IN", getDefault(uniqueRefNr, DBNull.Value));
                //Snigdharani - 08/01/2009 - Unique Invoice Reference Number(VEK Client)
                objWK.MyCommand.Parameters.Add("INV_TYPE_IN", InvType);
                objWK.MyCommand.Parameters.Add("TDS_REMARKS_IN", getDefault(_with1["TDS_REMARKS_IN"], DBNull.Value));
                //'For FAC
                objWK.MyCommand.Parameters.Add("SUPPLIER_MST_FK_IN", _with1["SUPPLIER_MST_FK_IN"]);
                objWK.MyCommand.Parameters.Add("IS_FAC_INV_IN", _with1["IS_FAC_INV"]);
                //'End
                objWK.MyCommand.Parameters.Add("AIF_IN", _with1["AIF_IN"]);
                objWK.MyCommand.Parameters.Add("RETURN_VALUE", _with1["CREATED_BY_FK_IN"]).Direction = ParameterDirection.Output;
                intSaveSucceeded = objWK.MyCommand.ExecuteNonQuery();
                intPkValue = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                lngInvPk = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);

                //If intSaveSucceeded > 0 Then
                //    TRAN.Commit()
                //Else
                //    TRAN.Rollback()
                //End If

                //If dsSave.Tables(1).Rows.Count > 0 Then
                //    If dsSave.Tables(0).Rows(0)["BUSINESS_TYPE_IN") = 2 Then
                //        cargotype = FetchCargoType(dsSave.Tables(1).Rows(0)["JOB_CARD_FK_IN"), _
                //        dsSave.Tables(0).Rows(0)["BUSINESS_TYPE_IN"), dsSave.Tables(0).Rows(0)["PROCESS_TYPE_IN"))
                //    End If
                //End If
                try
                {
                    objWK.MyCommand.Parameters.Clear();
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".CONSOL_INV_PKG.BB_CONSOL_INV_DETAILS_DEL";
                    objWK.MyCommand.Parameters.Add("CONSOL_INVOICE_FK_IN", intPkValue);
                    objWK.MyCommand.ExecuteNonQuery();

                }
                catch (Exception ex)
                {
                }


                for (intChldCnt = 0; intChldCnt <= dsSave.Tables[1].Rows.Count - 1; intChldCnt++)
                {
                    objWK.MyCommand.Parameters.Clear();
                    objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                    objWK.MyCommand.CommandText = objWK.MyUserName + ".CONSOL_INV_PKG.BB_CONSOL_INV_DETAILS_INS";

                    //TRAN = objWK.MyConnection.BeginTransaction()
                    //objWK.MyCommand.Transaction = TRAN

                    var _with2 = dsSave.Tables[1].Rows[intChldCnt];
                    objWK.MyCommand.Parameters.Add("CONSOL_INVOICE_FK_IN", intPkValue);
                    objWK.MyCommand.Parameters.Add("PROCESS_TYPE_IN", Convert.ToInt32(dsSave.Tables[0].Rows[0]["PROCESS_TYPE_IN"]));
                    objWK.MyCommand.Parameters.Add("BUSINESS_TYPE_IN", Convert.ToInt32(dsSave.Tables[0].Rows[0]["BUSINESS_TYPE_IN"]));
                    objWK.MyCommand.Parameters.Add("JOB_CARD_FK_IN", _with2["JOB_CARD_FK_IN"]);
                    objWK.MyCommand.Parameters.Add("FRT_OTH_ELEMENT_IN", _with2["FRT_OTH_ELEMENT_IN"]);
                    objWK.MyCommand.Parameters.Add("FRT_OTH_ELEMENT_FK_IN", _with2["FRT_OTH_ELEMENT_FK_IN"]);

                    objWK.MyCommand.Parameters.Add("FRTS_TBL_FK_IN", _with2["FRT_TBL_FK_IN"]);
                    objWK.MyCommand.Parameters.Add("COMMODITY_MST_FK_IN", _with2["COMMODITY_MST_FK_IN"]);
                    //FAHEEM
                    objWK.MyCommand.Parameters.Add("CURRENCY_MST_FK_IN", _with2["CURRENCY_MST_FK_IN"]);
                    objWK.MyCommand.Parameters.Add("ELEMENT_AMT_IN", _with2["ELEMENT_AMT_IN"]);
                    objWK.MyCommand.Parameters.Add("EXCHANGE_RATE_IN", _with2["EXCHANGE_RATE_IN"]);
                    objWK.MyCommand.Parameters.Add("AMT_IN_INV_CURR_IN", _with2["AMT_IN_INV_CURR_IN"]);
                    objWK.MyCommand.Parameters.Add("VAT_CODE_IN", _with2["VAT_CODE_IN"]);
                    //Added by Venkata
                    objWK.MyCommand.Parameters.Add("TAX_PCNT_IN", _with2["TAX_PCNT_IN"]);
                    objWK.MyCommand.Parameters.Add("TAX_AMT_IN", _with2["TAX_AMT_IN"]);
                    objWK.MyCommand.Parameters.Add("TOT_AMT_IN", _with2["TOT_AMT_IN"]);
                    objWK.MyCommand.Parameters.Add("TOT_AMT_IN_LOC_CURR_IN", _with2["TOT_AMT_IN_LOC_CURR_IN"]);
                    objWK.MyCommand.Parameters.Add("REMARKS_IN", _with2["REMARKS_IN"]);
                    objWK.MyCommand.Parameters.Add("JOBTYPE_IN", _with2["JOBTYPE_IN"]);
                    objWK.MyCommand.Parameters.Add("FRT_DESC_IN", _with2["FRT_DESC_IN"]);
                    // Added By jitendra 
                    objWK.MyCommand.Parameters.Add("LOGED_IN_LOC_FK_IN", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                    intSaveSucceeded = objWK.MyCommand.ExecuteNonQuery();
                }
                //modified by thiyagarajan
                //when we generate invoice for multiple jobcard , more than one jobcard pk stored into CONSOL_INVOICE_TRN_TBL
                //when we retrieve jobcard pk from that table for trackNtrace , error raising.
                //The moment a record stored into CONSOL_INVOICE_TRN_TBL , also we store it into TrackNtrace table
                if (intSaveSucceeded > 0)
                {
                    TRAN.Commit();
                    if (IsFac != 1)
                    {
                        if (CrLimit > 0)
                        {
                            SaveCreditLimit(NetAmt, Customer, CrLimitUsed, TRAN);
                        }
                        //By Amit Singh on 07-Sep-07
                        //To save the Invoice in Track N Trace table
                        //Air - Export
                        if (Convert.ToInt32(BizType) == 1 & Convert.ToInt32(Process) == 1)
                        {
                            SaveBBTrackAndTraceForInv(TRAN, Convert.ToInt32(lngInvPk), 1, 1, "Invoice", "INV-AIR-EXP", Convert.ToInt32(nLocationPk), objWK, "INS", Convert.ToInt32(CREATED_BY_FK),                            "O");
                            //Air - Import
                        }
                        else if (Convert.ToInt32(BizType) == 1 & Convert.ToInt32(Process )== 2)
                        {
                            SaveBBTrackAndTraceForInv(TRAN, Convert.ToInt32(lngInvPk), 1, 2, "Invoice", "INV-AIR-IMP", Convert.ToInt32(nLocationPk), objWK, "INS", Convert.ToInt32(CREATED_BY_FK),
                            "O");
                            //Sea - Export
                        }
                        else if (Convert.ToInt32(BizType) == 2 & Convert.ToInt32(Process) == 1)
                        {
                            SaveBBTrackAndTraceForInv(TRAN, Convert.ToInt32(lngInvPk), 2, 1, "Invoice", "INV-SEA-EXP", Convert.ToInt32(nLocationPk), objWK, "INS", Convert.ToInt32(CREATED_BY_FK),
                            "O");
                            //Sea - Import
                        }
                        else if (Convert.ToInt32(BizType) == 2 & Convert.ToInt32(Process) == 2)
                        {
                            SaveBBTrackAndTraceForInv(TRAN, Convert.ToInt32(lngInvPk), 2, 2, "Invoice", "INV-SEA-IMP", Convert.ToInt32(nLocationPk), objWK, "INS", Convert.ToInt32(CREATED_BY_FK),
                            "O");
                        }
                    }
                    //***************************************************************************************
                    if (InvStatus == 1 & intPkValue > 0)
                    {
                        if (Convert.ToBoolean(ConfigurationManager.AppSettings["QFINGeneral"]) == true)
                        {
                            try
                            {
                                TRAN = objWK.MyConnection.BeginTransaction();
                                objWK.MyCommand.Transaction = TRAN;
                                objWK.MyCommand.Parameters.Clear();
                                objWK.MyCommand.CommandText = objWK.MyUserName + ".ACCOUNTING_INTEGREATION_PKG.INVOICE_APPROVE_CANCEL";
                                objWK.MyCommand.Parameters.Add("INVOICE_TRN_FK_IN", intPkValue).Direction = ParameterDirection.Input;
                                objWK.MyCommand.Parameters.Add("LOCAL_CUR_FK", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                                objWK.MyCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                                objWK.MyCommand.ExecuteNonQuery();
                                TRAN.Commit();

                            }
                            catch (Exception ex)
                            {
                            }
                        }

                        //QFORBusinessDev.Master.cls_Scheduler objSch = new QFORBusinessDev.Master.cls_Scheduler();
                        //ArrayList schDtls = null;
                        //bool errGen = false;
                        //if (objSch.GetSchedulerPushType() == true)
                        //{
                        //    QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                        //    try
                        //    {
                        //        schDtls = objSch.FetchSchDtls();
                        //        //'Used to Fetch the Sch Dtls
                        //        objPush.UpdateTaxElement(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                        //        objPush.UpdatebankAcc(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                        //        objPush.UpdateCurrency(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                        //        objPush.UpdateTransaction(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, intPkValue);
                        //        if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                        //        {
                        //            objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                        //        }
                        //    }
                        //    catch (Exception ex)
                        //    {
                        //        if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                        //        {
                        //            objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
                        //        }
                        //    }
                        //}
                    }
                    //****************************************************************************************
                }
                else
                {
                    TRAN.Rollback();
                    //added by minakshi on 18-feb-09 for protocol rollbacking transcations
                    RollbackProtocolKey("CONSOLIDATED INVOICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(InvRefNo), System.DateTime.Now);
                }
                //Next intChldCnt
                //If CrLimit > 0 Then
                //    SaveCreditLimit(NetAmt, Customer, CrLimitUsed, TRAN)
                //End If
                return intSaveSucceeded;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                //added by minakshi on 18-feb-09 for protocol rollbacking transcations
                RollbackProtocolKey("CONSOLIDATED INVOICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), Convert.ToString(InvRefNo), System.DateTime.Now);
                throw ex;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }
        #endregion
        //By Amit Singh on 06-Sep-07
        #region "save TrackAndTrace"
        public ArrayList SaveBBTrackAndTraceForInv(OracleTransaction TRAN, int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, WorkFlow objWF, string flagInsUpd, long lngCreatedby,
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
                var _with3 = objWF.MyCommand;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWF.MyUserName + ".TRACK_N_TRACE_PKG.BB_TRACK_N_TRACE_INS";
                _with3.Transaction = TRAN1;
                _with3.Parameters.Clear();
                _with3.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("Container_Data_in", DBNull.Value).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with3.ExecuteNonQuery();
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
            finally
            {
                objWF.MyCommand.Connection.Close();
            }
        }
        #endregion

        #region " Protocol Reference Number"
        public string GenerateInvoiceNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow ObjWK = null)
        {
            return GenerateProtocolKey("CONSOLIDATED INVOICE", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy, ObjWK);
        }
        #endregion

        #region "Parent"
        //Created By Mani.Sureshkumar
        //Function for fetching Data in Listing Screen
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

            //Sea Export
            if (BizType == 2 & process == 1)
            {
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" HBL_EXP_TBL            HBL,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
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
                strCondition.Append(" JOB_CARD_TRN   JOB,");
                strCondition.Append(" CUSTOMER_MST_TBL       CMT,");
                strCondition.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                strCondition.Append(" USER_MST_TBL           UMT");
                strCondition.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
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

        #endregion

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
                BuildQuery.Append(" JOB_CARD_TRN   JOB,");
                BuildQuery.Append(" HBL_EXP_TBL            HBL,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
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
                BuildQuery.Append(" JOB_CARD_TRN   JOB,");
                BuildQuery.Append(" CUSTOMER_MST_TBL       CMT,");
                BuildQuery.Append(" CURRENCY_TYPE_MST_TBL  CUMT,");
                BuildQuery.Append(" USER_MST_TBL           UMT");
                BuildQuery.Append(" WHERE INVTRN.job_card_fk = JOB.JOB_CARD_TRN_PK");
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
        #endregion
        #region "GET PK"
        //adding by thiyagarajan on 10/11/08 to display PDF through JOBCARD Entry Screen :PTS Task
        public Int32 GetInvPk(string RefNo)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            try
            {
                Strsql = "select con.consol_invoice_pk from consol_invoice_tbl con where con.invoice_ref_no like '" + RefNo + "'";
                return Convert.ToInt32(Objwk.ExecuteScaler(Strsql));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "PK Value"
        private string AllMasterPKs(DataSet ds)
        {
            try
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
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

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
            string IsWriteOff = "";
            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strProcessType = arr[3];
            strloc = arr[4];
            if (arr.Length > 5)
                IsWriteOff = arr[5];
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                //'Snigdharani - To fetch invoices for which writeoff can be done
                if (IsWriteOff == "FROMWRITEOFF")
                {
                    selectCommand.CommandText = objWF.MyUserName + ".en_consol_invoice_pkg.GET_INVOICE_WRITEOFF";
                }
                else
                {
                    selectCommand.CommandText = objWF.MyUserName + ".en_consol_invoice_pkg.get_invoice";
                }
                //selectCommand.CommandText = objWF.MyUserName & ".en_consol_invoice_pkg.get_invoice"

                var _with4 = selectCommand.Parameters;
                _with4.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with4.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input;
                _with4.Add("LOCFK_IN", strloc).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
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
        public void SaveCreditLimit(double NetAmt, string Customer, double CrLimitUsed, OracleTransaction TRAN)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();

            OracleTransaction TRAN1 = null;
            OracleCommand cmd = new OracleCommand();
            TRAN1 = objWK.MyConnection.BeginTransaction();
            cmd.Transaction = TRAN1;
            string strSQL = null;
            double temp = 0;
            //temp = CrLimitUsed + NetAmt
            temp = CrLimitUsed - NetAmt;
            // Changed By Prakash Chandra on 4/06/2008 
            try
            {
                cmd.CommandType = CommandType.Text;
                cmd.Connection = TRAN1.Connection;
                cmd.Transaction = TRAN1;

                cmd.Parameters.Clear();
                strSQL = "update customer_mst_tbl a set a.credit_limit_used = " + temp;
                strSQL = strSQL + " where a.customer_name in ('" + Customer + "')";
                cmd.CommandText = strSQL;
                cmd.ExecuteNonQuery();
                TRAN1.Commit();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //#Region "Fetch Print Header Report "
        //        Public Function Fetch_Header_print(ByVal invPk As Integer, ByVal BizType As Integer, ByVal ProType As Integer) As DataSet
        //            Dim strQuery As New System.Text.StringBuilder
        //            Dim objWF As New WorkFlow

        //            Try


        //                strQuery.Append("" & vbCrLf)
        //                strQuery.Append("SELECT  distinct CIT.CONSOL_INVOICE_PK," & vbCrLf)
        //                strQuery.Append("       JCSE.JOBCARD_REF_NO," & vbCrLf)
        //                strQuery.Append("       CUST.CUSTOMER_ID," & vbCrLf)
        //                strQuery.Append("       ( SELECT PMTL.PORT_ID FROM PORT_MST_TBL PMTL WHERE PMTL.PORT_MST_PK= BST.PORT_MST_POL_FK ) POL," & vbCrLf)
        //                strQuery.Append("       ( SELECT PMTD.PORT_ID FROM PORT_MST_TBL PMTD WHERE PMTD.PORT_MST_PK= BST.PORT_MST_POD_FK ) POD," & vbCrLf)
        //                strQuery.Append("       HBL.HBL_REF_NO," & vbCrLf)
        //                strQuery.Append("       MBL.MBL_REF_NO," & vbCrLf)
        //                strQuery.Append("       CIT.INVOICE_AMT" & vbCrLf)
        //                strQuery.Append("  FROM CONSOL_INVOICE_TBL  CIT," & vbCrLf)
        //                strQuery.Append("       CONSOL_INVOICE_TRN_TBL CITT," & vbCrLf)
        //                strQuery.Append("       JOB_CARD_TRN JCSE," & vbCrLf)
        //                strQuery.Append("       CUSTOMER_MST_TBL CUST," & vbCrLf)
        //                strQuery.Append("       booking_mst_tbl BST," & vbCrLf)
        //                strQuery.Append("       HBL_EXP_TBL   HBL," & vbCrLf)
        //                strQuery.Append("       MBL_EXP_TBL MBL" & vbCrLf)
        //                strQuery.Append(" WHERE CITT.CONSOL_INVOICE_FK=CIT.CONSOL_INVOICE_PK" & vbCrLf)
        //                strQuery.Append("   AND CITT.JOB_CARD_FK= JCSE.JOB_CARD_TRN_PK(+)" & vbCrLf)
        //                strQuery.Append("   AND JCSE.CONSIGNEE_CUST_MST_FK=CUST.CUSTOMER_MST_PK" & vbCrLf)
        //                strQuery.Append("   AND JCSE.BOOKING_SEA_FK=BST.BOOKING_SEA_PK" & vbCrLf)
        //                strQuery.Append("   AND JCSE.HBL_EXP_TBL_FK=HBL.HBL_EXP_TBL_PK(+)" & vbCrLf)
        //                strQuery.Append("   AND JCSE.MBL_EXP_TBL_FK=MBL.MBL_EXP_TBL_PK(+)" & vbCrLf)
        //                strQuery.Append("   AND CIT.CONSOL_INVOICE_PK=" & invPk)
        //                strQuery.Append("   and cit.business_type=" & BizType)
        //                strQuery.Append("   and cit.process_type=" & ProType)

        //                Return objWF.GetDataSet(strQuery.ToString)

        //            Catch exp As Exception
        //                ErrorMessage = exp.Message
        //                Throw exp
        //            End Try



        //        End Function

        //#End Region
        //#Region "Fetch Print Sub Report"
        //        Public Function Fetch_Sub_print(ByVal invPk As Integer, ByVal BizType As Integer, ByVal ProType As Integer) As DataSet
        //            Dim strQuery As New System.Text.StringBuilder
        //            Dim objWF As New WorkFlow

        //            Try
        //                strQuery.Append("SELECT CITT.CONSOL_INVOICE_FK," & vbCrLf)
        //                strQuery.Append("       JCSE.JOBCARD_REF_NO," & vbCrLf)
        //                strQuery.Append("       FEMT.FREIGHT_ELEMENT_NAME," & vbCrLf)
        //                strQuery.Append("       CTMT.CURRENCY_ID," & vbCrLf)
        //                strQuery.Append("       CITT.ELEMENT_AMT," & vbCrLf)
        //                strQuery.Append("       CITT.EXCHANGE_RATE," & vbCrLf)
        //                strQuery.Append("       CITT.TOT_AMT" & vbCrLf)
        //                strQuery.Append("  FROM CONSOL_INVOICE_TRN_TBL  CITT," & vbCrLf)
        //                strQuery.Append("       consol_invoice_tbl cit, " & vbCrLf)
        //                strQuery.Append("       FREIGHT_ELEMENT_MST_TBL FEMT," & vbCrLf)
        //                strQuery.Append("       CURRENCY_TYPE_MST_TBL   CTMT," & vbCrLf)
        //                strQuery.Append("       JOB_CARD_TRN JCSE" & vbCrLf)
        //                strQuery.Append(" WHERE CITT.CONSOL_INVOICE_FK = " & invPk)
        //                strQuery.Append("   AND CITT.FRT_OTH_ELEMENT_FK = FEMT.FREIGHT_ELEMENT_MST_PK" & vbCrLf)
        //                strQuery.Append("   AND CITT.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK" & vbCrLf)
        //                strQuery.Append("   AND CITT.JOB_CARD_FK= JCSE.JOB_CARD_TRN_PK(+)" & vbCrLf)
        //                strQuery.Append("   and citt.consol_invoice_fk=cit.consol_invoice_pk" & vbCrLf)
        //                '  strQuery.Append("   AND CIT.CONSOL_INVOICE_PK=" & invPk)
        //                strQuery.Append("   and cit.business_type=" & BizType)
        //                strQuery.Append("   and cit.process_type=" & ProType)
        //                strQuery.Append("   " & vbCrLf)

        //                Return objWF.GetDataSet(strQuery.ToString)

        //            Catch exp As Exception
        //                ErrorMessage = exp.Message
        //                Throw exp
        //            End Try
        //        End Function
        //#End Region
        // ADDED BY JITENDRA

        #region "Fetch Job Card Sea/Air Details For Report"
        public DataSet FetchJobCardSeaDetails(string jobcardpk, int process)
        {
            // created by thiyagarajan
            StringBuilder Strsql = new StringBuilder();
            WorkFlow Objwk = new WorkFlow();
            if (process == 2)
            {
                Strsql.Append(" SELECT JSI.JOB_CARD_TRN_PK AS JOBCARDPK,");
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
                Strsql.Append(" SHIPPERCUSTDTLS.ADM_FAX_NO SHIPPERFAX,");
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
                Strsql.Append(" from JOB_CARD_TRN JSI,");
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
                Strsql.Append(" AND   JTSC.JOB_CARD_SEA_IMP_FK(+)=JSI.JOB_CARD_TRN_PK");
                Strsql.Append(" AND   STMST.SHIPPING_TERMS_MST_PK(+)=JSI.SHIPPING_TERMS_MST_FK");
                Strsql.Append(" AND   CONSCOUNTRY.COUNTRY_MST_PK(+)=CONSIGCUSTDTLS.ADM_COUNTRY_MST_FK");
                Strsql.Append(" AND   SHIPCOUNTRY.COUNTRY_MST_PK(+)=SHIPPERCUSTDTLS.ADM_COUNTRY_MST_FK ");
                Strsql.Append(" AND   AGENTMST.AGENT_MST_PK(+)=JSI.POL_AGENT_MST_FK");
                Strsql.Append(" AND   AGENTDTLS.AGENT_MST_FK(+)=AGENTMST.AGENT_MST_PK");
                Strsql.Append(" AND   AGENTCOUNTRY.COUNTRY_MST_PK(+)=AGENTDTLS.ADM_COUNTRY_MST_FK");
                Strsql.Append(" AND   DELMST.PLACE_PK(+)=JSI.DEL_PLACE_MST_FK");
                Strsql.Append(" AND CTMST.COMMODITY_GROUP_PK(+)=JSI.COMMODITY_GROUP_FK");
                Strsql.Append(" AND JTSIT.JOB_CARD_SEA_IMP_FK(+)=JSI.JOB_CARD_TRN_PK");
                Strsql.Append(" AND  nvl(JTSIT.ETA_DATE,sysdate)=(SELECT  nvl(MAX(JTT.ETA_DATE),sysdate) FROM JOB_TRN_SEA_IMP_TP JTT WHERE JTT.JOB_CARD_SEA_IMP_FK=JTSIT.JOB_CARD_SEA_IMP_FK)");
                Strsql.Append(" AND JSI.JOB_CARD_TRN_PK IN (" + jobcardpk + ")");
                Strsql.Append(" GROUP BY JSI.JOB_CARD_TRN_PK,");
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

                Strsql.Append("(select sum(jstc.gross_weight) from job_trn_sea_exp_cont jstc where jstc.job_card_sea_exp_fk=job.JOB_CARD_TRN_PK) GROSSWEIGHT,");

                Strsql.Append(" (select sum(jstc.volume_in_cbm) from job_trn_sea_exp_cont jstc where jstc.job_card_sea_exp_fk=job.JOB_CARD_TRN_PK) VOLUME,");
                Strsql.Append(" (select sum(jstc.chargeable_weight) from job_trn_sea_exp_cont jstc where jstc.job_card_sea_exp_fk=job.JOB_CARD_TRN_PK) CHARWT ");

                Strsql.Append(" from JOB_CARD_TRN job ,booking_mst_tbl booking, customer_contact_dtls consg");
                Strsql.Append(" where job.JOB_CARD_TRN_PK in (" + jobcardpk + ")");
                Strsql.Append(" and job.booking_sea_fk=booking.booking_sea_pk");
                Strsql.Append(" and job.consignee_cust_mst_fk=consg.customer_mst_fk");
            }
            try
            {
                return Objwk.GetDataSet(Strsql.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }
        #endregion

        #region "FetchJobCardAirDetails"
        //created by thiyagarjan
        public DataSet FetchJobCardAirDetails(string JobCardPK, int process)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            if (process == 2)
            {
                Strsql = "select JAI.JOB_CARD_AIR_IMP_PK AS JOBCARDPK,";
                Strsql +=  "JAI.JOBCARD_REF_NO JOBCARDNO,";
                Strsql +=  "JAI.UCR_NO AS UCRNO,";
                Strsql +=  "CONSIGCUST.CUSTOMER_NAME CONSIGNAME,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_ADDRESS_1 CONSIGADD1,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_ADDRESS_2 CONSIGADD2,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_ADDRESS_3 CONSIGADD3,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_ZIP_CODE CONSIGZIP,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_PHONE_NO_1 CONSIGPHONE,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_CITY CONSIGCITY,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_FAX_NO CONFAX,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_EMAIL_ID CONEMAIL,";
                Strsql +=  "CONSCOUNTRY.COUNTRY_NAME CONSCOUNTRY,";
                Strsql +=  "SHIPPERCUST.CUSTOMER_NAME SHIPPERNAME,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_ADDRESS_1 SHIPPERADD1,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_ADDRESS_2 SHIPPERADD2,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_ADDRESS_3 SHIPPERADD3,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_ZIP_CODE SHIPPERZIP,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_CITY SHIPPERCITY,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_FAX_NO SHIPPERFAX,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_EMAIL_ID SHIPPEREMAIL,";
                Strsql +=  "SHIPCOUNTRY.COUNTRY_NAME SHIPCOUNTRY,";
                Strsql +=  "AGENTMST.AGENT_NAME AGENTNAME,";
                Strsql +=  "AGENTDTLS.ADM_ADDRESS_1 AGENTADD1,";
                Strsql +=  "AGENTDTLS.ADM_ADDRESS_2 AGENTADD2,";
                Strsql +=  "AGENTDTLS.ADM_ADDRESS_3 AGENTADD3,";
                Strsql +=  "AGENTDTLS.ADM_CITY      AGENTCITY,";
                Strsql +=  "AGENTDTLS.ADM_ZIP_CODE  AGENTZIP,";
                Strsql +=  "AGENTDTLS.ADM_PHONE_NO_1 AGENTPHONE,";
                Strsql +=  "AGENTDTLS.ADM_FAX_NO    AGENTFAX,";
                Strsql +=  "AGENTDTLS.ADM_EMAIL_ID  AGENTEMAIL,";
                Strsql +=  "AGENTCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,";
                Strsql +=  "JAI.FLIGHT_NO  VES_VOY,";
                Strsql +=  "CGMST.COMMODITY_GROUP_DESC COMMTYPE,";
                Strsql +=  "(CASE WHEN JAI.HAWB_REF_NO IS NOT NULL THEN";
                Strsql +=  "JAI.HAWB_REF_NO";
                Strsql +=  " ELSE";
                Strsql +=  "JAI.MAWB_REF_NO END ) BLREFNO,";

                //modified by thiyagarjan

                //Strsql &= vbCrLf & "POL.PORT_NAME POLNAME,"
                //Strsql &= vbCrLf & "POD.PORT_NAME PODNAME,"

                Strsql += "(POL.PORT_ID || ','|| (select cont.country_name from country_mst_tbl cont where cont.country_mst_pk=POL.country_mst_fk)) POLNAME,";
                Strsql += "(POD.PORT_ID || ','|| (select cont1.country_name from country_mst_tbl cont1 where cont1.country_mst_pk=POD.country_mst_fk)) PODNAME,";


                Strsql +=  "DELMST.PLACE_NAME DEL_PLACE_NAME,";
                Strsql +=  "JAI.GOODS_DESCRIPTION,";
                Strsql +=  "JAI.ETD_DATE ETD,";
                Strsql +=  "JAI.ETA_DATE ETA,";
                Strsql +=  "JAI.CLEARANCE_ADDRESS CLEARANCEPOINT,";
                Strsql +=  "JAI.MARKS_NUMBERS MARKS,";
                Strsql +=  "STMST.INCO_CODE TERMS,";
                Strsql +=  "NVL(JAI.INSURANCE_AMT, 0) INSURANCE,";
                Strsql +=  "JAI.PYMT_TYPE PYMT_TYPE,";
                Strsql +=  "2 CARGO_TYPE,";
                Strsql +=  "SUM(JTSC.GROSS_WEIGHT) GROSSWEIGHT,";
                Strsql +=  " '' NETWEIGHT,";
                Strsql +=  " SUM(JTSC.CHARGEABLE_WEIGHT) CHARWT,";
                Strsql +=  "SUM(JTSC.VOLUME_IN_CBM) VOLUME";
                // Strsql &= vbCrLf & "MAX(JTAIT.ETA_DATE) ETA"
                Strsql +=  "from JOB_CARD_AIR_IMP_TBL JAI,";
                Strsql +=  "JOB_TRN_AIR_IMP_TP JTAIT,";
                Strsql +=  "CUSTOMER_MST_TBL CONSIGCUST,";
                Strsql +=  "CUSTOMER_CONTACT_DTLS CONSIGCUSTDTLS,";
                Strsql +=  "CUSTOMER_CONTACT_DTLS SHIPPERCUSTDTLS,";
                Strsql +=  "CUSTOMER_MST_TBL SHIPPERCUST,";
                Strsql +=  "AGENT_MST_TBL AGENTMST,";
                Strsql +=  "AGENT_CONTACT_DTLS AGENTDTLS,";
                Strsql +=  "PORT_MST_TBL POL,";
                Strsql +=  "PORT_MST_TBL POD,";
                Strsql +=  "JOB_TRN_AIR_IMP_CONT JTSC,";
                Strsql +=  "SHIPPING_TERMS_MST_TBL STMST,";
                Strsql +=  "COUNTRY_MST_TBL SHIPCOUNTRY,";
                Strsql +=  "COUNTRY_MST_TBL CONSCOUNTRY,";
                Strsql +=  "COUNTRY_MST_TBL AGENTCOUNTRY,";
                Strsql +=  "PLACE_MST_TBL DELMST,";
                Strsql +=  "COMMODITY_GROUP_MST_TBL CGMST";
                Strsql +=  "WHERE CONSIGCUST.CUSTOMER_MST_PK(+) = JAI.CONSIGNEE_CUST_MST_FK";
                Strsql +=  " AND   JTAIT.JOB_CARD_AIR_IMP_FK(+)= JAI.JOB_CARD_AIR_IMP_PK";
                Strsql +=  "AND  nvl(JTAIT.ETA_DATE,sysdate)=(SELECT  nvl(MAX(JTT.ETA_DATE),sysdate) FROM JOB_TRN_AIR_IMP_TP JTT WHERE JTT.JOB_CARD_AIR_IMP_FK=JTAIT.JOB_CARD_AIR_IMP_FK)";
                Strsql +=  "AND   SHIPPERCUST.CUSTOMER_MST_PK(+)=JAI.SHIPPER_CUST_MST_FK";
                Strsql +=  "AND   CONSIGCUSTDTLS.CUSTOMER_MST_FK(+)=CONSIGCUST.CUSTOMER_MST_PK";
                Strsql +=  "AND   SHIPPERCUSTDTLS.CUSTOMER_MST_FK(+)=SHIPPERCUST.CUSTOMER_MST_PK";
                Strsql +=  "AND   POL.PORT_MST_PK(+)=JAI.PORT_MST_POL_FK";
                Strsql +=  "AND   POD.PORT_MST_PK(+)=JAI.PORT_MST_POD_FK";
                Strsql +=  "AND   JTSC.JOB_CARD_AIR_IMP_FK(+)=JAI.JOB_CARD_AIR_IMP_PK";
                Strsql +=  "AND   STMST.SHIPPING_TERMS_MST_PK(+)=JAI.SHIPPING_TERMS_MST_FK";
                Strsql +=  "AND   CONSCOUNTRY.COUNTRY_MST_PK(+)=CONSIGCUSTDTLS.ADM_COUNTRY_MST_FK";
                Strsql +=  "AND   SHIPCOUNTRY.COUNTRY_MST_PK(+)=SHIPPERCUSTDTLS.ADM_COUNTRY_MST_FK ";
                Strsql +=  "AND   AGENTMST.AGENT_MST_PK(+)=JAI.POL_AGENT_MST_FK";
                Strsql +=  "AND   AGENTDTLS.AGENT_MST_FK(+)=AGENTMST.AGENT_MST_PK";
                Strsql +=  "AND   AGENTCOUNTRY.COUNTRY_MST_PK(+)=AGENTDTLS.ADM_COUNTRY_MST_FK";
                Strsql +=  "AND   DELMST.PLACE_PK(+)=JAI.DEL_PLACE_MST_FK";
                Strsql +=  "AND   CGMST.COMMODITY_GROUP_PK(+)=JAI.COMMODITY_GROUP_FK";
                Strsql +=  "AND   JAI.JOB_CARD_AIR_IMP_PK IN (" + JobCardPK + ")";
                Strsql +=  "GROUP BY JAI.JOB_CARD_AIR_IMP_PK,";
                Strsql +=  "JAI.JOBCARD_REF_NO ,";
                Strsql +=  "JAI.UCR_NO  ,";
                Strsql +=  "CONSIGCUST.CUSTOMER_NAME ,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_ADDRESS_1 ,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_ADDRESS_2 ,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_ADDRESS_3 ,";
                Strsql +=  " CONSIGCUSTDTLS.ADM_ZIP_CODE ,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_PHONE_NO_1 ,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_CITY ,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_FAX_NO ,";
                Strsql +=  "CONSIGCUSTDTLS.ADM_EMAIL_ID ,";
                Strsql +=  "CONSCOUNTRY.COUNTRY_NAME ,";
                Strsql +=  "SHIPPERCUST.CUSTOMER_NAME ,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_ADDRESS_1 ,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_ADDRESS_2 ,";
                Strsql +=  " SHIPPERCUSTDTLS.ADM_ADDRESS_3 ,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_ZIP_CODE ,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_PHONE_NO_1 ,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_CITY ,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_FAX_NO ,";
                Strsql +=  "SHIPPERCUSTDTLS.ADM_EMAIL_ID ,";
                Strsql +=  "SHIPCOUNTRY.COUNTRY_NAME,";
                Strsql +=  "AGENTMST.AGENT_NAME ,";
                Strsql +=  "AGENTDTLS.ADM_ADDRESS_1,";
                Strsql +=  "AGENTDTLS.ADM_ADDRESS_2 ,";
                Strsql +=  "AGENTDTLS.ADM_ADDRESS_3,";
                Strsql +=  "AGENTDTLS.ADM_CITY,";
                Strsql +=  "AGENTDTLS.ADM_ZIP_CODE ,";
                Strsql +=  " AGENTDTLS.ADM_PHONE_NO_1,";
                Strsql +=  " AGENTDTLS.ADM_FAX_NO ,";
                Strsql +=  " AGENTDTLS.ADM_EMAIL_ID,";
                Strsql +=  "AGENTCOUNTRY.COUNTRY_NAME,";
                Strsql +=  "JAI.FLIGHT_NO ,";
                Strsql +=  "CGMST.COMMODITY_GROUP_DESC,";
                Strsql +=  "(CASE WHEN JAI.HAWB_REF_NO IS NOT NULL THEN";
                Strsql +=  " JAI.HAWB_REF_NO";
                Strsql +=  " ELSE";
                Strsql +=  " JAI.MAWB_REF_NO END ) ,";

                Strsql +=  " POL.PORT_ID,POL.COUNTRY_MST_FK,";
                Strsql +=  " POD.PORT_ID,POD.COUNTRY_MST_FK,";

                Strsql +=  "DELMST.PLACE_NAME ,";
                Strsql +=  "JAI.GOODS_DESCRIPTION,";
                Strsql +=  "JAI.ETD_DATE ,";
                Strsql +=  "JAI.ETA_DATE ,";
                Strsql +=  "JAI.CLEARANCE_ADDRESS,";
                Strsql +=  "JAI.MARKS_NUMBERS ,";
                Strsql +=  "STMST.INCO_CODE,";
                Strsql +=  " NVL(JAI.INSURANCE_AMT,0),";
                Strsql +=  "JAI.PYMT_TYPE";

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
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        #endregion

        #region "Fetch Consol invoice Custumer "
        public DataSet CONSOL_INV_CUST_PRINT(int Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with5 = objWK.MyCommand.Parameters;
                _with5.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with5.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with5.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with5.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_INV_CUST_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion



        #region "Fetch Consol invoice Details Report "
        public DataSet CONSOL_INV_DETAIL_MAIN_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with6 = objWK.MyCommand.Parameters;
                _with6.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with6.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with6.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with6.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_DETAILS_MAIN_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion




        #region "Fetch Consol invoice Sub Details Report "
        public DataSet CONSOL_INV_DETAIL_SUB_MAIN_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with7 = objWK.MyCommand.Parameters;
                _with7.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with7.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with7.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with7.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_SUB_MAIN_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet CONSOL_INV_DETAIL_AIF_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with8 = objWK.MyCommand.Parameters;
                _with8.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with8.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with8.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with8.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_SUB_AIF_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion



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
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Location"

        public DataSet FetchLocation(long Loc)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
            StrSqlBuilder.Append("  COP.GST_NO,COP.COMPANY_REG_NO,COP.HOME_PAGE URL, ");
            StrSqlBuilder.Append("  L.LOCATION_ID , L.LOCATION_NAME, ");
            StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,L.E_MAIL_ID,");
            StrSqlBuilder.Append("  L.CITY,CMST.COUNTRY_NAME COUNTRY,L.ZIP, L.LOCATION_MST_PK ,L.VAT_NO");
            StrSqlBuilder.Append("  FROM CORPORATE_MST_TBL COP,LOCATION_MST_TBL L,COUNTRY_MST_TBL CMST");
            StrSqlBuilder.Append("  WHERE CMST.COUNTRY_MST_PK(+)=L.COUNTRY_MST_FK AND L.LOCATION_MST_PK = " + Loc + "");

            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchLocationNew(long USERPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
            StrSqlBuilder.Append("  COP.GST_NO,COP.COMPANY_REG_NO,COP.HOME_PAGE URL, ");
            StrSqlBuilder.Append("  L.LOCATION_ID , L.LOCATION_NAME, ");
            StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,EMT.EMAIL_ID E_MAIL_ID,");
            StrSqlBuilder.Append("  L.CITY,CMST.COUNTRY_NAME COUNTRY,L.ZIP, L.LOCATION_MST_PK");
            StrSqlBuilder.Append("  FROM CORPORATE_MST_TBL COP,LOCATION_MST_TBL L,COUNTRY_MST_TBL CMST,EMPLOYEE_MST_TBL  EMT,USER_MST_TBL      UMT");
            StrSqlBuilder.Append("  WHERE CMST.COUNTRY_MST_PK(+)=L.COUNTRY_MST_FK AND UMT.USER_MST_PK = " + USERPK + "");
            StrSqlBuilder.Append("  AND EMT.LOCATION_MST_FK = L.LOCATION_MST_PK(+)");
            StrSqlBuilder.Append("  AND UMT.EMPLOYEE_MST_FK=EMT.EMPLOYEE_MST_PK");

            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchBank()
        {
            WorkFlow ObjWk = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                ObjWk.OpenConnection();
                ObjWk.MyCommand.Connection = ObjWk.MyConnection;
                var _with10 = ObjWk.MyCommand;
                _with10.CommandType = CommandType.StoredProcedure;
                _with10.CommandText = ObjWk.MyUserName + ".CONSOL_INV_PKG.FETCH_BANK";
                ObjWk.MyCommand.Parameters.Clear();
                var _with11 = ObjWk.MyCommand.Parameters;
                _with11.Add("LOCATION_PK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with11.Add("BANK_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                ObjWk.MyDataAdapter.SelectCommand = ObjWk.MyCommand;
                ObjWk.MyDataAdapter.Fill(dsData);
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


        #region "Fetch Function for Bank"
        public DataSet BankDetails(Int64 BankPK)
        {
            //Dim Strsql As String
            //Strsql = String.Empty & vbCrLf
            //Strsql &= "(SELECT " & vbCrLf
            //Strsql &= "LOCBANK.LOCATION_BANK_PK LOCBANKPK, " & vbCrLf
            //Strsql &= "LOCBANK.VERSION_NO VERNO, " & vbCrLf
            //Strsql &= "LOCBANK.ACCOUNT_NO, " & vbCrLf
            //Strsql &= "LOCBANK.SWIFT_CODE, " & vbCrLf
            //Strsql &= "LOCBANK.BANK_GERO_NO, " & vbCrLf
            //Strsql &= "LOCBANK.IBAN," & vbCrLf
            //Strsql &= "LOCBANK.BRANCH," & vbCrLf
            //Strsql &= "LOCBANK.E_BANK_CODE," & vbCrLf
            //Strsql &= "LOCBANK.LOC_NAME," & vbCrLf
            //Strsql &= "LOCBANK.COUNTRY," & vbCrLf
            //Strsql &= "LOCBANK.BANK_NAME," & vbCrLf
            //Strsql &= "LOCBANK.BANK_ADDRESS," & vbCrLf
            //Strsql &= "LOC.OFFICE_NAME" & vbCrLf
            //Strsql &= "FROM " & vbCrLf
            //Strsql &= "LOCATION_BANK_TRN LOCBANK, " & vbCrLf
            //Strsql &= "Location_Mst_Tbl LOC " & vbCrLf
            //Strsql &= "WHERE  " & vbCrLf
            //Strsql &= "LOCBANK.Location_Mst_Fk = Loc.location_mst_pk " & vbCrLf
            //Strsql &= "AND LOCBANK.Location_Mst_Fk = " & P_Location_Mst_Fk & ")  " & vbCrLf

            //Strsql &= " " & vbCrLf
            //Dim objWF As New WorkFlow
            //Try
            //    Return objWF.GetDataSet(Strsql)
            //Catch sqlExp As OracleException
            //    ErrorMessage = sqlExp.Message
            //    Throw sqlExp
            //Catch exp As Exception
            //    ErrorMessage = exp.Message
            //    Throw exp
            //End Try
            WorkFlow ObjWk = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                ObjWk.OpenConnection();
                ObjWk.MyCommand.Connection = ObjWk.MyConnection;
                var _with12 = ObjWk.MyCommand;
                _with12.CommandType = CommandType.StoredProcedure;
                _with12.CommandText = ObjWk.MyUserName + ".CONSOL_INV_PKG.FETCH_BANK_REPORT";
                ObjWk.MyCommand.Parameters.Clear();
                var _with13 = ObjWk.MyCommand.Parameters;
                _with13.Add("BANK_PK_IN", BankPK).Direction = ParameterDirection.Input;
                _with13.Add("BANK_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                ObjWk.MyDataAdapter.SelectCommand = ObjWk.MyCommand;
                ObjWk.MyDataAdapter.Fill(dsData);
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

        // Addded by jitendra 


        #region "Fetch Barcode Manager Pk"
        public int FetchBarCodeManagerPk(int BizType, int ProcType)
        {
            string StrSql = null;
            DataSet DsBarManager = null;
            int strReturn = 0;
            StringBuilder strquery = new StringBuilder();
            try
            {
                WorkFlow objWF = new WorkFlow();
                //StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt where bdmt.config_id_fk='" & Configid & " '"

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
                    var _with14 = DsBarManager.Tables[0].Rows[0];
                    strReturn = Convert.ToInt32(_with14["bcd_mst_pk"]);
                }
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion


        #region "Fetch Barcode Type"
        public DataSet FetchBarCodeField(int BarCodeManagerPk)
        {
            string StrSql = null;
            DataSet DsBarManager = null;
            int strReturn = 0;
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();

            strQuery.Append("select distinct bdmt.bcd_mst_pk, bdmt.field_name, bdmt.default_value");
            strQuery.Append("  from barcode_data_mst_tbl bdmt, barcode_doc_data_tbl bddt");
            strQuery.Append(" where bdmt.bcd_mst_pk = bddt.bcd_mst_fk(+)");
            strQuery.Append("   and bdmt.BCD_MST_FK= " + BarCodeManagerPk);
            strQuery.Append(" ORDER BY default_value desc");

            // StrSql = "select bdmt.bcd_mst_pk, bdmt.field_name ,bdmt.default_value from barcode_data_mst_tbl bdmt, barcode_doc_data_tbl bddt where bdmt.bcd_mst_pk=bddt.bcd_mst_fk and bdmt.BCD_MST_FK=" & BarCodeManagerPk
            try
            {
                DsBarManager = objWF.GetDataSet(strQuery.ToString());
                return DsBarManager;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion
        #region "Fetch Cus pk only"




        #endregion
        //Public Function Fetch_CustmerPk(ByVal jobPk As String) As DataSet

        //    Dim ObjWF As New WorkFlow
        //    Dim strQuery As New StringBuilder
        //    Try
        //        strQuery.Append(" select distinct j.frtpayer_cust_mst_fk  from job_trn_sea_exp_fd J WHERE J.JOB_CARD_SEA_EXP_FK IN ( " & jobPk.TrimEnd(CChar(",")) & " )")

        //        Return ObjWF.GetDataSet(strQuery.ToString())

        //    Catch ex As Exception
        //        Throw ex
        //    End Try

        //End Function

        #region "Fetch Custumer pk And Jobcadpk "
        public DataSet Fetch_CustumerPk(int ConsPk)
        {
            try
            {
                DataSet DsCustPk = null;
                int strReturn = 0;
                WorkFlow objWF = new WorkFlow();
                StringBuilder strQuery = new StringBuilder();
                strQuery.Append("select distinct vb.job_card_fk, cv.customer_mst_fk, cv.CHK_INVOICE,VB.JOB_TYPE from consol_invoice_trn_tbl vb ,consol_invoice_tbl cv  ");
                strQuery.Append("where vb.consol_invoice_fk=" + ConsPk);
                strQuery.Append("and vb.consol_invoice_fk=cv.consol_invoice_pk");
                DsCustPk = objWF.GetDataSet(strQuery.ToString());
                return DsCustPk;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchSuppPk(int ConsPk)
        {
            try
            {
                DataSet DsCustPk = null;
                int strReturn = 0;
                WorkFlow objWF = new WorkFlow();
                StringBuilder strQuery = new StringBuilder();
                strQuery.Append("select distinct CV.SUPPLIER_MST_FK, cv.CHK_INVOICE from consol_invoice_trn_tbl vb ,consol_invoice_tbl cv  ");
                strQuery.Append("where vb.consol_invoice_fk=" + ConsPk);
                strQuery.Append("and vb.consol_invoice_fk=cv.consol_invoice_pk");
                DsCustPk = objWF.GetDataSet(strQuery.ToString());
                return DsCustPk;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion


        // For Enhance Search -- Jitendra 


        #region "Enhance Job Card Against Freight Payer  EXP"
        public string Enhance_JobCard_FreightPayer_EXP(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            string[] arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strProcessType = null;
            string strCustPk = null;

            string strloc = "";

            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strCustPk = arr[4];
            //strProcessType = arr(3)
            //strloc = arr(4)
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBCARD_FRTPAYER_PKG.GET_JOB_FRTPAYER_EXP";

                var _with15 = selectCommand.Parameters;
                _with15.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with15.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with15.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with15.Add("LOCATION_IN", loc).Direction = ParameterDirection.Input;
                _with15.Add("CUSTOMER_PK_IN", (!string.IsNullOrEmpty(strCustPk.Trim()) ? strCustPk : "")).Direction = ParameterDirection.Input;
                _with15.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Original;
                selectCommand.ExecuteNonQuery();
                //Manoharan 27July07: to convert Clob object to String
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                char[] charbuff = null;
                strReturn = strReader.ReadToEnd();
                //end
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
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

        #region "Enhance Job Card Against Freight Payer IMP"
        public string Enhance_JobCard_FreightPayer_IMP(string strCond, string loc = "0")
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
            string strCustPk = null;

            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            strBizType = arr[2];
            strCustPk = arr[4];
            //strloc = arr(4)

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOBCARD_FRTPAYER_PKG.GET_JOB_FRTPAYER_IMP";

                var _with16 = selectCommand.Parameters;
                _with16.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with16.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with16.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with16.Add("LOCATION_IN", loc).Direction = ParameterDirection.Input;
                _with16.Add("CUSTOMER_PK_IN", (!string.IsNullOrEmpty(strCustPk) ? strCustPk : "")).Direction = ParameterDirection.Input;

                _with16.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Original;

                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                char[] charbuff = null;
                strReturn = strReader.ReadToEnd();
                // strReturn = CStr(selectCommand.Parameters("RETURN_VALUE").Value)
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
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

        #region "Fetch Job Card Against Freight Payer  CUSTUMER "
        public string Enhance_Custumer_FreightPayer(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            string[] arr = null;
            string strSERACH_IN = "";
            string strCATEGORY_IN = "";
            string businessType = "";
            string Import = "";
            string Consignee = "0";
            string strLOC_MST_IN = "";
            string strReq = null;
            string strJobPk = null;
            string strProcess = "";

            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSERACH_IN = arr[1];
            if (arr.Length > 2)
                strCATEGORY_IN = arr[2];
            if (arr.Length > 3)
                strLOC_MST_IN = arr[3];
            if (arr.Length > 4)
                businessType = arr[4];
            //new condition added by vimlesh kumar for checking consignee
            //in place of location we need to pass pod pk 
            //this condition gives the consignee of pod location.
            if (arr.Length > 5)
                Consignee = "1";
            //added by gopi in import side we need show all shippper
            if (arr.Length > 6)
                Import = arr[6];
            if (arr.Length > 7)
                strJobPk = arr[7];
            if (arr.Length > 8)
                strProcess = arr[8];


            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOBCARD_FRTPAYER_PKG.GETFRTPAYER_JOB";
                var _with17 = SCM.Parameters;
                _with17.Add("CATEGORY_IN", (!string.IsNullOrEmpty(strCATEGORY_IN) ? strCATEGORY_IN : "")).Direction = ParameterDirection.Input;
                _with17.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with17.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with17.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with17.Add("CONSIGNEE_IN", Consignee).Direction = ParameterDirection.Input;
                _with17.Add("BIZ_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with17.Add("IMPORT_IN", (!string.IsNullOrEmpty(Import.Trim()) ? Import : "")).Direction = ParameterDirection.Input;
                _with17.Add("PROCESS_TYPE_IN", strProcess).Direction = ParameterDirection.Input;
                _with17.Add("JOB_CARD_PK_IN", (!string.IsNullOrEmpty(strJobPk) ? strJobPk : "")).Direction = ParameterDirection.Input;
                _with17.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
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


        #region "fetch_Cutumer_pk "
        public DataSet fetch_Cust_pk(string pk, string hblpk, string process, string biztype)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                if ((pk != "0") & (!string.IsNullOrEmpty(pk)))
                {
                    strQuery.Append("select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name");
                    if (process == "1" & biztype == "1")
                    {
                        strQuery.Append("  from job_card_air_exp_tbl j, customer_mst_tbl cmt");
                        strQuery.Append("    where j.job_card_air_exp_pk = " + pk);
                        strQuery.Append(" and j.shipper_cust_mst_fk = cmt.customer_mst_pk");
                    }
                    if (process == "2" & biztype == "1")
                    {
                        strQuery.Append("  from job_card_air_imp_tbl j, customer_mst_tbl cmt");
                        strQuery.Append("    where j.job_card_air_imp_pk = " + pk);
                        strQuery.Append(" and j.consignee_cust_mst_fk = cmt.customer_mst_pk");
                    }
                    if (process == "1" & biztype == "2")
                    {
                        strQuery.Append("  from JOB_CARD_TRN j, customer_mst_tbl cmt");
                        strQuery.Append("    where j.JOB_CARD_TRN_PK = " + pk);
                        strQuery.Append(" and j.shipper_cust_mst_fk = cmt.customer_mst_pk");
                    }
                    if (process == "2" & biztype == "2")
                    {
                        strQuery.Append("  from JOB_CARD_TRN j, customer_mst_tbl cmt");
                        strQuery.Append("   where j.JOB_CARD_TRN_PK = " + pk);
                        strQuery.Append(" and j.consignee_cust_mst_fk = cmt.customer_mst_pk");
                    }
                    strQuery.Append("");
                }
                else
                {
                    if ((hblpk != "0") & (!string.IsNullOrEmpty(hblpk)))
                    {
                        if (process == "1" & biztype == "1")
                        {
                            strQuery.Append(" select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name");
                            strQuery.Append(" from customer_mst_tbl cmt, hawb_exp_tbl HET ");
                            strQuery.Append(" WHERE   HET.HAWB_EXP_TBL_PK  =" + hblpk);
                            strQuery.Append(" and HET.shipper_cust_mst_fk = cmt.customer_mst_pk");
                        }
                        if (process == "1" & biztype == "2")
                        {
                            strQuery.Append(" select cmt.customer_mst_pk, cmt.customer_id, cmt.customer_name");
                            strQuery.Append(" from customer_mst_tbl cmt, HBL_EXP_TBL HBE");
                            strQuery.Append(" WHERE HBE.HBL_EXP_TBL_PK =" + hblpk);
                            strQuery.Append(" and hbe.shipper_cust_mst_fk = cmt.customer_mst_pk");

                        }
                    }

                }
                if ((!string.IsNullOrEmpty(strQuery.ToString())))
                {
                    return objWF.GetDataSet(strQuery.ToString());
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
            return new DataSet();
        }
        #endregion

        #region "Export to XML"
        public DataSet Export2XML(string InvPK = "0", string PayDueDt = "", int IsFACInv = 0)
        {

            WorkFlow objWF = new WorkFlow();
            DataTable dtInv = null;
            DataTable dtInvdet = null;
            DataTable dtJcHead = null;
            DataTable dtJcDet = null;
            DataSet MainDs = new DataSet();

            try
            {
                if (IsFACInv == 1)
                {
                    dtInv = getInvHeader(InvPK, PayDueDt, IsFACInv);
                    dtInvdet = getInvDetailsFAC(InvPK);
                    dtJcHead = getJcHeader(InvPK, IsFACInv);
                    dtJcDet = getJcDetailsFAC(InvPK);
                }
                else
                {
                    dtInv = getInvHeader(InvPK, PayDueDt, IsFACInv);
                    dtInvdet = getInvDetails(InvPK);
                    dtJcHead = getJcHeader(InvPK);
                    dtJcDet = getJcDetails(InvPK);
                }

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

        public DataTable getInvHeader(string InvPK = "0", string dueDate = "", int IsFACInv = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder();

            sb.Append("select distinct inv.consol_invoice_pk INVPK,");
            sb.Append("                INVtr.Job_Card_Fk JCPK,");
            sb.Append("                inv.invoice_ref_no invoice_nr,inv.INV_UNIQUE_REF_NR ref_nr,");
            sb.Append("                to_char(inv.invoice_date, 'dd-MON-yyyy') invoice_date,");
            sb.Append("                to_char(nvl(jcse.departure_date, jcse.etd_date), 'dd-MON-yyyy') activity_date,");
            //sb.Append("                to_char(inv.invoice_date, 'dd-MON-yyyy') invoice_actual_due_date,")
            //sb.Append("                to_char(inv.invoice_date, 'dd-MON-yyyy') invoice_adjusted_due_date,")
            //Snigdharani - 02/03/2009
            sb.Append("to_char(to_date('" + dueDate + "', 'dd/mm/yyyy'),'dd-MON-yyyy') invoice_actual_due_date,");
            sb.Append("to_char(to_date('" + dueDate + "', 'dd/mm/yyyy'),'dd-MON-yyyy') invoice_adjusted_due_date,");
            sb.Append("                INV_UNIQUE_REF_NR unique_ref_nr,");
            //End Snigdharani
            sb.Append("                'EXP' process_type,");
            sb.Append("                'SEA' business_type,");
            sb.Append("                ' ' ocr_no,");
            if (IsFACInv == 1)
            {
                sb.Append("                 OPR.OPERATOR_ID Supplier,");
            }
            else
            {
                sb.Append("                cust.customer_id customer,");
            }
            sb.Append("                ' ' AGENT,");
            if (IsFACInv == 1)
            {
                sb.Append("                'SUPPLIER' PARTY_TYPE,");
            }
            else
            {
                sb.Append("                'CUSTOMER' PARTY_TYPE,");
            }
            sb.Append("                nvl(shmpt.cargo_move_code, ' ') shipping_terms,");
            sb.Append("                'AS PER CONTRACT' payment_terms,");
            sb.Append("                currcorp.currency_id base_currency,");
            sb.Append("                curr.currency_id invoice_currency,");
            sb.Append("                'CONTAINER' shipment,");
            sb.Append("                nvl(inv.remarks, ' ') remarks,");
            sb.Append("                jcse.vessel_name vsl,");
            sb.Append("                jcse.voyage_flight_no voyage,");
            sb.Append("                'GENERAL' roe_basis,");
            sb.Append("                'STANDARD' roe_type,");
            sb.Append("                get_ex_rate(inv.currency_mst_fk,");
            sb.Append("                            currcorp.currency_mst_pk,");
            sb.Append("                            inv.invoice_date) roe_amount,");
            sb.Append("                '0' invoice_type,");
            sb.Append("                'RECORD1' ref_nr");
            sb.Append("  from consol_invoice_tbl     inv,");
            sb.Append("       JOB_CARD_TRN      jcse,");
            sb.Append("       currency_type_mst_tbl     curr,");
            sb.Append("       currency_type_mst_tbl     currcorp,");
            sb.Append("       consol_invoice_trn_tbl invtr,");
            if (IsFACInv == 1)
            {
                sb.Append("       OPERATOR_MST_TBL          OPR,");
            }
            else
            {
                sb.Append("       CUSTOMER_MST_TBL          cust,");
            }
            sb.Append("       cargo_move_mst_tbl   shmpt,");
            sb.Append("       AGENT_MST_TBL             AGT,");
            sb.Append("       AGENT_MST_TBL             AGTDP");
            sb.Append("  where jcse.JOB_CARD_TRN_PK = invtr.job_card_fk");
            sb.Append("   and inv.currency_mst_fk = curr.currency_mst_pk");
            sb.Append("   and invtr.consol_invoice_fk = inv.consol_invoice_pk");
            if (IsFACInv == 1)
            {
                sb.Append("    AND OPR.OPERATOR_MST_PK = INV.SUPPLIER_MST_FK ");
            }
            else
            {
                sb.Append("   and cust.customer_mst_pk = inv.customer_mst_fk");
            }
            sb.Append("   and jcse.cargo_move_fk = shmpt.cargo_move_pk(+)");
            sb.Append("   and currcorp.currency_mst_pk=" + HttpContext.Current.Session["currency_mst_pk"]);

            sb.Append("   and inv.consol_invoice_pk = " + InvPK);
            sb.Append("   AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
            sb.Append("   AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
            try
            {
                return objWF.GetDataTable(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
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
            sb.Append("                        INVTRN.VAT_CODE,");
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
            sb.Append("               JOB_CARD_TRN jcse,");
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
            sb.Append("           and jcse.JOB_CARD_TRN_PK = INVTRN.JOB_CARD_FK");
            sb.Append("           AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            sb.Append("           and cntr.container_type_mst_pk(+) = jobfrt.container_type_mst_fk");
            sb.Append("           AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("           AND QRY.FREIGHT_ELEMENT_MST_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK");
            //Snigdharani - 02/03/2009
            sb.Append("           AND INVTRN.FRT_OTH_ELEMENT_FK = FMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("           AND INVTRN.FRT_OTH_ELEMENT = 1");
            //End Snigdharani
            sb.Append("           and jccntr.job_card_sea_exp_fk = jcse.JOB_CARD_TRN_PK");
            sb.Append("           AND JOBFRT.FREIGHT_TYPE IN (1, 2)");
            sb.Append("           AND JOBFRT.JOB_CARD_SEA_EXP_FK = JCSE.JOB_CARD_TRN_PK");
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
            sb.Append("                        INVTRN.VAT_CODE,");
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
            sb.Append("               JOB_CARD_TRN jcse,");
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
            sb.Append("           and jcse.JOB_CARD_TRN_PK = invTRN.Job_Card_Fk");
            sb.Append("           AND joboth.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            sb.Append("           AND QRY.FREIGHT_ELEMENT_MST_FK = JOBOTH.FREIGHT_ELEMENT_MST_FK");
            sb.Append("           AND joboth.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("           AND JOBOTH.INV_AGENT_TRN_SEA_EXP_FK IS NULL");
            sb.Append("           AND JOBOTH.INV_CUST_TRN_SEA_EXP_FK IS NULL");
            sb.Append("           AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL");
            //Snigdharani - 02/03/2009
            sb.Append("           AND INVTRN.FRT_OTH_ELEMENT_FK = FMT.FREIGHT_ELEMENT_MST_PK");
            sb.Append("           AND INVTRN.FRT_OTH_ELEMENT = 2");
            //End Snigdharani
            sb.Append("           AND JOBOTH.Freight_Type IN (1, 2)");
            sb.Append("           and jccntr.job_card_sea_exp_fk = jcse.JOB_CARD_TRN_PK");
            sb.Append("           AND JOBOTH.JOB_CARD_SEA_EXP_FK = JCSE.JOB_CARD_TRN_PK) Q");
            try
            {
                return objWF.GetDataTable(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataTable getInvDetailsFAC(string InvPK = "0", short BizType = 0, short ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder();

            sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
            sb.Append("  FROM (select distinct inv.consol_invoice_pk INVPK,");
            sb.Append("                        INVTRN.JOB_CARD_FK JCPK,");
            sb.Append("                        (SELECT CEMT.COST_ELEMENT_ID  FROM COST_ELEMENT_MST_TBL CEMT, PARAMETERS_TBL P");
            sb.Append("                        WHERE CEMT.COST_ELEMENT_MST_PK = P.FRT_FAC_FK) CHARGE_CODE,");
            sb.Append("                        (SELECT CEMT.COST_ELEMENT_NAME  FROM COST_ELEMENT_MST_TBL CEMT, PARAMETERS_TBL P");
            sb.Append("                        WHERE CEMT.COST_ELEMENT_MST_PK = P.FRT_FAC_FK) CHARGE_DESC,");
            sb.Append("                        '' CONTAINER_TYPE,");
            sb.Append("'" + HttpContext.Current.Session["CURRENCY_ID"] + "' TRANSACTION_CURRENCY,");
            sb.Append("                        '' VAT_PERCENTAGE,");
            sb.Append("                        '' VAT_TYPE,");
            sb.Append("                        '' VAT_AMOUNT,");
            sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
            sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
            sb.Append(" " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
            sb.Append("                                    inv.invoice_date) ROE,");
            sb.Append("                        'STANDARD' ROE_TYPE,");
            sb.Append("                        ROUND(INVTRN.AMT_IN_INV_CURR, 2) AMT_IN_INV_CUR,");
            sb.Append("                        ROUND(INVTRN.AMT_IN_INV_CURR *");
            sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
            sb.Append(" " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
            sb.Append("                                    inv.invoice_date),");
            sb.Append("                               2) INV_AMT_IN_BASE_CUR,");
            sb.Append("                        (CASE");
            sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
            sb.Append("                           'P'");
            sb.Append("                          ELSE");
            sb.Append("                           'C'");
            sb.Append("                        END) PREPAID_COLLECT,");
            sb.Append("                       '' CHARGE_BASIS,");
            sb.Append("                        'OPEN' COLLECT_STATUS,");
            sb.Append("                        'F' ADDITIONALCHARGES");
            sb.Append("          from consol_invoice_trn_tbl invtrn,");
            sb.Append("               consol_invoice_tbl inv,");
            sb.Append("               JOB_CARD_TRN jcse ");
            sb.Append("         where inv.consol_invoice_pk in( " + InvPK + ")");
            sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
            sb.Append("           and jcse.JOB_CARD_TRN_PK = INVTRN.JOB_CARD_FK");
            sb.Append("           AND INV.IS_FAC_INV = 1) Q");

            try
            {
                return objWF.GetDataTable(sb.ToString());
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
        public DataTable getJcHeader(string InvPK = "0", int IsFACInv = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder();

            sb.Append("SELECT distinct INV.CONSOL_INVOICE_PK INVPK,");
            sb.Append("       INVtrn.Job_Card_Fk JCPK,");
            sb.Append("       TO_CHAR(JCSE.JOBCARD_DATE,'DD-MON-YYYY') SALES_DATE,");
            sb.Append("       to_char(nvl(jcse.departure_date, jcse.etd_date), 'dd-MON-yyyy') SALES_ACT_DATE,");
            sb.Append("       'SEA' BUSINESS_TYPE,");
            sb.Append("       nvl(POR.PLACE_CODE, ' ') POR,");
            sb.Append("       nvl(POL.PORT_ID, ' ') POL,");
            sb.Append("       nvl(POD.PORT_ID, ' ') POD,");
            sb.Append("       nvl(PLD.PLACE_CODE, ' ') PFD,");
            if (IsFACInv == 1)
            {
                sb.Append("        OPR.OPERATOR_ID  SUPPLIER, ");
            }
            else
            {
                sb.Append("       CUST.CUSTOMER_ID CUSTOMER,");
            }
            sb.Append("       ' ' AGENT,");
            if (IsFACInv == 1)
            {
                sb.Append("       'SUPPLIER' PARTY_TYPE,");
            }
            else
            {
                sb.Append("       'CUSTOMER' PARTY_TYPE,");
            }
            sb.Append("       nvl(JCSE.VESSEL_NAME, ' ') VSL,");
            sb.Append("       nvl(JCSE.Voyage_Flight_No, ' ') VOYAGE,");
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
            sb.Append("  FROM JOB_CARD_TRN   JCSE,");
            sb.Append("       consol_invoice_tbl   INV, consol_invoice_trn_tbl invtrn,");
            sb.Append("       booking_mst_tbl        BKG,");
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
            if (IsFACInv == 1)
            {
                sb.Append("        OPERATOR_MST_TBL   OPR ");
            }
            else
            {
                sb.Append("       CUSTOMER_MST_TBL    CUST ");
            }
            sb.Append(" WHERE INVtrn.Job_Card_Fk = JCSE.JOB_CARD_TRN_PK and inv.consol_invoice_pk = invtrn.consol_invoice_fk");
            sb.Append("   AND INV.Consol_Invoice_Pk = " + InvPK);
            sb.Append("   AND CORP.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
            sb.Append("   AND JCSE.BOOKING_MST_FK= BKG.BOOKING_MST_PK");
            sb.Append("   AND HBL.JOB_CARD_SEA_EXP_FK(+) = JCSE.JOB_CARD_TRN_PK");
            sb.Append("   AND SHMT.CARGO_MOVE_PK(+) = JCSE.Cargo_Move_Fk");
            if (IsFACInv == 1)
            {
                sb.Append("    AND INV.SUPPLIER_MST_FK = OPR.OPERATOR_MST_PK ");
            }
            else
            {
                sb.Append("   AND JCSE.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
            }
            sb.Append("   AND BKG.COL_PLACE_MST_FK = POR.PLACE_PK(+)");
            sb.Append("   AND BKG.DEL_PLACE_MST_FK = PLD.PLACE_PK(+)");
            sb.Append("   AND BKG.PORT_MST_POD_FK = POL.PORT_MST_PK");
            sb.Append("   AND BKG.PORT_MST_POL_FK = POD.PORT_MST_PK");
            sb.Append("   AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
            sb.Append("   AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
            try
            {
                return objWF.GetDataTable(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
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

            sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
            sb.Append("  FROM (select distinct INV.CONSOL_INVOICE_PK INVPK, ");
            sb.Append("                        JCSE.JOB_CARD_TRN_PK JCPK,");
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
            sb.Append("               JOB_CARD_TRN jcse,");
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
            //modified by thiyagarajan on 8/4/09
            //sb.Append("              JJ.FREIGHT_AMT / count(JJJ.CONTAINER_TYPE_MST_FK) RATE,")
            sb.Append("               (case when count(JJJ.CONTAINER_TYPE_MST_FK)=0 then null  else JJ.FREIGHT_AMT / count(JJJ.CONTAINER_TYPE_MST_FK)end) RATE, ");
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
            sb.Append("           and jcse.JOB_CARD_TRN_PK = invTRN.Job_Card_Fk");
            sb.Append("           AND JOBFRT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            sb.Append("           and cntr.container_type_mst_pk = jobfrt.container_type_mst_fk");
            sb.Append("           AND JOBFRT.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("           AND QRY.FREIGHT_ELEMENT_MST_FK = JOBFRT.FREIGHT_ELEMENT_MST_FK");
            sb.Append("           and jccntr.job_card_sea_exp_fk = jcse.JOB_CARD_TRN_PK");
            sb.Append("           AND JOBFRT.FREIGHT_TYPE IN (1, 2)");
            sb.Append("           AND JOBFRT.JOB_CARD_SEA_EXP_FK = JCSE.JOB_CARD_TRN_PK");
            sb.Append("           AND JCSE.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
            sb.Append("           AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
            sb.Append("           AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
            sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)");
            sb.Append("           AND USR.USER_MST_PK = JCSE.CREATED_BY_FK");
            sb.Append("           AND INV.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
            sb.Append("        union");
            sb.Append("        select distinct INV.CONSOL_INVOICE_PK INVPK,");
            sb.Append("                        JCSE.JOB_CARD_TRN_PK JCPK,");
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
            sb.Append("               JOB_CARD_TRN jcse,");
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
            //modified by thiyagarajan on 8/4/09
            //sb.Append("                       JJ.Amount / count(JJJ.CONTAINER_TYPE_MST_FK) RATE,")
            sb.Append("               (case when count(JJJ.CONTAINER_TYPE_MST_FK)=0 then null  else JJ.AMOUNT / count(JJJ.CONTAINER_TYPE_MST_FK)end) RATE, ");
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
            sb.Append("           and jcse.JOB_CARD_TRN_PK = INVTRN.JOB_CARD_FK");
            sb.Append("           AND joboth.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK");
            sb.Append("           AND QRY.FREIGHT_ELEMENT_MST_FK = JOBOTH.FREIGHT_ELEMENT_MST_FK");
            sb.Append("           AND joboth.FREIGHT_ELEMENT_MST_FK = FMT.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("           AND JOBOTH.INV_AGENT_TRN_SEA_EXP_FK IS NULL");
            sb.Append("           AND JOBOTH.INV_CUST_TRN_SEA_EXP_FK IS NULL");
            sb.Append("           AND JOBOTH.CONSOL_INVOICE_TRN_FK IS NULL");
            sb.Append("           AND JOBOTH.Freight_Type = 2");
            sb.Append("           and jccntr.job_card_sea_exp_fk = jcse.JOB_CARD_TRN_PK");
            sb.Append("           AND JOBOTH.JOB_CARD_SEA_EXP_FK = JCSE.JOB_CARD_TRN_PK");
            sb.Append("           AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
            sb.Append("           AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
            sb.Append("           AND USR.USER_MST_PK = JCSE.CREATED_BY_FK");
            sb.Append("           AND INV.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
            sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)) Q");
            try
            {
                return objWF.GetDataTable(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataTable getJcDetailsFAC(string InvPK = "0", short BizType = 0, short ProcessType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder();

            sb.Append("SELECT Q.*, 'RECORD1' REF_NR, 'RECORD1' JCREF_NR");
            sb.Append("  FROM (select distinct INV.CONSOL_INVOICE_PK INVPK, ");
            sb.Append("                        JCSE.JOB_CARD_TRN_PK JCPK,");
            sb.Append("                        (SELECT CEMT.COST_ELEMENT_ID  FROM COST_ELEMENT_MST_TBL CEMT, PARAMETERS_TBL P");
            sb.Append("                        WHERE CEMT.COST_ELEMENT_MST_PK = P.FRT_FAC_FK) CHARGE_CODE,");
            sb.Append("                        '' CONTAINER_TYPE,");
            sb.Append("'" + HttpContext.Current.Session["CURRENCY_ID"] + "' TRANSACTION_CURRENCY,");
            sb.Append("                        INVTRN.TAX_PCNT VAT_PERCENTAGE,");
            sb.Append("                        INVTRN.VAT_CODE VAT_TYPE,");
            sb.Append("                        INVTRN.TAX_AMT VAT_AMOUNT,");
            sb.Append("                        INVTRN.TOT_AMT AMOUNT_INCL_VAT,");
            sb.Append("                        get_ex_rate(inv.currency_mst_fk,");
            sb.Append("                                    " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
            sb.Append("                                    inv.invoice_date) ROE,");
            sb.Append("                        'STANDARD' ROE_TYPE,");
            sb.Append("                        ROUND(INVTRN.ELEMENT_AMT *");
            sb.Append("                               GET_EX_RATE(inv.currency_mst_fk,");
            sb.Append("                                    " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
            sb.Append("                                           inv.invoice_date),");
            sb.Append("                               2) AMOUNT_IN_BASECURRENCY,");
            sb.Append("                        (CASE");
            sb.Append("                          WHEN JCSE.PYMT_TYPE = 1 THEN");
            sb.Append("                           'P'");
            sb.Append("                          ELSE");
            sb.Append("                           'C'");
            sb.Append("                        END) PREPAID_COLLECT,");
            sb.Append("                         OPR.OPERATOR_ID SUPPLIER,");
            sb.Append("                        ' ' AGENT,");
            sb.Append("                        'SUPPLIER' PARTY_TYPE,");
            sb.Append("                        NVL(LOC.LOCATION_ID, ' ') LOCATION");
            sb.Append("          from CONSOL_INVOICE_TRN_TBL  invtrn,");
            sb.Append("               CONSOL_INVOICE_TBL  inv,");
            sb.Append("               OPERATOR_MST_TBL       OPR,");
            sb.Append("               JOB_CARD_TRN jcse,");
            sb.Append("               AGENT_MST_TBL AGT,");
            sb.Append("               AGENT_MST_TBL AGTDP,");
            sb.Append("               LOCATION_MST_TBL LOC,");
            sb.Append("               USER_MST_TBL USR ");
            sb.Append("         where inv.consol_invoice_pk in ( " + InvPK + ")");
            sb.Append("           and invtrn.consol_invoice_fk = inv.consol_invoice_pk");
            sb.Append("           and jcse.JOB_CARD_TRN_PK = invTRN.Job_Card_Fk");
            sb.Append("           AND JCSE.CB_AGENT_MST_FK = AGT.AGENT_MST_PK(+)");
            if (ProcessType == 1)
            {
                sb.Append("           AND JCSE.DP_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
            }
            else
            {
                sb.Append("           AND JCSE.CL_AGENT_MST_FK = AGTDP.AGENT_MST_PK(+)");
            }
            sb.Append("           AND USR.DEFAULT_LOCATION_FK = LOC.LOCATION_MST_PK(+)");
            sb.Append("           AND USR.USER_MST_PK = JCSE.CREATED_BY_FK");
            sb.Append("           AND INV.SUPPLIER_MST_FK = OPR.OPERATOR_MST_PK ");
            sb.Append("          ) Q");

            try
            {
                return objWF.GetDataTable(sb.ToString());
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
        #endregion

        #region "Fetch VAT % and VAT Code" 'Snigdharani - 20/02/2009
        public DataSet Fetch_VAT(int ConsPk)
        {
            try
            {
                DataSet DsVAT = null;
                WorkFlow objWF = new WorkFlow();
                StringBuilder sb = new StringBuilder();
                //sb.Append("SELECT INVTRN.VAT_CODE,")
                //sb.Append("       CFRT.VAT_PERCENTAGE,")
                //sb.Append("       CFRT.FREIGHT_ELEMENT_MST_FK")
                //sb.Append("  FROM CONSOL_INVOICE_TBL     INV,")
                //sb.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,")
                //sb.Append("       FRT_VAT_COUNTRY_TBL    CFRT")
                //sb.Append(" WHERE INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK")
                //sb.Append("   AND INVTRN.FRT_OTH_ELEMENT_FK = CFRT.FREIGHT_ELEMENT_MST_FK")
                // sb.Append("   AND INV.CONSOL_INVOICE_PK =" & ConsPk)
                sb.Append("SELECT INVTRN.VAT_CODE,");
                sb.Append("       INVTRN.TAX_PCNT VAT_PERCENTAGE,");
                sb.Append("       INVTRN.FRT_OTH_ELEMENT_FK FREIGHT_ELEMENT_MST_FK");
                sb.Append("  FROM CONSOL_INVOICE_TBL     INV,");
                sb.Append("       CONSOL_INVOICE_TRN_TBL INVTRN");
                sb.Append(" WHERE INV.CONSOL_INVOICE_PK = INVTRN.CONSOL_INVOICE_FK");
                sb.Append("   AND INV.CONSOL_INVOICE_PK =" + ConsPk);
                DsVAT = objWF.GetDataSet(sb.ToString());
                return DsVAT;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "FETCH FAC INVOICE"
        public DataSet FetchConsolidatableFAC(string MBLPKs, string BIZ_TYPE, string Process, string SuppPk, bool Edit = false, int ExType = 1)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            int CurrencyPK = 0;
            string LocationPK = null;
            try
            {
                objWF.MyCommand.Parameters.Clear();
                CurrencyPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                LocationPK = Convert.ToString(HttpContext.Current.Session["LOGED_IN_LOC_FK"]);

                var _with18 = objWF.MyCommand.Parameters;
                _with18.Add("MBL_PK_IN", (string.IsNullOrEmpty(MBLPKs) ? "" : MBLPKs)).Direction = ParameterDirection.Input;
                _with18.Add("CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with18.Add("LOCATION_FK_IN", (string.IsNullOrEmpty(LocationPK) ? "" : LocationPK)).Direction = ParameterDirection.Input;
                _with18.Add("BIZ_TYPE_IN", (string.IsNullOrEmpty(BIZ_TYPE) ? "" : BIZ_TYPE)).Direction = ParameterDirection.Input;
                _with18.Add("MBL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (Convert.ToInt32(BIZ_TYPE) == 2)
                {
                    dsAll = objWF.GetDataSet("FETCH_FACINVOICE_NEW_PKG", "FETCH_FACINVOICE");
                }
                else
                {
                    dsAll = objWF.GetDataSet("FETCH_FACINVOICE_NEW_PKG", "FETCH_FACINVOICEAIR");
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
        public DataSet FetchInvoiceDataFAC(string MBLPKs, int intInvPk, int nBaseCurrPK, string BIZ_TYPE, short Process, int UserPk, string SuppPk, string CreditLimit, string amount, int ExType = 1)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            int CurrencyPK = 0;
            string LocationPK = null;
            try
            {
                objWF.MyCommand.Parameters.Clear();
                CurrencyPK = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                LocationPK = Convert.ToString(HttpContext.Current.Session["LOGED_IN_LOC_FK"]);

                var _with19 = objWF.MyCommand.Parameters;
                _with19.Add("MBL_PK_IN", (string.IsNullOrEmpty(MBLPKs) ? "" : MBLPKs)).Direction = ParameterDirection.Input;
                _with19.Add("CURRENCY_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                _with19.Add("LOCATION_FK_IN", (string.IsNullOrEmpty(LocationPK) ? "" : LocationPK)).Direction = ParameterDirection.Input;
                _with19.Add("BIZ_TYPE_IN", (string.IsNullOrEmpty(BIZ_TYPE) ? "": BIZ_TYPE)).Direction = ParameterDirection.Input;
                _with19.Add("PROCESS_TYPE_IN", (Process == 0 ? 1 : Process)).Direction = ParameterDirection.Input;
                _with19.Add("OPERATOR_MST_FK_IN", SuppPk).Direction = ParameterDirection.Input;
                _with19.Add("ISBBC_INV_IN", 1).Direction = ParameterDirection.Input;
                _with19.Add("JOB_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("FETCH_FACINVOICE_NEW_PKG", "FETCH_FACINVOICEJOB");
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

        //'Sushama for FAC Invoice Print
        #region "Fetch FAC Consol invoice Supplier "
        public DataSet CONSOL_FAC_SUPP_RPT_PRINT(int Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with20 = objWK.MyCommand.Parameters;
                _with20.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with20.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with20.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with20.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_FAC_SUPP_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion
        #region "Fetch FAC invoice Details Report "
        public DataSet FAC_INV_DETAIL_MAIN_PRINT(string Inv_PK, int Biz_Type, int Process_Type, string User_Name)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with21 = objWK.MyCommand.Parameters;
                _with21.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with21.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with21.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with21.Add("USER_NAME_IN", User_Name).Direction = ParameterDirection.Input;
                _with21.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_DET_FAC_MAIN_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion
        #region "Fetch Consol FAC invoice Sub Details Report "
        public DataSet CONSOL_INV_DETAIL_SUB_MAIN_FAC_PRINT(string Inv_PK, int Biz_Type, int Process_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with22 = objWK.MyCommand.Parameters;
                _with22.Add("INV_PK_IN", Inv_PK).Direction = ParameterDirection.Input;
                _with22.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with22.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with22.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_PKG", "CONSOL_SUB_MAIN_FAC_RPT_PRINT");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion
        #region "Fetch FAC Shipment Details Report "
        //MBL_pk, CURRENCY_FK_IN, HttpContext.Current.Session("LOGED_IN_LOC_FK"), IIf(BizType Is Nothing, biz, BizType), MBL_CUR
        public DataSet FAC_INV_SHIPMENT_DETAIL_MAIN_PRINT(string MBL_pk, int CURRENCY_FK, int LOG_IN_CURRENCY_FK, int LOCATION_FK, int Biz_Type)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with23 = objWK.MyCommand.Parameters;
                _with23.Add("MBL_PK_IN", MBL_pk).Direction = ParameterDirection.Input;
                _with23.Add("CURRENCY_FK_IN", CURRENCY_FK).Direction = ParameterDirection.Input;
                _with23.Add("LOG_IN_LOC_CURR_FK_IN", LOG_IN_CURRENCY_FK).Direction = ParameterDirection.Input;
                _with23.Add("LOCATION_FK_IN", LOCATION_FK).Direction = ParameterDirection.Input;
                _with23.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with23.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (Biz_Type == 2)
                {
                    return objWK.GetDataSet("CONSOL_INV_PKG", "FETCH_FACINVOICESEA_PRINT");
                }
                else
                {
                    return objWK.GetDataSet("CONSOL_INV_PKG", "FETCH_FACINVOICEAIR_PRINT");
                }

            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        public DataSet CONSOL_DRAFT_INV_DETAIL_SUB_MAIN_PRINT(string JobPks, string FrtElePKs, int Biz_Type, int Process_Type, int Loc_fk = 0, int CurrFK = 0, string ContPKS = "", int Log_Curr_fk = 0)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with24 = objWK.MyCommand.Parameters;
                _with24.Add("JOB_CARD_PKS_IN", JobPks).Direction = ParameterDirection.Input;
                _with24.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with24.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with24.Add("FREIGHT_ELE_FKS_IN", FrtElePKs).Direction = ParameterDirection.Input;
                _with24.Add("LOG_IN_LOC_FK_IN", Loc_fk).Direction = ParameterDirection.Input;
                _with24.Add("CURRENCY_FK_IN", CurrFK).Direction = ParameterDirection.Input;
                _with24.Add("CONTAINER_FKS_IN", ContPKS).Direction = ParameterDirection.Input;
                _with24.Add("LOGIN_CUR_FK_IN", Log_Curr_fk).Direction = ParameterDirection.Input;
                _with24.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_DRAFT_PKG", "BBCONSOL_SUB_MAIN_RPT_PRINT");
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
        public DataSet CONSOL_DRAFT_AIF_DETAIL_SUB_MAIN_PRINT(string JobPks, string FrtElePKs, int Biz_Type, int Process_Type, int Loc_fk = 0, int CurrFK = 0, string ContPKS = "", int Log_Curr_fk = 0)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with25 = objWK.MyCommand.Parameters;
                _with25.Add("JOB_CARD_PKS_IN", JobPks).Direction = ParameterDirection.Input;
                _with25.Add("BIZ_TYPE_IN", Biz_Type).Direction = ParameterDirection.Input;
                _with25.Add("PROCESS_TYPE_IN", Process_Type).Direction = ParameterDirection.Input;
                _with25.Add("FREIGHT_ELE_FKS_IN", FrtElePKs).Direction = ParameterDirection.Input;
                _with25.Add("LOG_IN_LOC_FK_IN", Loc_fk).Direction = ParameterDirection.Input;
                _with25.Add("CURRENCY_FK_IN", CurrFK).Direction = ParameterDirection.Input;
                _with25.Add("CONTAINER_FKS_IN", ContPKS).Direction = ParameterDirection.Input;
                _with25.Add("LOGIN_CUR_FK_IN", Log_Curr_fk).Direction = ParameterDirection.Input;
                _with25.Add("CUR_CONSOL_INV", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("CONSOL_INV_DRAFT_PKG", "BBCONSOL_SUB_AIF_RPT_PRINT");
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
    }
}