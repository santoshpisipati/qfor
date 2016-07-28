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

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsCreditNoteToAgentAirEntry : CommonFeatures
    {
        #region " Fetch Functionality"

        /// <summary>
        /// Fetches the after save.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <returns></returns>
        public DataSet FetchAfterSave(string strPK)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            strSqlBuilder.Append("  SELECT INV.JOB_CARD_AIR_EXP_FK ,");
            strSqlBuilder.Append("  CR.CR_AGENT_AIR_EXP_PK,");
            strSqlBuilder.Append("  INV.INV_AGENT_AIR_EXP_PK ,");
            strSqlBuilder.Append("  INV.CB_AGENT_MST_FK ,");
            strSqlBuilder.Append("  INV.CURRENCY_MST_FK ,");
            strSqlBuilder.Append("  JC.JOBCARD_REF_NO ,");
            strSqlBuilder.Append("  INV.INVOICE_REF_NO ,");
            strSqlBuilder.Append("  CR.CREDIT_NOTE_REF_NO,");
            strSqlBuilder.Append("  CR.CREDIT_NOTE_DATE,");
            strSqlBuilder.Append("  TO_CHAR(INV.INVOICE_DATE, '" + dateFormat + "') AS INV_DATE,");
            strSqlBuilder.Append("  INV.GROSS_INV_AMT AS G_AMT,");
            strSqlBuilder.Append("  (SELECT SUM(NVL(INVT.TAX_AMT,0)) FROM INV_AGENT_TRN_AIR_EXP_TBL INVT WHERE INVT.INV_AGENT_AIR_EXP_FK = INV.INV_AGENT_AIR_EXP_PK) AS VAT_AMT,");
            strSqlBuilder.Append(" INV.DISCOUNT_AMT AS DISCOUNT,");
            strSqlBuilder.Append(" (SELECT sum(nvl(it.amt_in_inv_curr,0)) FROM inv_agent_trn_air_exp_tbl it where it.inv_agent_air_exp_fk=inv.inv_agent_air_exp_pk)  AS INV_AMT,");
            strSqlBuilder.Append(" CR.CREDIT_NOTE_AMT,");
            strSqlBuilder.Append(" CR.REMARKS,");
            strSqlBuilder.Append(" CUR.CURRENCY_ID,");
            strSqlBuilder.Append(" AG.AGENT_NAME,");
            strSqlBuilder.Append(" (SELECT SUM(CR.CREDIT_NOTE_AMT) ");
            strSqlBuilder.Append(" FROM CR_AGENT_AIR_EXP_TBL CR ");
            strSqlBuilder.Append(" WHERE CR.INV_AGENT_AIR_EXP_FK = INV.INV_AGENT_AIR_EXP_PK) AS CREDITAMT, ");
            strSqlBuilder.Append("   UMTCRT.USER_NAME    AS CREATED_BY, ");
            strSqlBuilder.Append("   UMTUPD.USER_NAME    AS LAST_MODIFIED_BY, ");
            strSqlBuilder.Append("   UMTAPP.USER_NAME    AS APPROVED_BY, ");
            strSqlBuilder.Append("   TO_DATE(CR.CREATED_DT) CREATED_BY_DT, ");
            strSqlBuilder.Append("   TO_DATE(CR.LAST_MODIFIED_DT) LAST_MODIFIEDBY_DT, ");
            strSqlBuilder.Append("   TO_DATE(CR.LAST_MODIFIED_DT) APPROVED_DT ");
            strSqlBuilder.Append(" FROM CR_AGENT_AIR_EXP_TBL CR, ");
            strSqlBuilder.Append(" INV_AGENT_AIR_EXP_TBL INV,");
            strSqlBuilder.Append(" JOB_CARD_AIR_EXP_TBL  JC,");
            strSqlBuilder.Append(" AGENT_MST_TBL         AG,");
            strSqlBuilder.Append(" CURRENCY_TYPE_MST_TBL CUR,");
            strSqlBuilder.Append("  USER_MST_TBL UMTCRT, ");
            strSqlBuilder.Append("  USER_MST_TBL UMTUPD, ");
            strSqlBuilder.Append("  USER_MST_TBL UMTAPP ");
            strSqlBuilder.Append(" WHERE INV.JOB_CARD_AIR_EXP_FK = JC.JOB_CARD_AIR_EXP_PK(+)");
            strSqlBuilder.Append(" AND INV.CB_AGENT_MST_FK = AG.AGENT_MST_PK(+)");
            strSqlBuilder.Append(" AND INV.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK(+)");
            strSqlBuilder.Append(" AND CR.INV_AGENT_AIR_EXP_FK=INV.INV_AGENT_AIR_EXP_PK ");
            strSqlBuilder.Append(" AND CR.CR_AGENT_AIR_EXP_PK=" + strPK);
            strSqlBuilder.Append(" AND UMTCRT.USER_MST_PK(+) = CR.CREATED_BY_FK ");
            strSqlBuilder.Append(" AND UMTUPD.USER_MST_PK(+) = CR.LAST_MODIFIED_BY_FK  ");
            strSqlBuilder.Append(" AND UMTAPP.USER_MST_PK(+) = CR.LAST_MODIFIED_BY_FK  ");
            strSqlBuilder.Append(" ORDER BY INV.INVOICE_REF_NO ");
            try
            {
                return objWK.GetDataSet(strSqlBuilder.ToString());
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

        #endregion " Fetch Functionality"

        #region " Fetch Report Data"

        /// <summary>
        /// Fetches the report data.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <param name="AgentFlag">The agent flag.</param>
        /// <returns></returns>
        public DataSet FetchReportData(long strPK, short AgentFlag)
        {
            //Agent Flag = 1 for CB Agent
            //Agent Flag = 2 for DP Agent

            StringBuilder strSqlBuilder = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            strSqlBuilder.Append(" SELECT BAT.customer_ref_no CUSTREF,CRSTBL.CR_AGENT_AIR_EXP_PK CBPK,");
            strSqlBuilder.Append(" CRSTBL.CREDIT_NOTE_REF_NO CRREFNO,");
            strSqlBuilder.Append(" CRSTBL.CREDIT_NOTE_AMT CRAMT,");
            strSqlBuilder.Append(" INVAGTEXP.INV_AGENT_AIR_EXP_PK INVPK,");
            strSqlBuilder.Append(" INVAGTEXP.INVOICE_REF_NO       INVREFNO,");
            strSqlBuilder.Append(" JSE.JOB_CARD_AIR_EXP_PK        JOBPK,");
            strSqlBuilder.Append(" JSE.JOBCARD_REF_NO             JOBREFNO,");
            strSqlBuilder.Append(" '' CLEARANCEPOINT,");
            strSqlBuilder.Append(" AMST.AGENT_NAME         AGENTNAME,");
            strSqlBuilder.Append(" AMST.ACCOUNT_NO        AGENTREFNO,");
            strSqlBuilder.Append(" ADTLS.ADM_ADDRESS_1     AGENTADD1,");
            strSqlBuilder.Append(" ADTLS.ADM_ADDRESS_2     AGENTADD2,");
            strSqlBuilder.Append(" ADTLS.ADM_ADDRESS_3     AGENTADD3,");
            strSqlBuilder.Append(" ADTLS.ADM_CITY          AGENTCITY,");
            strSqlBuilder.Append(" ADTLS.ADM_ZIP_CODE      AGENTZIP,");
            strSqlBuilder.Append(" ADTLS.ADM_PHONE_NO_1    AGENTPHONE,");
            strSqlBuilder.Append(" ADTLS.ADM_FAX_NO        AGENTFAX,");
            strSqlBuilder.Append(" ADTLS.ADM_EMAIL_ID      AGENTEMAIL,");
            strSqlBuilder.Append(" AGTCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,");
            strSqlBuilder.Append(" SHIPMST.CUSTOMER_NAME    SHIPPER,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_ADDRESS_1   SHIPPERADD1,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_ADDRESS_2   SHIPPERADD2,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_ADDRESS_3   SHIPPERADD3,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_CITY        SHIPPERCITY,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_ZIP_CODE    SHIPPERZIP,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_PHONE_NO_1  SHIPPERPHONE,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_FAX_NO      SHIPPERFAX,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_EMAIL_ID    SHIPPEREMAIL,");
            strSqlBuilder.Append(" SHIPCOUNTRY.COUNTRY_NAME SHIPPERCOUNTRY,");
            strSqlBuilder.Append(" JSE.ETD_DATE ETD,");
            strSqlBuilder.Append(" JSE.ETA_DATE ETA ,");
            strSqlBuilder.Append(" BAT.CARGO_TYPE CARGO_TYPE,");
            strSqlBuilder.Append(" CURRMST.CURRENCY_ID CURRID,");
            strSqlBuilder.Append(" CURRMST.CURRENCY_NAME CURRNAME,");
            strSqlBuilder.Append(" JSE.FLIGHT_NO VES_FLIGHT,");
            strSqlBuilder.Append(" JSE.PYMT_TYPE PYMT,");
            strSqlBuilder.Append(" JSE.GOODS_DESCRIPTION GOODS,");
            strSqlBuilder.Append(" JSE.MARKS_NUMBERS MARKS,");
            strSqlBuilder.Append(" NVL(JSE.INSURANCE_AMT, 0) INSURANCE,");
            strSqlBuilder.Append(" STMST.INCO_CODE TERMS,");
            strSqlBuilder.Append(" AMST.VAT_NO AGTVATNO,");
            strSqlBuilder.Append(" 2 PAYMENTDAYS,");
            strSqlBuilder.Append(" COLMST.PLACE_NAME COLPLACE,");
            strSqlBuilder.Append(" DELMST.PLACE_NAME DELPLACE,");
            strSqlBuilder.Append(" POLMST.PORT_NAME POL,");
            strSqlBuilder.Append(" PODMST.PORT_NAME POD,");
            strSqlBuilder.Append(" (CASE");
            strSqlBuilder.Append(" WHEN JSE.HAWB_EXP_TBL_FK IS NOT NULL THEN");
            strSqlBuilder.Append("  HAWB.HAWB_REF_NO");
            strSqlBuilder.Append(" ELSE");
            strSqlBuilder.Append("  MAWB.MAWB_REF_NO");
            strSqlBuilder.Append(" END) BLREFNO,");
            strSqlBuilder.Append(" CGMST.COMMODITY_GROUP_DESC COMMODITY,");
            strSqlBuilder.Append(" SUM(JSEC.VOLUME_IN_CBM) VOLUME,");
            strSqlBuilder.Append(" SUM(JSEC.GROSS_WEIGHT) GROSS,");
            //strSqlBuilder.Append(" SUM(JSEC.NET_WEIGHT) NETWT,")
            strSqlBuilder.Append(" 0 NETWT,");
            strSqlBuilder.Append(" SUM(JSEC.CHARGEABLE_WEIGHT) CHARWT, ");
            strSqlBuilder.Append(" CRSTBL.Remarks ");
            strSqlBuilder.Append(" FROM CR_AGENT_AIR_EXP_TBL CRSTBL, INV_AGENT_AIR_EXP_TBL INVAGTEXP,");
            strSqlBuilder.Append(" CURRENCY_TYPE_MST_TBL CURRMST,");
            strSqlBuilder.Append(" JOB_CARD_AIR_EXP_TBL    JSE,");
            strSqlBuilder.Append(" JOB_TRN_AIR_EXP_CONT    JSEC,");
            strSqlBuilder.Append(" SHIPPING_TERMS_MST_TBL  STMST,");
            strSqlBuilder.Append(" BOOKING_AIR_TBL         BAT,");
            strSqlBuilder.Append(" PLACE_MST_TBL           COLMST,");
            strSqlBuilder.Append(" PLACE_MST_TBL           DELMST,");
            strSqlBuilder.Append(" PORT_MST_TBL            POLMST,");
            strSqlBuilder.Append(" PORT_MST_TBL            PODMST,");
            strSqlBuilder.Append(" HAWB_EXP_TBL            HAWB,");
            strSqlBuilder.Append(" MAWB_EXP_TBL            MAWB,");
            strSqlBuilder.Append(" COMMODITY_GROUP_MST_TBL CGMST,");

            strSqlBuilder.Append(" AGENT_MST_TBL      AMST,");
            strSqlBuilder.Append(" AGENT_CONTACT_DTLS ADTLS,");
            strSqlBuilder.Append(" COUNTRY_MST_TBL    AGTCOUNTRY,");

            strSqlBuilder.Append(" CUSTOMER_MST_TBL      SHIPMST,");
            strSqlBuilder.Append(" CUSTOMER_CONTACT_DTLS SHIPDTLS,");
            strSqlBuilder.Append("  COUNTRY_MST_TBL SHIPCOUNTRY");

            strSqlBuilder.Append(" WHERE CRSTBL.INV_AGENT_AIR_EXP_FK=INVAGTEXP.INV_AGENT_AIR_EXP_PK");
            strSqlBuilder.Append(" AND INVAGTEXP.JOB_CARD_AIR_EXP_FK = JSE.JOB_CARD_AIR_EXP_PK");
            strSqlBuilder.Append(" AND CURRMST.CURRENCY_MST_PK(+) = INVAGTEXP.CURRENCY_MST_FK");
            strSqlBuilder.Append(" AND JSE.JOB_CARD_AIR_EXP_PK = JSEC.JOB_CARD_AIR_EXP_FK");
            strSqlBuilder.Append(" AND STMST.SHIPPING_TERMS_MST_PK(+) = JSE.SHIPPING_TERMS_MST_FK");
            strSqlBuilder.Append(" AND BAT.BOOKING_AIR_PK(+) = JSE.BOOKING_AIR_FK");
            strSqlBuilder.Append(" AND COLMST.PLACE_PK(+) = BAT.COL_PLACE_MST_FK");
            strSqlBuilder.Append(" AND DELMST.PLACE_PK(+) = BAT.DEL_PLACE_MST_FK");
            strSqlBuilder.Append(" AND POLMST.PORT_MST_PK = BAT.PORT_MST_POL_FK");
            strSqlBuilder.Append(" AND PODMST.PORT_MST_PK = BAT.PORT_MST_POD_FK");
            strSqlBuilder.Append(" AND HAWB.HAWB_EXP_TBL_PK(+) = JSE.HAWB_EXP_TBL_FK");
            strSqlBuilder.Append(" AND MAWB.MAWB_EXP_TBL_PK(+) = JSE.MAWB_EXP_TBL_FK");
            strSqlBuilder.Append(" AND CGMST.COMMODITY_GROUP_PK(+) = JSE.COMMODITY_GROUP_FK");

            if (AgentFlag == 1)
            {
                strSqlBuilder.Append(" AND AMST.AGENT_MST_PK = JSE.CB_AGENT_MST_FK");
            }
            else
            {
                strSqlBuilder.Append(" AND AMST.AGENT_MST_PK = JSE.dp_agent_mst_fk");
            }

            strSqlBuilder.Append(" AND ADTLS.AGENT_MST_FK(+) = AMST.AGENT_MST_PK");
            strSqlBuilder.Append(" AND AGTCOUNTRY.COUNTRY_MST_PK(+) = ADTLS.ADM_COUNTRY_MST_FK");
            if (AgentFlag == 1)
            {
                strSqlBuilder.Append(" AND SHIPMST.CUSTOMER_MST_PK(+) = JSE.Shipper_CUST_MST_FK");
            }
            else
            {
                strSqlBuilder.Append(" AND SHIPMST.CUSTOMER_MST_PK(+) = JSE.CONSIGNEE_CUST_MST_FK");
            }
            strSqlBuilder.Append(" AND SHIPDTLS.CUSTOMER_MST_FK(+) = SHIPMST.CUSTOMER_MST_PK");
            strSqlBuilder.Append(" AND SHIPDTLS.ADM_COUNTRY_MST_FK = SHIPCOUNTRY.COUNTRY_MST_PK(+)");
            strSqlBuilder.Append(" AND CRSTBL.CR_AGENT_AIR_EXP_PK=" + strPK);
            strSqlBuilder.Append(" GROUP BY CRSTBL.CR_AGENT_AIR_EXP_PK,");
            strSqlBuilder.Append(" CRSTBL.CREDIT_NOTE_REF_NO,");
            strSqlBuilder.Append(" CRSTBL.CREDIT_NOTE_AMT,");
            strSqlBuilder.Append(" INVAGTEXP.INV_AGENT_AIR_EXP_PK,");
            strSqlBuilder.Append(" INVAGTEXP.INVOICE_REF_NO,");
            strSqlBuilder.Append(" JSE.JOB_CARD_AIR_EXP_PK,");
            strSqlBuilder.Append(" JSE.JOBCARD_REF_NO,");
            strSqlBuilder.Append(" AMST.AGENT_NAME,");
            strSqlBuilder.Append(" AMST.ACCOUNT_NO ,");
            strSqlBuilder.Append(" ADTLS.ADM_ADDRESS_1,");
            strSqlBuilder.Append(" ADTLS.ADM_ADDRESS_2,");
            strSqlBuilder.Append(" ADTLS.ADM_ADDRESS_3,");
            strSqlBuilder.Append(" ADTLS.ADM_CITY,");
            strSqlBuilder.Append(" ADTLS.ADM_ZIP_CODE,");
            strSqlBuilder.Append(" ADTLS.ADM_PHONE_NO_1,");
            strSqlBuilder.Append(" ADTLS.ADM_FAX_NO,");
            strSqlBuilder.Append(" ADTLS.ADM_EMAIL_ID,");
            strSqlBuilder.Append(" AGTCOUNTRY.COUNTRY_NAME,");
            strSqlBuilder.Append(" SHIPMST.CUSTOMER_NAME,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_ADDRESS_1,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_ADDRESS_2,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_ADDRESS_3,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_CITY,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_ZIP_CODE,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_PHONE_NO_1,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_FAX_NO,");
            strSqlBuilder.Append(" SHIPDTLS.ADM_EMAIL_ID,");
            strSqlBuilder.Append(" SHIPCOUNTRY.COUNTRY_NAME,");

            strSqlBuilder.Append(" JSE.ETD_DATE ,");
            strSqlBuilder.Append(" JSE.ETA_DATE,");
            strSqlBuilder.Append(" BAT.CARGO_TYPE,");
            strSqlBuilder.Append(" CURRMST.CURRENCY_ID,");
            strSqlBuilder.Append(" CURRMST.CURRENCY_NAME,");
            strSqlBuilder.Append(" JSE.FLIGHT_NO,");
            strSqlBuilder.Append(" JSE.PYMT_TYPE,");
            strSqlBuilder.Append(" JSE.GOODS_DESCRIPTION,");
            strSqlBuilder.Append(" JSE.MARKS_NUMBERS,");
            strSqlBuilder.Append(" JSE.INSURANCE_AMT,");
            strSqlBuilder.Append(" STMST.INCO_CODE,");
            strSqlBuilder.Append(" AMST.VAT_NO,");
            strSqlBuilder.Append(" COLMST.PLACE_NAME,");
            strSqlBuilder.Append(" DELMST.PLACE_NAME,");
            strSqlBuilder.Append(" POLMST.PORT_NAME,");
            strSqlBuilder.Append(" PODMST.PORT_NAME,");
            strSqlBuilder.Append(" (CASE");
            strSqlBuilder.Append("  WHEN JSE.HAWB_EXP_TBL_FK IS NOT NULL THEN");
            strSqlBuilder.Append(" HAWB.HAWB_REF_NO");
            strSqlBuilder.Append(" ELSE");
            strSqlBuilder.Append("  MAWB.MAWB_REF_NO");
            strSqlBuilder.Append(" END),");
            strSqlBuilder.Append(" CGMST.COMMODITY_GROUP_DESC ,");
            strSqlBuilder.Append(" CRSTBL.Remarks,  ");
            strSqlBuilder.Append(" BAT.customer_ref_no");

            try
            {
                return objWK.GetDataSet(strSqlBuilder.ToString());
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

        /// <summary>
        /// Fetches the container details.
        /// </summary>
        /// <param name="CRPK">The CRPK.</param>
        /// <returns></returns>
        public DataSet FetchContainerDetails(Int64 CRPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            strSQL = "SELECT JTSIC.PALETTE_SIZE  CONTAINER";
            strSQL += "FROM CR_AGENT_AIR_EXP_TBL CRSTBL,INV_AGENT_AIR_EXP_TBL IASI,";
            strSQL += "JOB_CARD_AIR_EXP_TBL  JSE,";
            strSQL += "JOB_TRN_AIR_EXP_CONT JTSIC";
            strSQL += "WHERE IASI.JOB_CARD_AIR_EXP_FK = JSE.JOB_CARD_AIR_EXP_PK";
            strSQL += "AND JTSIC.JOB_CARD_AIR_EXP_FK = JSE.JOB_CARD_AIR_EXP_PK";
            strSQL += "AND CRSTBL.INV_AGENT_AIR_EXP_FK=IASI.INV_AGENT_AIR_EXP_PK";
            strSQL += "AND CRSTBL.CR_AGENT_AIR_EXP_PK=" + CRPK;
            try
            {
                return (objWK.GetDataSet(strSQL));
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

        #endregion " Fetch Report Data"
    }
}