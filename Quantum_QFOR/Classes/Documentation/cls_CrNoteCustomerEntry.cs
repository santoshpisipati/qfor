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
    public class clsCrNoteCustomerEntry : CommonFeatures
    {
        #region " Fetch"

        /// <summary>
        /// Fetches the after save.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <returns></returns>
        public DataSet FetchAfterSave(string strPK)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            WorkFlow objWK = new WorkFlow();

            strSqlBuilder.Append("  SELECT INV.JOB_CARD_TRN_FK ,");
            strSqlBuilder.Append("  INV.INV_CUST_SEA_EXP_PK,");
            strSqlBuilder.Append("  CR.CR_CUST_SEA_EXP_PK,");
            strSqlBuilder.Append("  JC.SHIPPER_CUST_MST_FK ,");
            strSqlBuilder.Append("  CR.CURRENCY_MST_FK ,");
            strSqlBuilder.Append("  JC.JOBCARD_REF_NO ,");
            strSqlBuilder.Append("  INV.INVOICE_REF_NO ,");
            strSqlBuilder.Append("  CR.CREDIT_NOTE_REF_NO,");
            strSqlBuilder.Append("  CR.CREDIT_NOTE_DATE,");
            strSqlBuilder.Append("  TO_CHAR(INV.INVOICE_DATE, '" + dateFormat + "') AS INV_DATE,");
            strSqlBuilder.Append("  INV.INVOICE_AMT AS G_AMT,");
            strSqlBuilder.Append("  (SELECT SUM(NVL(INVT.TAX_AMT,0)) FROM ");
            strSqlBuilder.Append("  INV_CUST_TRN_SEA_EXP_TBL INVT  ");
            strSqlBuilder.Append("  WHERE INVT.INV_CUST_SEA_EXP_FK = INV.INV_CUST_SEA_EXP_PK) AS VAT_AMT, ");
            strSqlBuilder.Append("  INV.DISCOUNT_AMT AS DISCOUNT,");
            strSqlBuilder.Append("  INV.NET_PAYABLE AS INV_AMT,");
            strSqlBuilder.Append("  CR.CREDIT_NOTE_AMT,");
            strSqlBuilder.Append("  CR.REMARKS,");
            strSqlBuilder.Append("  CUR.CURRENCY_ID,");
            strSqlBuilder.Append("  (SELECT SUM(CREDIT_NOTE_AMT) ");
            strSqlBuilder.Append(" FROM CR_CUST_SEA_EXP_TBL CRN ");
            strSqlBuilder.Append("   WHERE CRN.INV_CUST_SEA_EXP_FK=INV.INV_CUST_SEA_EXP_PK) AS TOTAL_CREDIT_AMT,");
            strSqlBuilder.Append("   CUST.CUSTOMER_NAME");
            strSqlBuilder.Append("   FROM CR_CUST_SEA_EXP_TBL CR, ");
            strSqlBuilder.Append("  INV_CUST_SEA_EXP_TBL INV,");
            strSqlBuilder.Append("  JOB_CARD_TRN  JC,");
            strSqlBuilder.Append("  CUSTOMER_MST_TBL         CUST,");
            strSqlBuilder.Append("  CURRENCY_TYPE_MST_TBL CUR");
            strSqlBuilder.Append("  WHERE INV.JOB_CARD_TRN_FK = JC.JOB_CARD_TRN_PK(+)");
            strSqlBuilder.Append("  AND JC.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)");
            strSqlBuilder.Append("  AND CR.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK(+)");
            strSqlBuilder.Append("  AND CR.INV_CUST_SEA_EXP_FK=INV.INV_CUST_SEA_EXP_PK ");
            strSqlBuilder.Append(" AND CR.CR_CUST_SEA_EXP_PK=" + strPK + "");
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

        #endregion " Fetch"

        #region " Fetch Report Data"

        /// <summary>
        /// Fetches the report data.
        /// </summary>
        /// <param name="InvPK">The inv pk.</param>
        /// <returns></returns>
        public DataSet FetchReportData(long InvPK)
        {
            StringBuilder Strsql = new StringBuilder();
            WorkFlow ObjWk = new WorkFlow();

            Strsql.Append("  SELECT  CRSTBL.CR_CUST_SEA_EXP_PK CRPK, ");
            Strsql.Append("    CRSTBL.CREDIT_NOTE_REF_NO CRREFNO,");
            Strsql.Append("    CRSTBL.CREDIT_NOTE_AMT CRAMT,");
            Strsql.Append("    INVCUSTEXP.INV_CUST_SEA_EXP_PK INVPK,");
            Strsql.Append("    INVCUSTEXP.INVOICE_REF_NO       INVREFNO,");
            Strsql.Append("    JSE.JOB_CARD_TRN_PK JOBPK,");
            Strsql.Append("    JSE.JOBCARD_REF_NO  JOBREFNO,");
            Strsql.Append("    '' CLEARANCEPOINT,");
            Strsql.Append("    TO_CHAR(JSE.ETD_DATE,'" + dateFormat + "') ETD,");
            Strsql.Append("    TO_CHAR(JSE.ETA_DATE,'" + dateFormat + "') ETA,");
            Strsql.Append("    BAT.CARGO_TYPE CARGO_TYPE,");
            Strsql.Append("    SHIPMST.CUSTOMER_NAME    AGENTNAME,");
            Strsql.Append("    BAT.CUSTOMER_REF_NO  AGENTREFNO,");
            Strsql.Append("    SHIPDTLS.ADM_ADDRESS_1   AGENTADD1,");
            Strsql.Append("    SHIPDTLS.ADM_ADDRESS_2   AGENTADD2,");
            Strsql.Append("    SHIPDTLS.ADM_ADDRESS_3   AGENTADD3,");
            Strsql.Append("    SHIPDTLS.ADM_CITY        AGENTCITY,");
            Strsql.Append("    SHIPDTLS.ADM_ZIP_CODE    AGENTZIP,");
            Strsql.Append("    SHIPDTLS.ADM_PHONE_NO_1  AGENTPHONE,");
            Strsql.Append("    SHIPDTLS.ADM_FAX_NO      AGENTFAX,");
            Strsql.Append("    SHIPDTLS.ADM_EMAIL_ID    AGENTEMAIL,");
            Strsql.Append("    SHIPCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,");
            Strsql.Append("    CONSMST.CUSTOMER_NAME    SHIPPER,");
            Strsql.Append("    CONSDTLS.ADM_ADDRESS_1   SHIPPERADD1,");
            Strsql.Append("    CONSDTLS.ADM_ADDRESS_2   SHIPPERADD2,");
            Strsql.Append("   CONSDTLS.ADM_ADDRESS_3   SHIPPERADD3,");
            Strsql.Append("    CONSDTLS.ADM_CITY        SHIPPERCITY,");
            Strsql.Append("    CONSDTLS.ADM_ZIP_CODE    SHIPPERZIP,");
            Strsql.Append("    CONSDTLS.ADM_PHONE_NO_1  SHIPPERPHONE,");
            Strsql.Append("    CONSDTLS.ADM_FAX_NO      SHIPPERFAX,");
            Strsql.Append("    CONSDTLS.ADM_EMAIL_ID    SHIPPEREMAIL,");
            Strsql.Append("    CONSCOUNTRY.COUNTRY_NAME SHIPPERCOUNTRY,");
            Strsql.Append("    CURRMST.CURRENCY_ID CURRID,");
            Strsql.Append("    CURRMST.CURRENCY_NAME CURRNAME,");
            Strsql.Append("    (CASE WHEN JSE.VOYAGE IS NOT NULL THEN");
            Strsql.Append("    JSE.VESSEL_NAME || '-' || JSE.VOYAGE");
            Strsql.Append("    ELSE");
            Strsql.Append("    JSE.VESSEL_NAME END) VES_FLIGHT,");
            Strsql.Append("    JSE.PYMT_TYPE PYMT,");
            Strsql.Append("    JSE.GOODS_DESCRIPTION GOODS,");
            Strsql.Append("    JSE.MARKS_NUMBERS MARKS,");
            Strsql.Append("    NVL(JSE.INSURANCE_AMT, 0) INSURANCE,");
            Strsql.Append("    STMST.INCO_CODE TERMS,");
            Strsql.Append("    SHIPMST.VAT_NO AGTVATNO,");
            Strsql.Append("    SHIPMST.CREDIT_DAYS PAYMENTDAYS,");
            Strsql.Append("    COLMST.PLACE_NAME COLPLACE,");
            Strsql.Append("    DELMST.PLACE_NAME DELPLACE,");
            Strsql.Append("    POLMST.PORT_NAME POL,");
            Strsql.Append("    PODMST.PORT_NAME POD,");
            Strsql.Append("    (CASE");
            Strsql.Append("     WHEN JSE.HBL_HAWB_FK IS NOT NULL THEN");
            Strsql.Append("     HBL.HBL_REF_NO");
            Strsql.Append("    ELSE");
            Strsql.Append("    MBL.MBL_REF_NO");
            Strsql.Append("    END) BLREFNO,");
            Strsql.Append("     CGMST.COMMODITY_GROUP_DESC COMMODITY,");
            Strsql.Append("    SUM(JSEC.VOLUME_IN_CBM) VOLUME,");
            Strsql.Append("    SUM(JSEC.GROSS_WEIGHT) GROSS,");
            Strsql.Append("    SUM(JSEC.NET_WEIGHT) NETWT,");
            Strsql.Append("    SUM(JSEC.CHARGEABLE_WEIGHT) CHARWT ,");
            Strsql.Append(" CRSTBL.remarks ");

            Strsql.Append("    FROM CR_CUST_SEA_EXP_TBL CRSTBL, INV_CUST_SEA_EXP_TBL INVCUSTEXP,");
            Strsql.Append("     CURRENCY_TYPE_MST_TBL CURRMST,");
            Strsql.Append("    JOB_CARD_TRN    JSE,");
            Strsql.Append("    JOB_TRN_CONT    JSEC,");
            Strsql.Append("    SHIPPING_TERMS_MST_TBL  STMST,");
            Strsql.Append("    BOOKING_MST_TBL         BAT,");
            Strsql.Append("    PLACE_MST_TBL           COLMST,");
            Strsql.Append("    PLACE_MST_TBL           DELMST,");
            Strsql.Append("    PORT_MST_TBL            POLMST,");
            Strsql.Append("    PORT_MST_TBL            PODMST,");
            Strsql.Append("    HBL_EXP_TBL            HBL,");
            Strsql.Append("    MBL_EXP_TBL            MBL,");
            Strsql.Append("    COMMODITY_GROUP_MST_TBL CGMST,");
            Strsql.Append("    CUSTOMER_MST_TBL      SHIPMST,");
            Strsql.Append("    CUSTOMER_CONTACT_DTLS SHIPDTLS,");
            Strsql.Append("    COUNTRY_MST_TBL       SHIPCOUNTRY,");
            Strsql.Append("    CUSTOMER_MST_TBL      CONSMST,");
            Strsql.Append("    CUSTOMER_CONTACT_DTLS CONSDTLS,");
            Strsql.Append("    COUNTRY_MST_TBL CONSCOUNTRY");

            Strsql.Append("    WHERE CRSTBL.INV_CUST_SEA_EXP_FK =INVCUSTEXP.INV_CUST_SEA_EXP_PK AND");
            Strsql.Append("    INVCUSTEXP.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK");
            Strsql.Append("    AND CURRMST.CURRENCY_MST_PK(+) = INVCUSTEXP.CURRENCY_MST_FK");
            Strsql.Append("    AND JSE.JOB_CARD_TRN_PK = JSEC.JOB_CARD_TRN_FK(+)");
            Strsql.Append("    AND STMST.SHIPPING_TERMS_MST_PK(+) = JSE.SHIPPING_TERMS_MST_FK");
            Strsql.Append("     AND BAT.BOOKING_MST_PK(+) = JSE.BOOKING_SEA_FK");
            Strsql.Append("    AND COLMST.PLACE_PK(+) = BAT.COL_PLACE_MST_FK");
            Strsql.Append("    AND DELMST.PLACE_PK(+) = BAT.DEL_PLACE_MST_FK");
            Strsql.Append("    AND POLMST.PORT_MST_PK = BAT.PORT_MST_POL_FK");
            Strsql.Append("    AND PODMST.PORT_MST_PK = BAT.PORT_MST_POD_FK");
            Strsql.Append("    AND HBL.HBL_EXP_TBL_PK(+) = JSE.hbl_hawb_fk");
            Strsql.Append("    AND MBL.MBL_EXP_TBL_PK(+) = JSE.mbl_mawb_fk");
            Strsql.Append("    AND CGMST.COMMODITY_GROUP_PK(+) = JSE.COMMODITY_GROUP_FK");
            Strsql.Append("    AND CONSMST.CUSTOMER_MST_PK(+) = JSE.CONSIGNEE_CUST_MST_FK");
            Strsql.Append("    AND CONSDTLS.CUSTOMER_MST_FK(+) = CONSMST.CUSTOMER_MST_PK");
            Strsql.Append("    AND CONSDTLS.ADM_COUNTRY_MST_FK = CONSCOUNTRY.COUNTRY_MST_PK(+)");
            Strsql.Append("    AND SHIPMST.CUSTOMER_MST_PK(+) = CRSTBL.SHIPPER_CUST_MST_FK");
            Strsql.Append("    AND SHIPDTLS.CUSTOMER_MST_FK(+) = SHIPMST.CUSTOMER_MST_PK");
            Strsql.Append("    AND SHIPDTLS.ADM_COUNTRY_MST_FK = SHIPCOUNTRY.COUNTRY_MST_PK(+)");
            Strsql.Append("    AND CRSTBL.CR_CUST_SEA_EXP_PK =" + InvPK);

            Strsql.Append("    GROUP BY CRSTBL.CR_CUST_SEA_EXP_PK,");
            Strsql.Append("    CRSTBL.CREDIT_NOTE_REF_NO,");
            Strsql.Append("    CRSTBL.CREDIT_NOTE_AMT ,");
            Strsql.Append("    INVCUSTEXP.INV_CUST_SEA_EXP_PK,");
            Strsql.Append("    INVCUSTEXP.INVOICE_REF_NO,");
            Strsql.Append("    JSE.JOB_CARD_TRN_PK,");
            Strsql.Append("    JSE.JOBCARD_REF_NO,");
            Strsql.Append("    TO_CHAR(JSE.ETD_DATE,'" + dateFormat + "'),");
            Strsql.Append("    TO_CHAR(JSE.ETA_DATE,'" + dateFormat + "'),");
            Strsql.Append("    BAT.CARGO_TYPE,");
            Strsql.Append("    SHIPMST.CUSTOMER_NAME,");
            Strsql.Append("    BAT.CUSTOMER_REF_NO,");
            Strsql.Append("    SHIPDTLS.ADM_ADDRESS_1,");
            Strsql.Append("    SHIPDTLS.ADM_ADDRESS_2,");
            Strsql.Append("    SHIPDTLS.ADM_ADDRESS_3,");
            Strsql.Append("    SHIPDTLS.ADM_CITY,");
            Strsql.Append("    SHIPDTLS.ADM_ZIP_CODE,");
            Strsql.Append("    SHIPDTLS.ADM_PHONE_NO_1,");
            Strsql.Append("    SHIPDTLS.ADM_FAX_NO,");
            Strsql.Append("    SHIPDTLS.ADM_EMAIL_ID,");
            Strsql.Append("    SHIPCOUNTRY.COUNTRY_NAME,");
            Strsql.Append("    SHIPMST.VAT_NO ,");
            Strsql.Append("    SHIPMST.CREDIT_DAYS,");
            Strsql.Append("    CONSMST.CUSTOMER_NAME    ,");
            Strsql.Append("    CONSDTLS.ADM_ADDRESS_1  ,");
            Strsql.Append("    CONSDTLS.ADM_ADDRESS_2 ,");
            Strsql.Append("    CONSDTLS.ADM_ADDRESS_3 ,");
            Strsql.Append("    CONSDTLS.ADM_CITY   ,");
            Strsql.Append("    CONSDTLS.ADM_ZIP_CODE ,");
            Strsql.Append("    CONSDTLS.ADM_PHONE_NO_1 ,");
            Strsql.Append("    CONSDTLS.ADM_FAX_NO  ,");
            Strsql.Append("    CONSDTLS.ADM_EMAIL_ID  ,");
            Strsql.Append("    CONSCOUNTRY.COUNTRY_NAME ,");
            Strsql.Append("    CURRMST.CURRENCY_ID,");
            Strsql.Append("    CURRMST.CURRENCY_NAME,");
            Strsql.Append("    (CASE WHEN JSE.VOYAGE IS NOT NULL THEN");
            Strsql.Append("    JSE.VESSEL_NAME || '-' || JSE.VOYAGE");
            Strsql.Append("    ELSE");
            Strsql.Append("    JSE.VESSEL_NAME END),");
            Strsql.Append("    JSE.PYMT_TYPE,");
            Strsql.Append("    JSE.GOODS_DESCRIPTION,");
            Strsql.Append("    JSE.MARKS_NUMBERS,");
            Strsql.Append("    JSE.INSURANCE_AMT,");
            Strsql.Append("    STMST.INCO_CODE,");
            Strsql.Append("    COLMST.PLACE_NAME,");
            Strsql.Append("    DELMST.PLACE_NAME,");
            Strsql.Append("    POLMST.PORT_NAME,");
            Strsql.Append("    PODMST.PORT_NAME,");
            Strsql.Append("    (CASE");
            Strsql.Append("     WHEN JSE.HBL_HAWB_FK IS NOT NULL THEN");
            Strsql.Append("    HBL.HBL_REF_NO");
            Strsql.Append("    ELSE");
            Strsql.Append("    MBL.MBL_REF_NO");
            Strsql.Append("    END),");
            Strsql.Append("    CGMST.COMMODITY_GROUP_DESC, ");
            Strsql.Append("    CRSTBL.remarks ");

            try
            {
                return ObjWk.GetDataSet(Strsql.ToString());
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

        #endregion " Fetch Report Data"

        #region " Fetch Container Details "

        /// <summary>
        /// Fetches the container details.
        /// </summary>
        /// <param name="CRPK">The CRPK.</param>
        /// <returns></returns>
        public DataSet FetchContainerDetails(long CRPK)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            strSqlBuilder.Append(" SELECT ICSE.INV_CUST_SEA_EXP_PK ,JTEC.CONTAINER_NUMBER CONTAINER");
            strSqlBuilder.Append(" FROM CR_CUST_SEA_EXP_TBL CRSTBL,INV_CUST_SEA_EXP_TBL ICSE,");
            strSqlBuilder.Append(" JOB_CARD_TRN  JSE,");
            strSqlBuilder.Append(" JOB_TRN_CONT JTEC");
            strSqlBuilder.Append(" WHERE(ICSE.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK)");
            strSqlBuilder.Append(" AND JTEC.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK");
            strSqlBuilder.Append(" AND CRSTBL.INV_CUST_SEA_EXP_FK=ICSE.INV_CUST_SEA_EXP_PK");
            strSqlBuilder.Append(" AND CRSTBL.CR_CUST_SEA_EXP_PK=" + CRPK + "");
            try
            {
                return (objWK.GetDataSet(strSqlBuilder.ToString()));
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

        #endregion " Fetch Container Details "

        #region "Credit Limit Used"

        /// <summary>
        /// Saves the credit limit.
        /// </summary>
        /// <param name="CrnAmt">The CRN amt.</param>
        /// <param name="Customer">The customer.</param>
        /// <param name="CrLimitUsed">The cr limit used.</param>
        /// <param name="TRAN">The tran.</param>
        public void SaveCreditLimit(double CrnAmt, string Customer, double CrLimitUsed, OracleTransaction TRAN)
        {
            WorkFlow objWK = new WorkFlow();
            Int16 exe = default(Int16);
            OracleCommand cmd = new OracleCommand();
            double temp = 0;
            string strSQL = null;
            temp = CrLimitUsed - CrnAmt;
            try
            {
                cmd.CommandType = CommandType.Text;
                cmd.Connection = TRAN.Connection;
                cmd.Transaction = TRAN;
                cmd.Parameters.Clear();
                strSQL = "update customer_mst_tbl a set a.credit_limit_used = " + temp;
                strSQL = strSQL + " where a.customer_name in ('" + Customer + "')";
                cmd.CommandText = strSQL;
                //exe = cmd.ExecuteNonQuery();
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

        #endregion "Credit Limit Used"
    }
}