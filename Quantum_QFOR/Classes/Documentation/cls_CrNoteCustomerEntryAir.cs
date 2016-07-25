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
    public class clsCrNoteCustomerEntryAir : CommonFeatures
    {
        #region " Fetch Functionality "

        /// <summary>
        /// Fetches the after save.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <returns></returns>
        public DataSet FetchAfterSave(string strPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            strSQL = "SELECT INV.JOB_CARD_AIR_EXP_FK ," + "INV.INV_CUST_AIR_EXP_PK," + "CR.CR_CUST_AIR_EXP_PK," + "JC.SHIPPER_CUST_MST_FK ," + "CR.CURRENCY_MST_FK ," + "JC.JOBCARD_REF_NO ," + "INV.INVOICE_REF_NO ," + "CR.CREDIT_NOTE_REF_NO," + "CR.CREDIT_NOTE_DATE," + "TO_CHAR(INV.INVOICE_DATE, '" + dateFormat + "') AS INV_DATE," + "INV.INVOICE_AMT AS G_AMT," + "(SELECT SUM(NVL(INVT.TAX_AMT,0)) FROM INV_CUST_TRN_AIR_EXP_TBL INVT WHERE INVT.INV_CUST_AIR_EXP_FK = INV.INV_CUST_AIR_EXP_PK) AS VAT_AMT," + "INV.DISCOUNT_AMT AS DISCOUNT," + "INV.NET_PAYABLE AS INV_AMT," + "CR.CREDIT_NOTE_AMT," + "CR.REMARKS," + "CUR.CURRENCY_ID," + "(SELECT SUM(CREDIT_NOTE_AMT) FROM CR_CUST_AIR_EXP_TBL CRN WHERE CRN.INV_CUST_AIR_EXP_FK=INV.INV_CUST_AIR_EXP_PK) AS TOTAL_CREDIT_AMT," + "CUST.CUSTOMER_NAME" + "FROM CR_CUST_AIR_EXP_TBL CR, " + "INV_CUST_AIR_EXP_TBL INV," + "JOB_CARD_TRN  JC," + "CUSTOMER_MST_TBL         CUST," + "CURRENCY_TYPE_MST_TBL CUR" + "WHERE INV.JOB_CARD_AIR_EXP_FK = JC.JOB_CARD_TRN_PK(+)" + "AND JC.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)" + "AND CR.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK(+)" + "AND CR.INV_CUST_AIR_EXP_FK=INV.INV_CUST_AIR_EXP_PK " + "AND CR.CR_CUST_AIR_EXP_PK=" + strPK;

            try
            {
                return objWK.GetDataSet(strSQL);
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

        #endregion " Fetch Functionality "

        #region " Fetch Report Data "

        /// <summary>
        /// Fetches the report data.
        /// </summary>
        /// <param name="CRPK">The CRPK.</param>
        /// <returns></returns>
        public DataSet FetchReportData(long CRPK)
        {
            StringBuilder StrSqlBuilder = new StringBuilder();
            WorkFlow ObjWk = new WorkFlow();
            StrSqlBuilder.Append(" SELECT  CRSTBL.CR_CUST_AIR_EXP_PK CRPK,");
            StrSqlBuilder.Append(" CRSTBL.CREDIT_NOTE_REF_NO CRREFNO,");
            StrSqlBuilder.Append("  CRSTBL.CREDIT_NOTE_AMT CRAMT,");
            StrSqlBuilder.Append("  INVCUSTEXP.INV_CUST_AIR_EXP_PK INVPK,");
            StrSqlBuilder.Append("  INVCUSTEXP.INVOICE_REF_NO       INVREFNO,");
            StrSqlBuilder.Append("  JSE.JOB_CARD_TRN_PK        JOBPK,");
            StrSqlBuilder.Append("  JSE.JOBCARD_REF_NO             JOBREFNO,");
            StrSqlBuilder.Append("  '' CLEARANCEPOINT,");
            StrSqlBuilder.Append("  TO_CHAR(JSE.ETD_DATE,'" + dateFormat + "') ETD,");
            StrSqlBuilder.Append("  TO_CHAR(JSE.ETA_DATE,'" + dateFormat + "') ETA,");
            StrSqlBuilder.Append("  BAT.CARGO_TYPE CARGO_TYPE,");
            StrSqlBuilder.Append("  SHIPMST.CUSTOMER_NAME    AGENTNAME,");
            StrSqlBuilder.Append("  BAT.CUSTOMER_REF_NO  AGENTREFNO,");
            StrSqlBuilder.Append("  SHIPDTLS.ADM_ADDRESS_1   AGENTADD1,");
            StrSqlBuilder.Append("  SHIPDTLS.ADM_ADDRESS_2   AGENTADD2,");
            StrSqlBuilder.Append("  SHIPDTLS.ADM_ADDRESS_3   AGENTADD3,");
            StrSqlBuilder.Append("  SHIPDTLS.ADM_CITY        AGENTCITY,");
            StrSqlBuilder.Append("  SHIPDTLS.ADM_ZIP_CODE    AGENTZIP,");
            StrSqlBuilder.Append("  SHIPDTLS.ADM_PHONE_NO_1  AGENTPHONE,");
            StrSqlBuilder.Append("  SHIPDTLS.ADM_FAX_NO      AGENTFAX,");
            StrSqlBuilder.Append("  SHIPDTLS.ADM_EMAIL_ID    AGENTEMAIL,");
            StrSqlBuilder.Append("  SHIPCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,");
            StrSqlBuilder.Append("  CONSMST.CUSTOMER_NAME    SHIPPER,");
            StrSqlBuilder.Append("  CONSDTLS.ADM_ADDRESS_1   SHIPPERADD1,");
            StrSqlBuilder.Append("  CONSDTLS.ADM_ADDRESS_2   SHIPPERADD2,");
            StrSqlBuilder.Append("  CONSDTLS.ADM_ADDRESS_3   SHIPPERADD3,");
            StrSqlBuilder.Append("  CONSDTLS.ADM_CITY        SHIPPERCITY,");
            StrSqlBuilder.Append("  CONSDTLS.ADM_ZIP_CODE    SHIPPERZIP,");
            StrSqlBuilder.Append("  CONSDTLS.ADM_PHONE_NO_1  SHIPPERPHONE,");
            StrSqlBuilder.Append("  CONSDTLS.ADM_FAX_NO      SHIPPERFAX,");
            StrSqlBuilder.Append("  CONSDTLS.ADM_EMAIL_ID    SHIPPEREMAIL,");
            StrSqlBuilder.Append("  CONSCOUNTRY.COUNTRY_NAME SHIPPERCOUNTRY,");
            StrSqlBuilder.Append(" CURRMST.CURRENCY_ID CURRID,");
            StrSqlBuilder.Append(" CURRMST.CURRENCY_NAME CURRNAME,");
            StrSqlBuilder.Append(" JSE.FLIGHT_NO VES_FLIGHT,");
            StrSqlBuilder.Append(" JSE.PYMT_TYPE PYMT,");
            StrSqlBuilder.Append(" JSE.GOODS_DESCRIPTION GOODS,");
            StrSqlBuilder.Append(" JSE.MARKS_NUMBERS MARKS,");
            StrSqlBuilder.Append(" NVL(JSE.INSURANCE_AMT, 0) INSURANCE,");
            StrSqlBuilder.Append(" STMST.INCO_CODE TERMS,");
            StrSqlBuilder.Append(" SHIPMST.VAT_NO AGTVATNO,");
            StrSqlBuilder.Append(" SHIPMST.CREDIT_DAYS PAYMENTDAYS,");
            StrSqlBuilder.Append(" COLMST.PLACE_NAME COLPLACE,");
            StrSqlBuilder.Append(" DELMST.PLACE_NAME DELPLACE,");
            StrSqlBuilder.Append(" POLMST.PORT_NAME POL,");
            StrSqlBuilder.Append(" PODMST.PORT_NAME POD,");
            StrSqlBuilder.Append(" (CASE");
            StrSqlBuilder.Append("  WHEN JSE.HAWB_EXP_TBL_FK IS NOT NULL THEN");
            StrSqlBuilder.Append("  HAWB.HAWB_REF_NO");
            StrSqlBuilder.Append(" ELSE");
            StrSqlBuilder.Append(" MAWB.MAWB_REF_NO");
            StrSqlBuilder.Append(" END) BLREFNO,");
            StrSqlBuilder.Append("  CGMST.COMMODITY_GROUP_DESC COMMODITY,");
            StrSqlBuilder.Append(" SUM(JSEC.VOLUME_IN_CBM) VOLUME,");
            StrSqlBuilder.Append(" SUM(JSEC.GROSS_WEIGHT) GROSS,");
            // StrSqlBuilder.Append(" SUM(JSEC.NET_WEIGHT) NETWT,")
            StrSqlBuilder.Append(" 0 NETWT,");
            StrSqlBuilder.Append(" SUM(JSEC.CHARGEABLE_WEIGHT) CHARWT,");
            StrSqlBuilder.Append(" CRSTBL.Remarks ");

            StrSqlBuilder.Append(" FROM CR_CUST_AIR_EXP_TBL CRSTBL, INV_CUST_AIR_EXP_TBL INVCUSTEXP,");
            StrSqlBuilder.Append("  CURRENCY_TYPE_MST_TBL CURRMST,");

            StrSqlBuilder.Append(" JOB_CARD_TRN    JSE,");
            StrSqlBuilder.Append(" JOB_TRN_CONT    JSEC,");

            StrSqlBuilder.Append(" SHIPPING_TERMS_MST_TBL  STMST,");
            StrSqlBuilder.Append(" BOOKING_MST_TBL         BAT,");
            StrSqlBuilder.Append(" PLACE_MST_TBL           COLMST,");
            StrSqlBuilder.Append(" PLACE_MST_TBL           DELMST,");
            StrSqlBuilder.Append(" PORT_MST_TBL            POLMST,");
            StrSqlBuilder.Append(" PORT_MST_TBL            PODMST,");
            StrSqlBuilder.Append(" HAWB_EXP_TBL            HAWB,");
            StrSqlBuilder.Append(" MAWB_EXP_TBL            MAWB,");
            StrSqlBuilder.Append(" COMMODITY_GROUP_MST_TBL CGMST,");

            StrSqlBuilder.Append(" CUSTOMER_MST_TBL      SHIPMST,");
            StrSqlBuilder.Append(" CUSTOMER_CONTACT_DTLS SHIPDTLS,");
            StrSqlBuilder.Append(" COUNTRY_MST_TBL       SHIPCOUNTRY,");

            StrSqlBuilder.Append(" CUSTOMER_MST_TBL      CONSMST,");
            StrSqlBuilder.Append(" CUSTOMER_CONTACT_DTLS CONSDTLS,");
            StrSqlBuilder.Append(" COUNTRY_MST_TBL CONSCOUNTRY");

            StrSqlBuilder.Append(" WHERE CRSTBL.INV_CUST_AIR_EXP_FK =INVCUSTEXP.INV_CUST_AIR_EXP_PK AND");
            StrSqlBuilder.Append(" INVCUSTEXP.JOB_CARD_AIR_EXP_FK = JSE.JOB_CARD_TRN_PK");
            StrSqlBuilder.Append(" AND CURRMST.CURRENCY_MST_PK(+) = INVCUSTEXP.CURRENCY_MST_FK");

            StrSqlBuilder.Append(" AND JSE.JOB_CARD_TRN_PK = JSEC.JOB_CARD_TRN_FK");
            StrSqlBuilder.Append(" AND STMST.SHIPPING_TERMS_MST_PK(+) = JSE.SHIPPING_TERMS_MST_FK");
            StrSqlBuilder.Append("  AND BAT.BOOKING_MST_PK(+) = JSE.BOOKING_MST_FK");
            StrSqlBuilder.Append(" AND COLMST.PLACE_PK(+) = BAT.COL_PLACE_MST_FK");
            StrSqlBuilder.Append(" AND DELMST.PLACE_PK(+) = BAT.DEL_PLACE_MST_FK");
            StrSqlBuilder.Append(" AND POLMST.PORT_MST_PK = BAT.PORT_MST_POL_FK");
            StrSqlBuilder.Append(" AND PODMST.PORT_MST_PK = BAT.PORT_MST_POD_FK");
            StrSqlBuilder.Append(" AND HAWB.HAWB_EXP_TBL_PK(+) = JSE.HBL_HAWB_FK");
            StrSqlBuilder.Append(" AND MAWB.MAWB_EXP_TBL_PK(+) = JSE.MBL_MAWB_FK");
            StrSqlBuilder.Append(" AND CGMST.COMMODITY_GROUP_PK(+) = JSE.COMMODITY_GROUP_FK");
            StrSqlBuilder.Append(" AND CONSMST.CUSTOMER_MST_PK(+) = JSE.CONSIGNEE_CUST_MST_FK");
            StrSqlBuilder.Append(" AND CONSDTLS.CUSTOMER_MST_FK(+) = CONSMST.CUSTOMER_MST_PK");
            StrSqlBuilder.Append(" AND CONSDTLS.ADM_COUNTRY_MST_FK = CONSCOUNTRY.COUNTRY_MST_PK(+)");
            StrSqlBuilder.Append(" AND SHIPMST.CUSTOMER_MST_PK(+) = CRSTBL.SHIPPER_CUST_MST_FK");
            StrSqlBuilder.Append(" AND SHIPDTLS.CUSTOMER_MST_FK(+) = SHIPMST.CUSTOMER_MST_PK");
            StrSqlBuilder.Append(" AND SHIPDTLS.ADM_COUNTRY_MST_FK = SHIPCOUNTRY.COUNTRY_MST_PK(+)");
            StrSqlBuilder.Append(" AND CRSTBL.CR_CUST_AIR_EXP_PK =" + CRPK);
            StrSqlBuilder.Append(" GROUP BY CRSTBL.CR_CUST_AIR_EXP_PK,");
            StrSqlBuilder.Append(" CRSTBL.CREDIT_NOTE_REF_NO,");
            StrSqlBuilder.Append(" CRSTBL.CREDIT_NOTE_AMT ,");
            StrSqlBuilder.Append(" INVCUSTEXP.INV_CUST_AIR_EXP_PK,");
            StrSqlBuilder.Append(" INVCUSTEXP.INVOICE_REF_NO,");
            StrSqlBuilder.Append(" JSE.JOB_CARD_TRN_PK,");
            StrSqlBuilder.Append(" JSE.JOBCARD_REF_NO,");
            StrSqlBuilder.Append("  TO_CHAR(JSE.ETD_DATE,'" + dateFormat + "'),");
            StrSqlBuilder.Append("  TO_CHAR(JSE.ETA_DATE,'" + dateFormat + "'),");
            StrSqlBuilder.Append(" BAT.CARGO_TYPE,");
            StrSqlBuilder.Append("  SHIPMST.CUSTOMER_NAME,");
            StrSqlBuilder.Append(" BAT.CUSTOMER_REF_NO,");
            StrSqlBuilder.Append(" SHIPDTLS.ADM_ADDRESS_1,");
            StrSqlBuilder.Append(" SHIPDTLS.ADM_ADDRESS_2,");
            StrSqlBuilder.Append(" SHIPDTLS.ADM_ADDRESS_3,");
            StrSqlBuilder.Append(" SHIPDTLS.ADM_CITY,");
            StrSqlBuilder.Append(" SHIPDTLS.ADM_ZIP_CODE,");
            StrSqlBuilder.Append(" SHIPDTLS.ADM_PHONE_NO_1,");
            StrSqlBuilder.Append(" SHIPDTLS.ADM_FAX_NO,");
            StrSqlBuilder.Append(" SHIPDTLS.ADM_EMAIL_ID,");
            StrSqlBuilder.Append(" SHIPCOUNTRY.COUNTRY_NAME,");
            StrSqlBuilder.Append(" SHIPMST.VAT_NO ,");
            StrSqlBuilder.Append(" SHIPMST.CREDIT_DAYS,");
            StrSqlBuilder.Append(" CONSMST.CUSTOMER_NAME    ,");
            StrSqlBuilder.Append(" CONSDTLS.ADM_ADDRESS_1  ,");
            StrSqlBuilder.Append(" CONSDTLS.ADM_ADDRESS_2 ,");
            StrSqlBuilder.Append(" CONSDTLS.ADM_ADDRESS_3 ,");
            StrSqlBuilder.Append(" CONSDTLS.ADM_CITY   ,");
            StrSqlBuilder.Append(" CONSDTLS.ADM_ZIP_CODE ,");
            StrSqlBuilder.Append(" CONSDTLS.ADM_PHONE_NO_1 ,");
            StrSqlBuilder.Append(" CONSDTLS.ADM_FAX_NO  ,");
            StrSqlBuilder.Append(" CONSDTLS.ADM_EMAIL_ID  ,");
            StrSqlBuilder.Append(" CONSCOUNTRY.COUNTRY_NAME ,");
            StrSqlBuilder.Append(" CURRMST.CURRENCY_ID,");
            StrSqlBuilder.Append(" CURRMST.CURRENCY_NAME,");
            StrSqlBuilder.Append(" JSE.FLIGHT_NO,");
            StrSqlBuilder.Append(" JSE.PYMT_TYPE,");
            StrSqlBuilder.Append(" JSE.GOODS_DESCRIPTION,");
            StrSqlBuilder.Append(" JSE.MARKS_NUMBERS,");
            StrSqlBuilder.Append(" JSE.INSURANCE_AMT,");
            StrSqlBuilder.Append(" STMST.INCO_CODE,");
            StrSqlBuilder.Append(" COLMST.PLACE_NAME,");
            StrSqlBuilder.Append(" DELMST.PLACE_NAME,");
            StrSqlBuilder.Append("  POLMST.PORT_NAME,");
            StrSqlBuilder.Append("   PODMST.PORT_NAME,");
            StrSqlBuilder.Append("   (CASE");
            StrSqlBuilder.Append("  WHEN JSE.HAWB_EXP_TBL_FK IS NOT NULL THEN");
            StrSqlBuilder.Append("  HAWB.HAWB_REF_NO");
            StrSqlBuilder.Append("   ELSE");
            StrSqlBuilder.Append("   MAWB.MAWB_REF_NO");
            StrSqlBuilder.Append("   END),");
            StrSqlBuilder.Append("  CGMST.COMMODITY_GROUP_DESC , ");
            StrSqlBuilder.Append("  CRSTBL.Remarks ");

            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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

        #endregion " Fetch Report Data "

        #region " Fetch Container Details "

        /// <summary>
        /// Fetches the container details.
        /// </summary>
        /// <param name="CRPK">The CRPK.</param>
        /// <returns></returns>
        public DataSet FetchContainerDetails(long CRPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            strSQL = "SELECT ICSE.INV_CUST_AIR_EXP_PK ,JTEC.PALETTE_SIZE CONTAINER FROM CR_CUST_AIR_EXP_TBL CRSTBL,INV_CUST_AIR_EXP_TBL ICSE,  JOB_CARD_TRN  JSE,  JOB_TRN_AIR_EXP_CONT JTEC  WHERE(ICSE.JOB_CARD_AIR_EXP_FK = JSE.JOB_CARD_TRN_PK)  AND JTEC.JOB_CARD_AIR_EXP_FK = JSE.JOB_CARD_TRN_PK  AND CRSTBL.INV_CUST_AIR_EXP_FK=ICSE.INV_CUST_AIR_EXP_PK  AND CRSTBL.CR_CUST_AIR_EXP_PK=" + CRPK;

            try
            {
                return (objWK.GetDataSet(strSQL));
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
        /// <param name="TRAN">The tran.</param>
        public void SaveCreditLimit(double CrnAmt, string Customer, OracleTransaction TRAN)
        {
            WorkFlow objWK = new WorkFlow();
            Int16 exe = default(Int16);
            OracleCommand cmd = new OracleCommand();
            string strSQL = null;
            try
            {
                cmd.CommandType = CommandType.Text;
                cmd.Connection = TRAN.Connection;
                cmd.Transaction = TRAN;

                cmd.Parameters.Clear();
                strSQL = "update customer_mst_tbl a set a.credit_limit_used = a.credit_limit_used - " + CrnAmt;
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