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
    public class clsCreditNoteToAgentSeaImpEntry : CommonFeatures
    {
        #region " Enhance Search"

        /// <summary>
        /// Fetches the invoice no for credit.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchInvoiceNoForCredit(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;

            //arr = strCond.Split("~");
            //strReq = arr(0);
            //strSERACH_IN = arr(1);
            //strBizType = arr(2);

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandText = "";
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_INV_REF_NO_PKG.GET_INV_REF_CR_TO_AGENT_IMP";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
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
            finally
            {
                selectCommand.Connection.Close();
            }
        }

        #endregion " Enhance Search"

        #region " Fetch Function"

        /// <summary>
        /// Fetches the after save.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <returns></returns>
        public DataSet FetchAfterSave(string strPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();

            strSQL = " SELECT INV.JOB_CARD_FK ,";
            strSQL += " CR.CR_AGENT_PK,";
            strSQL += " INV.INV_AGENT_PK ,";
            strSQL += " INV.AGENT_MST_FK ,";
            strSQL += " INV.CURRENCY_MST_FK ,";
            strSQL += " JC.JOBCARD_REF_NO ,";
            strSQL += " INV.INVOICE_REF_NO ,";
            strSQL += " CR.CREDIT_NOTE_REF_NO,";
            strSQL += " CR.CREDIT_NOTE_DATE,";
            strSQL += " TO_CHAR(INV.INVOICE_DATE, '" + dateFormat + "') AS INV_DATE,";
            strSQL += " INV.GROSS_INV_AMT AS G_AMT,";
            strSQL += " (SELECT SUM(NVL(INVT.TAX_AMT,0)) FROM INV_AGENT_TRN_TBL INVT WHERE INVT.INV_AGENT_FK = INV.INV_AGENT_PK) AS VAT_AMT,";
            strSQL += " INV.DISCOUNT_AMT AS DISCOUNT,";
            strSQL += " INV.NET_INV_AMT AS INV_AMT,";
            strSQL += " CR.CREDIT_NOTE_AMT,";
            strSQL += " CR.REMARKS,";
            strSQL += " CUR.CURRENCY_ID,";
            strSQL += " AG.AGENT_NAME,";
            strSQL += " (SELECT SUM(CREDIT_NOTE_AMT) FROM CR_AGENT_TBL CRN WHERE CRN.INV_AGENT_FK=INV.INV_AGENT_PK) AS TOTAL_CREDIT_AMT, ";
            strSQL += "   UMTCRT.USER_NAME    AS CREATED_BY, ";
            strSQL += "   UMTUPD.USER_NAME    AS LAST_MODIFIED_BY, ";
            strSQL += "   UMTAPP.USER_NAME    AS APPROVED_BY, ";
            strSQL += "   TO_DATE(CR.CREATED_DT) CREATED_BY_DT, ";
            strSQL += "   TO_DATE(CR.LAST_MODIFIED_DT) LAST_MODIFIEDBY_DT, ";
            strSQL += "   TO_DATE(CR.LAST_MODIFIED_DT) APPROVED_DT ";
            strSQL += " FROM CR_AGENT_TBL CR, ";
            strSQL += " INV_AGENT_TBL INV,";
            strSQL += " JOB_CARD_TRN  JC,";
            strSQL += " AGENT_MST_TBL         AG,";
            strSQL += " CURRENCY_TYPE_MST_TBL CUR,";
            strSQL += "  USER_MST_TBL UMTCRT, ";
            strSQL += "  USER_MST_TBL UMTUPD, ";
            strSQL += "  USER_MST_TBL UMTAPP ";
            strSQL += " WHERE INV.JOB_CARD_FK = JC.JOB_CARD_TRN_PK(+)";
            strSQL += " AND INV.AGENT_MST_FK = AG.AGENT_MST_PK(+)";
            strSQL += " AND INV.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK(+)";
            strSQL += " AND CR.INV_AGENT_FK=INV.INV_AGENT_PK ";
            strSQL += " AND CR.CR_AGENT_PK=" + strPK;
            strSQL += " AND UMTCRT.USER_MST_PK(+) = CR.CREATED_BY_FK ";
            strSQL += " AND UMTUPD.USER_MST_PK(+) = CR.LAST_MODIFIED_BY_FK  ";
            strSQL += " AND UMTAPP.USER_MST_PK(+) = CR.LAST_MODIFIED_BY_FK  ";
            strSQL += " ORDER BY INV.INVOICE_REF_NO";

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

        #endregion " Fetch Function"

        #region " Fetch Report Data"

        /// <summary>
        /// Fetches the report data.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <param name="AgentFlag">The agent flag.</param>
        /// <returns></returns>
        public DataSet FetchReportData(long strPK, short AgentFlag = 1)
        {
            StringBuilder strSqlBuilder = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            strSqlBuilder.Append("  SELECT CRSTBL.CR_AGENT_PK CRPK,");
            strSqlBuilder.Append("  CRSTBL.CREDIT_NOTE_REF_NO CRREFNO,");
            strSqlBuilder.Append("  CRSTBL.CREDIT_NOTE_AMT CRAMT,");
            strSqlBuilder.Append("  INVAGTIMP.INV_AGENT_PK INVPK,");
            strSqlBuilder.Append("  INVAGTIMP.INVOICE_REF_NO       INVREFNO,");
            strSqlBuilder.Append("  JSI.JOB_CARD_TRN_PK        JOBPK,");
            strSqlBuilder.Append("  JSI.JOBCARD_REF_NO             JOBREFNO,");
            strSqlBuilder.Append("  JSI.CLEARANCE_ADDRESS          CLEARANCEPOINT,");
            strSqlBuilder.Append("  AMST.AGENT_NAME                AGENTNAME,");
            strSqlBuilder.Append("  AMST.ACCOUNT_NO                AGENTREFNO,");
            strSqlBuilder.Append("  ADTLS.ADM_ADDRESS_1            AGENTADD1,");
            strSqlBuilder.Append("  ADTLS.ADM_ADDRESS_2            AGENTADD2,");
            strSqlBuilder.Append("  ADTLS.ADM_ADDRESS_3            AGENTADD3,");
            strSqlBuilder.Append("  ADTLS.ADM_CITY                 AGENTCITY,");
            strSqlBuilder.Append("  ADTLS.ADM_ZIP_CODE             AGENTZIP,");
            strSqlBuilder.Append("  ADTLS.ADM_PHONE_NO_1           AGENTPHONE,");
            strSqlBuilder.Append("  ADTLS.ADM_FAX_NO               AGENTFAX,");
            strSqlBuilder.Append("  ADTLS.ADM_EMAIL_ID             AGENTEMAIL,");
            strSqlBuilder.Append("  AGTCOUNTRY.COUNTRY_NAME        AGENTCOUNTRY,");
            strSqlBuilder.Append("  SHIPMST.CUSTOMER_NAME    SHIPPER,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_ADDRESS_1   SHIPPERADD1,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_ADDRESS_2   SHIPPERADD2,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_ADDRESS_3   SHIPPERADD3,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_CITY        SHIPPERCITY,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_ZIP_CODE    SHIPPERZIP,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_PHONE_NO_1  SHIPPERPHONE,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_FAX_NO      SHIPPERFAX,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_EMAIL_ID    SHIPPEREMAIL,");
            strSqlBuilder.Append("  SHIPCOUNTRY.COUNTRY_NAME SHIPPERCOUNTRY,");
            strSqlBuilder.Append("  JSI.ETD_DATE             ETD,");
            strSqlBuilder.Append("  JSI.ETA_DATE             ETA,");
            strSqlBuilder.Append("  JSI.CARGO_TYPE           CARGO_TYPE,");
            strSqlBuilder.Append("  CURRMST.CURRENCY_ID      CURRID,");
            strSqlBuilder.Append("  CURRMST.CURRENCY_NAME    CURRNAME,");
            strSqlBuilder.Append("  (CASE");
            strSqlBuilder.Append("  WHEN JSI.VOYAGE IS NOT NULL THEN");
            strSqlBuilder.Append("  JSI.VESSEL_NAME || '-' || JSI.VOYAGE");
            strSqlBuilder.Append("  ELSE");
            strSqlBuilder.Append("   JSI.VOYAGE");
            strSqlBuilder.Append(" END) VES_FLIGHT,");
            strSqlBuilder.Append("  JSI.PYMT_TYPE   PYMT,");
            strSqlBuilder.Append("  JSI.GOODS_DESCRIPTION    GOODS,");
            strSqlBuilder.Append("  JSI.MARKS_NUMBERS        MARKS,");
            strSqlBuilder.Append("  NVL(JSI.INSURANCE_AMT, 0) INSURANCE,");
            strSqlBuilder.Append("  STMST.INCO_CODE           TERMS,");
            strSqlBuilder.Append("  AMST.VAT_NO              AGTVATNO,");
            strSqlBuilder.Append("  2 PAYMENTDAYS,");
            strSqlBuilder.Append("  ''                       COLPLACE,");
            strSqlBuilder.Append("  DELMST.PLACE_NAME        DELPLACE,");
            strSqlBuilder.Append("  POLMST.PORT_NAME         POL,");
            strSqlBuilder.Append("  PODMST.PORT_NAME         POD,");
            strSqlBuilder.Append("  (CASE");
            strSqlBuilder.Append("  WHEN JSI.HBL_REF_NO IS NOT NULL THEN");
            strSqlBuilder.Append("   JSI.HBL_REF_NO");
            strSqlBuilder.Append("  ELSE");
            strSqlBuilder.Append("   JSI.MBL_REF_NO");
            strSqlBuilder.Append("  END) BLREFNO,");
            strSqlBuilder.Append("  CGMST.COMMODITY_GROUP_DESC    COMMODITY,");
            strSqlBuilder.Append("  SUM(JSIC.VOLUME_IN_CBM)       VOLUME,");
            strSqlBuilder.Append("  SUM(JSIC.GROSS_WEIGHT)        GROSS,");
            strSqlBuilder.Append("  SUM(JSIC.NET_WEIGHT)          NETWT,");
            strSqlBuilder.Append("  SUM(JSIC.CHARGEABLE_WEIGHT)   CHARWT,");
            strSqlBuilder.Append("  CRSTBL.REMARKS  ");
            strSqlBuilder.Append(" FROM CR_AGENT_TBL CRSTBL, INV_AGENT_TBL INVAGTIMP,");
            strSqlBuilder.Append(" CURRENCY_TYPE_MST_TBL CURRMST,");
            strSqlBuilder.Append("  JOB_CARD_TRN   JSI,");
            strSqlBuilder.Append("  JOB_TRN_CONT   JSIC,");
            strSqlBuilder.Append("  SHIPPING_TERMS_MST_TBL STMST,");
            strSqlBuilder.Append("  PLACE_MST_TBL DELMST,");
            strSqlBuilder.Append("  PORT_MST_TBL  POLMST,");
            strSqlBuilder.Append("  PORT_MST_TBL  PODMST,");
            strSqlBuilder.Append("  COMMODITY_GROUP_MST_TBL CGMST,");
            strSqlBuilder.Append("  AGENT_MST_TBL      AMST,");
            strSqlBuilder.Append("  AGENT_CONTACT_DTLS ADTLS,");
            strSqlBuilder.Append("  COUNTRY_MST_TBL    AGTCOUNTRY,");
            strSqlBuilder.Append("  CUSTOMER_MST_TBL      SHIPMST,");
            strSqlBuilder.Append("  CUSTOMER_CONTACT_DTLS SHIPDTLS,");
            strSqlBuilder.Append("  COUNTRY_MST_TBL       SHIPCOUNTRY");

            strSqlBuilder.Append("  WHERE CRSTBL.INV_AGENT_FK=INVAGTIMP.INV_AGENT_PK AND");
            strSqlBuilder.Append("  INVAGTIMP.JOB_CARD_FK = JSI.JOB_CARD_TRN_PK AND");
            strSqlBuilder.Append("  CURRMST.CURRENCY_MST_PK(+) = INVAGTIMP.CURRENCY_MST_FK AND");
            strSqlBuilder.Append("  JSI.JOB_CARD_TRN_PK = JSIC.JOB_CARD_TRN_FK(+) AND");
            strSqlBuilder.Append("  STMST.SHIPPING_TERMS_MST_PK(+) = JSI.SHIPPING_TERMS_MST_FK");
            strSqlBuilder.Append("  AND DELMST.PLACE_PK(+) = JSI.DEL_PLACE_MST_FK AND");
            strSqlBuilder.Append("  POLMST.PORT_MST_PK = JSI.PORT_MST_POL_FK AND");
            strSqlBuilder.Append("  PODMST.PORT_MST_PK = JSI.PORT_MST_POD_FK");
            strSqlBuilder.Append("  AND CGMST.COMMODITY_GROUP_PK(+) = JSI.COMMODITY_GROUP_FK");

            if (AgentFlag == 1)
            {
                strSqlBuilder.Append("  AND AMST.AGENT_MST_PK(+) = JSI.cb_agent_mst_fk AND");
                strSqlBuilder.Append("  SHIPMST.CUSTOMER_MST_PK(+) = JSI.consignee_CUST_MST_FK AND");
            }
            else
            {
                strSqlBuilder.Append("  AND AMST.AGENT_MST_PK(+) = JSI.pol_agent_mst_fk ");
                strSqlBuilder.Append("  AND SHIPMST.CUSTOMER_MST_PK(+) = JSI.SHIPPER_CUST_MST_FK AND");
            }

            strSqlBuilder.Append("  ADTLS.AGENT_MST_FK(+) = AMST.AGENT_MST_PK AND");
            strSqlBuilder.Append("  AGTCOUNTRY.COUNTRY_MST_PK(+) = ADTLS.ADM_COUNTRY_MST_FK AND");
            strSqlBuilder.Append("  SHIPDTLS.CUSTOMER_MST_FK(+) = SHIPMST.CUSTOMER_MST_PK AND");
            strSqlBuilder.Append("  SHIPDTLS.ADM_COUNTRY_MST_FK = SHIPCOUNTRY.COUNTRY_MST_PK(+)");
            strSqlBuilder.Append("  AND CRSTBL.CAGENT_SEA_IMP_PK =" + strPK);
            strSqlBuilder.Append("  GROUP BY CRSTBL.CR_AGENT_SEA_IMP_PK ,");
            strSqlBuilder.Append("  CRSTBL.CREDIT_NOTE_REF_NO ,");
            strSqlBuilder.Append("  CRSTBL.CREDIT_NOTE_AMT,");
            strSqlBuilder.Append("  INVAGTIMP.INV_AGENT_PK,");
            strSqlBuilder.Append("  INVAGTIMP.INVOICE_REF_NO,");
            strSqlBuilder.Append("  JSI.JOB_CARD_TRN_PK,");
            strSqlBuilder.Append("  JSI.JOBCARD_REF_NO,");
            strSqlBuilder.Append("  JSI.CLEARANCE_ADDRESS,");
            strSqlBuilder.Append("  AMST.AGENT_NAME,");
            strSqlBuilder.Append("  AMST.ACCOUNT_NO,");
            strSqlBuilder.Append("  ADTLS.ADM_ADDRESS_1,");
            strSqlBuilder.Append("  ADTLS.ADM_ADDRESS_2,");
            strSqlBuilder.Append("  ADTLS.ADM_ADDRESS_3,");
            strSqlBuilder.Append("  ADTLS.ADM_CITY,");
            strSqlBuilder.Append("  ADTLS.ADM_ZIP_CODE,");
            strSqlBuilder.Append("  ADTLS.ADM_PHONE_NO_1,");
            strSqlBuilder.Append("  ADTLS.ADM_FAX_NO,");
            strSqlBuilder.Append("  ADTLS.ADM_EMAIL_ID,");
            strSqlBuilder.Append("  AGTCOUNTRY.COUNTRY_NAME,");
            strSqlBuilder.Append("  SHIPMST.CUSTOMER_NAME,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_ADDRESS_1,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_ADDRESS_2,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_ADDRESS_3,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_CITY,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_ZIP_CODE,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_PHONE_NO_1,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_FAX_NO,");
            strSqlBuilder.Append("  SHIPDTLS.ADM_EMAIL_ID,");
            strSqlBuilder.Append("  SHIPCOUNTRY.COUNTRY_NAME,");
            strSqlBuilder.Append("   JSI.ETD_DATE,");
            strSqlBuilder.Append("   JSI.ETA_DATE,");
            strSqlBuilder.Append("   JSI.CARGO_TYPE,");
            strSqlBuilder.Append("   CURRMST.CURRENCY_ID,");
            strSqlBuilder.Append("   CURRMST.CURRENCY_NAME,");
            strSqlBuilder.Append("   (CASE");
            strSqlBuilder.Append("     WHEN JSI.VOYAGE IS NOT NULL THEN");
            strSqlBuilder.Append("      JSI.VESSEL_NAME || '-' || JSI.VOYAGE");
            strSqlBuilder.Append("     ELSE");
            strSqlBuilder.Append("      JSI.VOYAGE");
            strSqlBuilder.Append("   END),");
            strSqlBuilder.Append("   JSI.PYMT_TYPE,");
            strSqlBuilder.Append("   JSI.GOODS_DESCRIPTION,");
            strSqlBuilder.Append("   JSI.MARKS_NUMBERS,");
            strSqlBuilder.Append("   JSI.INSURANCE_AMT,");
            strSqlBuilder.Append("   STMST.INCO_CODE,");
            strSqlBuilder.Append("   AMST.VAT_NO,");
            strSqlBuilder.Append("   DELMST.PLACE_NAME,");
            strSqlBuilder.Append("   POLMST.PORT_NAME,");
            strSqlBuilder.Append("   PODMST.PORT_NAME,");
            strSqlBuilder.Append("   (CASE");
            strSqlBuilder.Append("     WHEN JSI.HBL_REF_NO IS NOT NULL THEN");
            strSqlBuilder.Append("      JSI.HBL_REF_NO");
            strSqlBuilder.Append("     ELSE");
            strSqlBuilder.Append("      JSI.MBL_REF_NO");
            strSqlBuilder.Append("   END),");
            strSqlBuilder.Append("   CGMST.COMMODITY_GROUP_DESC, ");
            strSqlBuilder.Append("   CRSTBL.REMARKS  ");

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

        /// <summary>
        /// Fetches the container details.
        /// </summary>
        /// <param name="CRPK">The CRPK.</param>
        /// <returns></returns>
        public DataSet FetchContainerDetails(Int64 CRPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            strSQL = "SELECT JTSIC.CONTAINER_NUMBER  CONTAINER";
            strSQL += "FROM CR_AGENT_TBL CRSTBL,INV_AGENT_TBL IASI,";
            strSQL += "JOB_CARD_TRN  JSE,";
            strSQL += "JOB_TRN_CONT JTSIC";
            strSQL += "WHERE IASI.JOB_CARD_FK = JSE.JOB_CARD_TRN_PK";
            strSQL += "AND JTSIC.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK";
            strSQL += "AND CRSTBL.INV_AGENT_FK=IASI.INV_AGENT_PK";
            strSQL += "AND CRSTBL.CR_AGENT_PK=" + CRPK;
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

        #endregion " Fetch Report Data"
    }
}