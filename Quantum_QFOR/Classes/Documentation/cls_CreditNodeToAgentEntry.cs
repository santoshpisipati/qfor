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
    public class cls_CreditNoteToAgentEntry : CommonFeatures
    {
        #region " Enhance Search Function"

        /// <summary>
        /// Fetches the invoice no for credit.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchInvoiceNoForCredit(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strAgentType = null;
            string strProcessType = null;
            string strLoc = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            strProcessType = Convert.ToString(arr.GetValue(3));
            strAgentType = Convert.ToString(arr.GetValue(4));
            strLoc = loc;
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandText = "";
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_INV_REF_NO_PKG.GET_INV_REF_CR_TO_AGENT_NEW";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input;
                _with1.Add("AGENTTYPE_IN", strAgentType).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 10000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
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
                selectCommand.Connection.Close();
            }
        }

        #endregion " Enhance Search Function"

        #region " Fetch Functionality"

        /// <summary>
        /// Fetches the after save.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <param name="AgentType">Type of the agent.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchAfterSave(string strPK, short AgentType = 0, short BizType = 0, short ProcessType = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            strSQL.Append(" SELECT INV.JOB_CARD_FK , ");
            strSQL.Append(" CR.CR_AGENT_PK,");
            strSQL.Append(" INV.INV_AGENT_PK ,");
            strSQL.Append(" INV.AGENT_MST_FK ,");
            strSQL.Append(" INV.CURRENCY_MST_FK ,");
            strSQL.Append(" JC.JOBCARD_REF_NO ,");
            strSQL.Append(" INV.INVOICE_REF_NO ,");
            strSQL.Append(" CR.CREDIT_NOTE_REF_NO,");
            strSQL.Append(" CR.CREDIT_NOTE_DATE,");
            strSQL.Append(" TO_CHAR(INV.INVOICE_DATE, '" + dateFormat + "') AS INV_DATE,");
            strSQL.Append(" INV.GROSS_INV_AMT AS G_AMT,");
            strSQL.Append(" (SELECT SUM(NVL(INVT.TAX_AMT,0))  FROM INV_AGENT_TRN_TBL INVT  WHERE INVT.INV_AGENT_FK = INV.INV_AGENT_PK) AS VAT_AMT,");
            strSQL.Append(" INV.DISCOUNT_AMT AS DISCOUNT,");
            strSQL.Append(" (SELECT sum(nvl(it.amt_in_inv_curr,0)) FROM inv_agent_trn_tbl it where it.inv_agent_fk=inv.inv_agent_pk) AS INV_AMT,");
            strSQL.Append(" CR.CREDIT_NOTE_AMT,");
            strSQL.Append(" CR.REMARKS,");
            strSQL.Append(" CUR.CURRENCY_ID,");
            strSQL.Append("    AG.AGENT_NAME,");
            strSQL.Append(" (SELECT SUM(CR.CREDIT_NOTE_AMT) ");
            strSQL.Append(" FROM CR_AGENT_TBL CR ");
            strSQL.Append(" WHERE CR.INV_AGENT_FK = INV.INV_AGENT_PK) AS CREDITAMT, ");
            strSQL.Append("   UMTCRT.USER_NAME    AS CREATED_BY, ");
            strSQL.Append("   UMTUPD.USER_NAME    AS LAST_MODIFIED_BY, ");
            strSQL.Append("   UMTAPP.USER_NAME    AS APPROVED_BY, ");
            strSQL.Append("   TO_DATE(CR.CREATED_DT) CREATED_BY_DT, ");
            strSQL.Append("   TO_DATE(CR.LAST_MODIFIED_DT) LAST_MODIFIEDBY_DT, ");
            strSQL.Append("   TO_DATE(CR.LAST_MODIFIED_DT) APPROVED_DT ");
            strSQL.Append(" FROM CR_AGENT_TBL CR, ");
            strSQL.Append("   INV_AGENT_TBL INV,");
            strSQL.Append(" JOB_CARD_TRN  JC,");
            strSQL.Append(" AGENT_MST_TBL         AG,");
            strSQL.Append(" CURRENCY_TYPE_MST_TBL CUR, ");
            strSQL.Append("  USER_MST_TBL UMTCRT, ");
            strSQL.Append("  USER_MST_TBL UMTUPD, ");
            strSQL.Append("  USER_MST_TBL UMTAPP ");
            strSQL.Append(" WHERE INV.JOB_CARD_FK = JC.JOB_CARD_TRN_PK(+)");
            strSQL.Append(" AND INV.AGENT_MST_FK = AG.AGENT_MST_PK(+)");
            strSQL.Append(" AND INV.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK(+)");
            strSQL.Append(" AND CR.INV_AGENT_FK=INV.INV_AGENT_PK ");
            strSQL.Append(" AND CR.CR_AGENT_PK= " + strPK + "");
            strSQL.Append(" AND UMTCRT.USER_MST_PK(+) = CR.CREATED_BY_FK ");
            strSQL.Append(" AND UMTUPD.USER_MST_PK(+) = CR.LAST_MODIFIED_BY_FK  ");
            strSQL.Append(" AND UMTAPP.USER_MST_PK(+) = CR.LAST_MODIFIED_BY_FK  ");
            strSQL.Append(" ORDER BY INV.INVOICE_REF_NO");
            //Testing purpose only''Remove once invoice is completed
            try
            {
                return objWK.GetDataSet(strSQL.ToString());
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
        public DataSet FetchReportData(long strPK, short AgentFlag = 1)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWK = new WorkFlow();
            AgentFlag = 1; //Agent
            AgentFlag = 2; //Agent

            strSQL.Append(" SELECT ");
            strSQL.Append("     BAT.customer_ref_no CUSTREF, ");
            strSQL.Append("     CRSTBL.CR_AGENT_PK CBPK, ");
            strSQL.Append("     CRSTBL.CREDIT_NOTE_REF_NO CRREFNO,");
            strSQL.Append("     CRSTBL.CREDIT_NOTE_AMT CRAMT,");
            strSQL.Append("     INVAGTEXP.INV_AGENT_PK INVPK,");
            strSQL.Append("     INVAGTEXP.INVOICE_REF_NO       INVREFNO,");
            strSQL.Append("     JSE.JOB_CARD_TRN_PK        JOBPK,");
            strSQL.Append("     JSE.JOBCARD_REF_NO             JOBREFNO,");
            strSQL.Append("     '' CLEARANCEPOINT,");
            strSQL.Append("     AMST.AGENT_NAME         AGENTNAME,");
            strSQL.Append("     AMST.ACCOUNT_NO        AGENTREFNO,");
            strSQL.Append("     ADTLS.ADM_ADDRESS_1     AGENTADD1,");
            strSQL.Append("     ADTLS.ADM_ADDRESS_2     AGENTADD2,");
            strSQL.Append("     ADTLS.ADM_ADDRESS_3     AGENTADD3,");
            strSQL.Append("     ADTLS.ADM_CITY          AGENTCITY,");
            strSQL.Append("     ADTLS.ADM_ZIP_CODE      AGENTZIP,");
            strSQL.Append("     ADTLS.ADM_PHONE_NO_1    AGENTPHONE,");
            strSQL.Append("     ADTLS.ADM_FAX_NO        AGENTFAX,");
            strSQL.Append("     ADTLS.ADM_EMAIL_ID      AGENTEMAIL,");
            strSQL.Append("     AGTCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,");

            strSQL.Append("      SHIPMST.CUSTOMER_NAME    SHIPPER,");
            strSQL.Append("      SHIPDTLS.ADM_ADDRESS_1   SHIPPERADD1,");
            strSQL.Append("     SHIPDTLS.ADM_ADDRESS_2   SHIPPERADD2,");
            strSQL.Append("     SHIPDTLS.ADM_ADDRESS_3   SHIPPERADD3,");
            strSQL.Append("     SHIPDTLS.ADM_CITY        SHIPPERCITY,");
            strSQL.Append("     SHIPDTLS.ADM_ZIP_CODE    SHIPPERZIP,");
            strSQL.Append("     SHIPDTLS.ADM_PHONE_NO_1  SHIPPERPHONE,");
            strSQL.Append("     SHIPDTLS.ADM_FAX_NO      SHIPPERFAX,");
            strSQL.Append("     SHIPDTLS.ADM_EMAIL_ID    SHIPPEREMAIL,");
            strSQL.Append("     SHIPCOUNTRY.COUNTRY_NAME SHIPPERCOUNTRY,");
            strSQL.Append("     TO_CHAR(JSE.ETD_DATE,'" + dateFormat + "') ETD,");
            strSQL.Append("     TO_CHAR(JSE.ETA_DATE,'" + dateFormat + "') ETA ,");
            strSQL.Append("     BAT.CARGO_TYPE CARGO_TYPE,");
            strSQL.Append("     CURRMST.CURRENCY_ID CURRID,");
            strSQL.Append("     CURRMST.CURRENCY_NAME CURRNAME,");
            strSQL.Append("     (CASE WHEN JSE.VOYAGE IS NOT NULL THEN");
            strSQL.Append("     JSE.VESSEL_NAME || '-' || JSE.VOYAGE ELSE");
            strSQL.Append("     JSE.VOYAGE END) VES_FLIGHT,");
            strSQL.Append("     JSE.PYMT_TYPE PYMT,");
            strSQL.Append("     JSE.GOODS_DESCRIPTION GOODS,");
            strSQL.Append("     JSE.MARKS_NUMBERS MARKS,");
            strSQL.Append("     NVL(JSE.INSURANCE_AMT, 0) INSURANCE,");
            strSQL.Append("     STMST.INCO_CODE TERMS,");
            strSQL.Append("     AMST.VAT_NO AGTVATNO,");
            strSQL.Append("     2 PAYMENTDAYS,");
            strSQL.Append("     COLMST.PLACE_NAME COLPLACE,");
            strSQL.Append("     DELMST.PLACE_NAME DELPLACE,");
            strSQL.Append("     POLMST.PORT_NAME POL,");
            strSQL.Append("     PODMST.PORT_NAME POD,");
            strSQL.Append("     (CASE");
            strSQL.Append("     WHEN JSE.HBL_EXP_TBL_FK IS NOT NULL THEN");
            strSQL.Append("         HBL.HBL_REF_NO");
            strSQL.Append("         ELSE");
            strSQL.Append("         MBL.MBL_REF_NO");
            strSQL.Append("     END) BLREFNO,");
            strSQL.Append("     CGMST.COMMODITY_GROUP_DESC COMMODITY,");
            strSQL.Append("     SUM(JSEC.VOLUME_IN_CBM) VOLUME,");
            strSQL.Append("     SUM(JSEC.GROSS_WEIGHT) GROSS,");
            strSQL.Append("     SUM(JSEC.NET_WEIGHT) NETWT,");
            strSQL.Append("     SUM(JSEC.CHARGEABLE_WEIGHT) CHARWT,");
            strSQL.Append("     CRSTBL.REMARKS ");

            strSQL.Append("    FROM CR_AGENT_TBL CRSTBL, INV_AGENT_TBL INVAGTEXP,");
            strSQL.Append("         CURRENCY_TYPE_MST_TBL CURRMST,");
            strSQL.Append("         JOB_CARD_TRN    JSE,");
            strSQL.Append("         JOB_TRN_CONT    JSEC,");
            strSQL.Append("         SHIPPING_TERMS_MST_TBL  STMST,");
            strSQL.Append("         BOOKING_MST_TBL         BAT,");
            strSQL.Append("         PLACE_MST_TBL           COLMST,");
            strSQL.Append("         PLACE_MST_TBL           DELMST,");
            strSQL.Append("         PORT_MST_TBL            POLMST,");
            strSQL.Append("         PORT_MST_TBL            PODMST,");
            strSQL.Append("         HBL_EXP_TBL            HBL,");
            strSQL.Append("         MBL_EXP_TBL            MBL,");
            strSQL.Append("         COMMODITY_GROUP_MST_TBL CGMST,");

            strSQL.Append("   AGENT_MST_TBL      AMST,");
            strSQL.Append("   AGENT_CONTACT_DTLS ADTLS,");
            strSQL.Append("   COUNTRY_MST_TBL    AGTCOUNTRY,");

            strSQL.Append("   CUSTOMER_MST_TBL      SHIPMST,");
            strSQL.Append("   CUSTOMER_CONTACT_DTLS SHIPDTLS,");
            strSQL.Append("    COUNTRY_MST_TBL SHIPCOUNTRY");

            strSQL.Append("   WHERE CRSTBL.INV_AGENT_FK=INVAGTEXP.INV_AGENT_PK");
            strSQL.Append("   AND INVAGTEXP.JOB_CARD_FK = JSE.JOB_CARD_TRN_PK");
            strSQL.Append("   AND CURRMST.CURRENCY_MST_PK(+) = INVAGTEXP.CURRENCY_MST_FK");
            strSQL.Append("   AND JSE.JOB_CARD_TRN_PK = JSEC.JOB_CARD_TRN_FK");

            if ((AgentFlag == 1))
            {
                strSQL.Append(" AND SHIPMST.CUSTOMER_MST_PK(+) = JSE.Shipper_Cust_Mst_Fk");
            }
            else
            {
                strSQL.Append(" AND SHIPMST.CUSTOMER_MST_PK(+) = JSE.consignee_cust_mst_fk");
            }

            strSQL.Append("   AND STMST.SHIPPING_TERMS_MST_PK(+) = JSE.SHIPPING_TERMS_MST_FK");
            strSQL.Append("   AND BAT.BOOKING_SEA_PK(+) = JSE.BOOKING_SEA_FK");
            strSQL.Append("   AND COLMST.PLACE_PK(+) = BAT.COL_PLACE_MST_FK");
            strSQL.Append("   AND DELMST.PLACE_PK(+) = BAT.DEL_PLACE_MST_FK");
            strSQL.Append("   AND POLMST.PORT_MST_PK = BAT.PORT_MST_POL_FK");
            strSQL.Append("   AND PODMST.PORT_MST_PK = BAT.PORT_MST_POD_FK");
            strSQL.Append("   AND HBL.HBL_EXP_TBL_PK(+) = JSE.HBL_HAWB_fk");
            strSQL.Append("   AND MBL.MBL_EXP_TBL_PK(+) = JSE.MBL_MAWB_FK");
            strSQL.Append("   AND CGMST.COMMODITY_GROUP_PK(+) = JSE.COMMODITY_GROUP_FK");

            strSQL.Append("   AND AMST.AGENT_MST_PK = CRSTBL.AGENT_MST_FK");
            strSQL.Append("   AND ADTLS.AGENT_MST_FK(+) = AMST.AGENT_MST_PK");
            strSQL.Append("   AND AGTCOUNTRY.COUNTRY_MST_PK(+) = ADTLS.ADM_COUNTRY_MST_FK");

            strSQL.Append("   AND SHIPDTLS.CUSTOMER_MST_FK(+) = SHIPMST.CUSTOMER_MST_PK");
            strSQL.Append("   AND SHIPDTLS.ADM_COUNTRY_MST_FK = SHIPCOUNTRY.COUNTRY_MST_PK(+)");
            strSQL.Append(" AND CRSTBL.CR_AGENT_PK=" + strPK);
            strSQL.Append("   GROUP BY CRSTBL.CR_AGENT_PK,");
            strSQL.Append("   CRSTBL.CREDIT_NOTE_REF_NO,");
            strSQL.Append("   CRSTBL.CREDIT_NOTE_AMT,");
            strSQL.Append("   INVAGTEXP.INV_AGENT_PK,");
            strSQL.Append("   INVAGTEXP.INVOICE_REF_NO,");
            strSQL.Append("   JSE.JOB_CARD_TRN_PK,");
            strSQL.Append("   JSE.JOBCARD_REF_NO,");
            strSQL.Append("   AMST.AGENT_NAME,");
            strSQL.Append("   AMST.ACCOUNT_NO ,");
            strSQL.Append("   ADTLS.ADM_ADDRESS_1,");
            strSQL.Append("   ADTLS.ADM_ADDRESS_2,");
            strSQL.Append("   ADTLS.ADM_ADDRESS_3,");
            strSQL.Append("   ADTLS.ADM_CITY,");
            strSQL.Append("   ADTLS.ADM_ZIP_CODE,");
            strSQL.Append("   ADTLS.ADM_PHONE_NO_1,");
            strSQL.Append("   ADTLS.ADM_FAX_NO,");
            strSQL.Append("   ADTLS.ADM_EMAIL_ID,");
            strSQL.Append("   AGTCOUNTRY.COUNTRY_NAME,");
            strSQL.Append("   SHIPMST.CUSTOMER_NAME,");
            strSQL.Append("   SHIPDTLS.ADM_ADDRESS_1,");
            strSQL.Append("   SHIPDTLS.ADM_ADDRESS_2,");
            strSQL.Append("   SHIPDTLS.ADM_ADDRESS_3,");
            strSQL.Append("    SHIPDTLS.ADM_CITY,");
            strSQL.Append("    SHIPDTLS.ADM_ZIP_CODE,");
            strSQL.Append("   SHIPDTLS.ADM_PHONE_NO_1,");
            strSQL.Append("   SHIPDTLS.ADM_FAX_NO,");
            strSQL.Append("   SHIPDTLS.ADM_EMAIL_ID,");
            strSQL.Append("   SHIPCOUNTRY.COUNTRY_NAME,");

            strSQL.Append("    TO_CHAR(JSE.ETD_DATE,'" + dateFormat + "') ,");
            strSQL.Append("   TO_CHAR(JSE.ETA_DATE,'" + dateFormat + "'),");
            strSQL.Append("    BAT.CARGO_TYPE,");
            strSQL.Append("   CURRMST.CURRENCY_ID,");
            strSQL.Append("   CURRMST.CURRENCY_NAME,");
            strSQL.Append("   (CASE WHEN JSE.VOYAGE IS NOT NULL THEN");
            strSQL.Append("   JSE.VESSEL_NAME || '-' || JSE.VOYAGE ELSE");
            strSQL.Append("   JSE.VOYAGE END),");
            strSQL.Append("    JSE.PYMT_TYPE,");
            strSQL.Append("   JSE.GOODS_DESCRIPTION,");
            strSQL.Append("   JSE.MARKS_NUMBERS,");
            strSQL.Append("   JSE.INSURANCE_AMT,");
            strSQL.Append("   STMST.INCO_CODE,");
            strSQL.Append("   AMST.VAT_NO,");
            strSQL.Append("   COLMST.PLACE_NAME,");
            strSQL.Append("   DELMST.PLACE_NAME,");
            strSQL.Append("    POLMST.PORT_NAME,");
            strSQL.Append("   PODMST.PORT_NAME,");
            strSQL.Append("   (CASE");
            strSQL.Append("    WHEN JSE.HBL_EXP_TBL_FK IS NOT NULL THEN");
            strSQL.Append("   HBL.HBL_REF_NO");
            strSQL.Append("   ELSE");
            strSQL.Append("    MBL.MBL_REF_NO");
            strSQL.Append("   END),");
            strSQL.Append("   CGMST.COMMODITY_GROUP_DESC,");
            strSQL.Append("    CRSTBL.REMARKS, ");
            strSQL.Append("    BAT.customer_ref_no  ");

            try
            {
                return objWK.GetDataSet(strSQL.ToString());
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

        #region " Save Functionality"

        public ArrayList save(string strpk, string creditNoteNo, string CB_AGENT_MST_FK, string CREDIT_NOTE_DATE, string CURRENCY_MST_FK, string CREDIT_NOTE_AMT, string INV_AGENT_SEA_EXP_FK, string REMARKS, long lngLocPk, long nEmpId,
        short AgentType, short BizType, short ProcessType)
        {
            WorkFlow objWK = new WorkFlow();
            cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
            Int16 exe = default(Int16);
            long lngCreditPk = 0;
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            string CreditNo = null;
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            objWK.MyCommand.Connection = objWK.MyConnection;
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                if (ProcessType == 1 & BizType == 1)
                {
                    if (string.IsNullOrEmpty(creditNoteNo.ToString()))
                    {
                        CreditNo = GenerateProtocolKey("AGENT CREDIT NOTE AIR EXPORT", lngLocPk, nEmpId, DateTime.Today, "", "", "", CREATED_BY, objWK);
                        if (CreditNo == "Protocol Not Defined.")
                        {
                            arrMessage.Add("Protocol Not Defined.");
                            return arrMessage;
                        }
                    }
                    else
                    {
                        CreditNo = creditNoteNo.ToString();
                    }
                }
                else if (ProcessType == 2 & BizType == 1)
                {
                    if (string.IsNullOrEmpty(creditNoteNo.ToString()))
                    {
                        CreditNo = GenerateProtocolKey("AGENT CREDIT NOTE AIR IMPORT", lngLocPk, nEmpId, DateTime.Today, "", "", "", CREATED_BY, objWK);
                        if (CreditNo == "Protocol Not Defined.")
                        {
                            arrMessage.Add("Protocol Not Defined.");
                            return arrMessage;
                        }
                    }
                    else
                    {
                        CreditNo = creditNoteNo.ToString();
                    }
                }
                else if (ProcessType == 1 & BizType == 2)
                {
                    if (string.IsNullOrEmpty(creditNoteNo.ToString()))
                    {
                        CreditNo = GenerateProtocolKey("AGENT CREDIT NOTE SEA EXPORT", lngLocPk, nEmpId, DateTime.Today, "", "", "", CREATED_BY, objWK);
                        if (CreditNo == "Protocol Not Defined.")
                        {
                            arrMessage.Add("Protocol Not Defined.");
                            return arrMessage;
                        }
                    }
                    else
                    {
                        CreditNo = creditNoteNo.ToString();
                    }
                }
                else if (ProcessType == 2 & BizType == 2)
                {
                    if (string.IsNullOrEmpty(creditNoteNo.ToString()))
                    {
                        CreditNo = GenerateProtocolKey("AGENT CREDIT NOTE SEA IMPORT", lngLocPk, nEmpId, DateTime.Today, "", "", "", CREATED_BY, objWK);
                        if (CreditNo == "Protocol Not Defined.")
                        {
                            arrMessage.Add("Protocol Not Defined.");
                            return arrMessage;
                        }
                    }
                    else
                    {
                        CreditNo = creditNoteNo;
                    }
                }
                objWK.MyCommand.Parameters.Clear();
                insCommand.CommandText = objWK.MyUserName + ".CR_AGENT_TBL_PKG.CR_AGENT_TBL_INS";
                insCommand.Connection = objWK.MyConnection;
                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.Transaction = TRAN;
                var _with2 = insCommand.Parameters;
                _with2.Add("CONFIG_PK_IN", ConfigurationPK);
                _with2.Add("CREATED_BY_FK_IN", CREATED_BY);
                _with2.Add("AGENT_MST_FK_IN", ifDBNull(CB_AGENT_MST_FK));
                _with2.Add("CREDIT_NOTE_REF_NO_IN", CreditNo);
                _with2.Add("CREDIT_NOTE_DATE_IN", ifDateNull(Convert.ToDateTime(CREDIT_NOTE_DATE)));
                _with2.Add("CURRENCY_MST_FK_IN", ifDBNull(CURRENCY_MST_FK));
                _with2.Add("CREDIT_NOTE_AMT_IN", ifDBNull(Convert.ToDouble(CREDIT_NOTE_AMT)));
                _with2.Add("INV_AGENT_FK_IN", ifDBNull(INV_AGENT_SEA_EXP_FK));
                _with2.Add("REMARKS_IN", ifDBNull(REMARKS));
                _with2.Add("CB_DP_LOAD_AGENT_IN", ifDBNull(AgentType));
                _with2.Add("BUSINESS_TYPE_IN", ifDBNull(BizType));
                _with2.Add("PROCESS_TYPE_IN", ifDBNull(ProcessType));
                _with2.Add("RETURN_VALUE", OracleDbType.Int32, 10, "CR_AGENT_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                exe = Convert.ToInt16(insCommand.ExecuteNonQuery());
                lngCreditPk = Convert.ToInt64(insCommand.Parameters["RETURN_VALUE"].Value);
                objTrackNTrace.SaveTrackAndTrace(Convert.ToInt32(lngCreditPk), 2, 1, "Credit Note to Agent", "CREDIT-AGT-SEA-EXP", Convert.ToInt32(lngLocPk), objWK, "INS", CREATED_BY, "O");
                if (exe > 0)
                {
                    TRAN.Commit();
                    //Push to financial system if realtime is selected
                    if (lngCreditPk > 0)
                    {
                        if (Convert.ToBoolean(ConfigurationManager.AppSettings["QFINGeneral"]) == true)
                        {
                            try
                            {
                                TRAN = objWK.MyConnection.BeginTransaction();
                                objWK.MyCommand.Transaction = TRAN;
                                objWK.MyCommand.Parameters.Clear();
                                objWK.MyCommand.CommandText = objWK.MyUserName + ".ACCOUNTING_INTEGREATION_PKG.CREDIT_NOTE_AGENT_APPROVE";
                                objWK.MyCommand.Parameters.Add("CR_NOTE_PK_IN", lngCreditPk).Direction = ParameterDirection.Input;
                                objWK.MyCommand.Parameters.Add("LOCAL_CUR_FK", Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
                                objWK.MyCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                objWK.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                                objWK.MyCommand.ExecuteNonQuery();
                                TRAN.Commit();
                            }
                            catch (Exception ex)
                            {
                            }
                        }

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
                            //    objPush.UpdateTaxElement(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                            //    objPush.UpdatebankAcc(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                            //    objPush.UpdateCurrency(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                            //    objPush.UpdateTransaction(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, lngCreditPk);
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
                    arrMessage.Add("All Data Saved Successfully");
                    strpk = Convert.ToString(insCommand.Parameters["RETURN_VALUE"].Value);
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new ArrayList();
        }

        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return col;
            }
        }

        private object ifDateNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return Convert.ToDateTime(col);
            }
        }

        #endregion " Save Functionality"
    }
}