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
using System;
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
   	public class cls_CSVInterfaceSAGEAir : CommonFeatures
    {

        #region "Private Variables"
        private long _PkValueMain;
        #endregion
        private string _BatchRefNumber;

        #region "Property"
        public long PkValueMain
        {
            get { return _PkValueMain; }
        }
        public string BatchRefNumber
        {
            get { return _BatchRefNumber; }
        }
        #endregion

        #region "Save Function"
        public ArrayList SaveBatch(DataSet dsMain, string lblDisplayBatchNo, long nLocationId, long nEmpId, short BusinessType)
        {
            string BatchRefNo = null;
            bool IsUpdate = false;
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            try
            {

                if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["BATCH_MST_PK"].ToString()))
                {
                    if (string.IsNullOrEmpty(lblDisplayBatchNo))
                    {
                        BatchRefNo = GenerateBatchNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK);
                        if (BatchRefNo == "Protocol Not Defined.")
                        {
                            arrMessage.Add("Protocol Not Defined.");
                            return arrMessage;
                        }
                        else if (BatchRefNo.Length > 25)
                        {
                            arrMessage.Add("Protocol should be less than 25 Characters");
                            return arrMessage;
                        }
                        BatchRefNo += ".CSV";
                    }
                    else
                    {
                        BatchRefNo = lblDisplayBatchNo;
                    }

                    objWK.MyCommand.Parameters.Clear();
                    var _with1 = objWK.MyCommand;
                    _with1.CommandType = CommandType.StoredProcedure;
                    _with1.CommandText = objWK.MyUserName + ".BATCH_MST_TBL_PKG.BATCH_MST_TBL_INS";
                    _with1.Parameters.Add("BATCH_REF_NO_IN", BatchRefNo).Direction = ParameterDirection.Input;
                    _with1.Parameters["BATCH_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with1.Parameters.Add("FROM_DATE_IN", Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["FROM_DATE"])).Direction = ParameterDirection.Input;
                    _with1.Parameters["FROM_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with1.Parameters.Add("TO_DATE_IN", Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["TO_DATE"])).Direction = ParameterDirection.Input;
                    _with1.Parameters["TO_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with1.Parameters.Add("CATEGORY_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CATEGORY"])).Direction = ParameterDirection.Input;
                    _with1.Parameters["CATEGORY_IN"].SourceVersion = DataRowVersion.Current;

                    _with1.Parameters.Add("DOCUMENT_TYPE_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["DOCUMENT_TYPE"])).Direction = ParameterDirection.Input;
                    _with1.Parameters["DOCUMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with1.Parameters.Add("BUSINESS_TYPE_IN", 1).Direction = ParameterDirection.Input;

                    _with1.Parameters["BUSINESS_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with1.Parameters.Add("MODE_EXP_IMP_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["MODE_EXP_IMP"])).Direction = ParameterDirection.Input;

                    _with1.Parameters["MODE_EXP_IMP_IN"].SourceVersion = DataRowVersion.Current;

                    _with1.Parameters.Add("LOCATION_MST_FK_IN", nLocationId).Direction = ParameterDirection.Input;

                    _with1.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                    _with1.Parameters.Add("CONFIG_MST_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                    _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with1.ExecuteNonQuery();
                }
                if (string.Compare(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value), "batch")>0)
                {
                    arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    _PkValueMain = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                    _BatchRefNumber = BatchRefNo;
                    arrMessage.Add("datasaved");
                }
                if (string.Compare(arrMessage[0].ToString(), "datasaved")>0)
                {
                    arrMessage.Add("All data saved successfully");
                    TRAN.Commit();
                    lblDisplayBatchNo = BatchRefNo;
                    return arrMessage;
                }
                else
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }
        public string GenerateBatchNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
        {
            string functionReturnValue = null;
            functionReturnValue = GenerateProtocolKey("CSV INWARD (AIR)", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy, objWK);
            return functionReturnValue;
        }
        #endregion

        #region "Generate CSV Function"
        public DataSet GenCSVAir(string strFDate = "", string strTDate = "", string strCategory = "", string strDocType = "", string strReGenStatus = "", bool Import = false, bool Export = false, bool All = false)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQLI = null;
            string strSQLE = null;
            string strSQLA = null;
            string strToCurr = null;
            Int32 intLoc = default(Int32);
            System.Web.UI.Page objPage = new System.Web.UI.Page();
            intLoc = (Int32)objPage.Session["LOGED_IN_LOC_FK"];
            long lngToCurr = 0;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            strToCurr += " SELECT CORP.CURRENCY_MST_FK "  + "FROM CORPORATE_MST_TBL CORP, LOCATION_MST_TBL LOC "  + "WHERE LOC.CORPORATE_MST_FK = CORP.CORPORATE_MST_PK "  + "AND LOC.LOCATION_MST_PK = " + intLoc ;
            lngToCurr = Convert.ToInt64((new WorkFlow()).ExecuteScaler(strToCurr));

            if (Import | All)
            {
                if (strCategory == "1" & strDocType == "1")
                {
                    sb.Remove(0, sb.Length);
                    sb.Append("SELECT 'SI' AS DOCUMENT_CODE,");
                    sb.Append("       CMT.CUSTOMER_ID CUSTOMER_CODE,");
                    sb.Append("       '4000' AS IE_INDICATOR,");
                    sb.Append("       '1' AS COST_CENTRE_CODE,");
                    sb.Append("       TO_CHAR(ICAIT.INVOICE_DATE, 'ddmmyyyy') AS INVOICE_DATE,");
                    sb.Append("       ICAIT.INVOICE_REF_NO AS REFERENCE_NUMBER,");
                    sb.Append("       JCAIT.JOBCARD_REF_NO AS JOB_NO,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN GET_EX_RATE(ICAIT.CURRENCY_MST_FK, " + lngToCurr + ", ICAIT.INVOICE_DATE) IS NULL THEN");
                    sb.Append("          ROUND(ICAIT.NET_RECEIVABLE, 2)");
                    sb.Append("         ELSE");
                    sb.Append("          ROUND((GET_EX_RATE(ICAIT.CURRENCY_MST_FK, " + lngToCurr + ", ICAIT.INVOICE_DATE) *");
                    sb.Append("                ICAIT.NET_RECEIVABLE),");
                    sb.Append("                2)");
                    sb.Append("       END AS INVOICE_VALUE,");
                    sb.Append("       DECODE(TRN.TAX_AMT, NULL, 2, 1) AS VAT_INDICATOR,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN GET_EX_RATE(ICAIT.CURRENCY_MST_FK, " + lngToCurr + ", ICAIT.INVOICE_DATE) IS NULL THEN");
                    sb.Append("          ROUND(NVL(TRN.TAX_AMT, 0), 2)");
                    sb.Append("         ELSE");
                    sb.Append("          ROUND((GET_EX_RATE(ICAIT.CURRENCY_MST_FK, " + lngToCurr + ", ICAIT.INVOICE_DATE) *");
                    sb.Append("                NVL(TRN.TAX_AMT, 0)),");
                    sb.Append("                2)");
                    sb.Append("       END AS VAT_AMOUNT");
                    sb.Append("  FROM CONSOL_INVOICE_TBL     ICAIT,");
                    sb.Append("       CONSOL_INVOICE_TRN_TBL TRN,");
                    sb.Append("       JOB_CARD_TRN   JCAIT,");
                    sb.Append("       CUSTOMER_MST_TBL       CMT");
                    sb.Append(" WHERE TRN.JOB_CARD_FK = JCAIT.JOB_CARD_TRN_PK");
                    sb.Append("   AND TRN.CONSOL_INVOICE_FK = ICAIT.CONSOL_INVOICE_PK");
                    sb.Append("   AND JCAIT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                    sb.Append("   AND ICAIT.PROCESS_TYPE = 2");
                    sb.Append("   AND ICAIT.BUSINESS_TYPE = 1");
                    sb.Append("   AND ICAIT.INVOICE_DATE >= TO_DATE('" + strFDate + "','" + dateFormat + "')");
                    sb.Append("   AND ICAIT.INVOICE_DATE <= TO_DATE('" + strTDate + "','" + dateFormat + "')");
                    strSQLI = sb.ToString();

                    if (!!string.IsNullOrEmpty(strReGenStatus))
                    {
                        strSQLI += " AND ICAIT.BATCH_MST_FK IS NULL";
                    }
                }
                else if (strCategory == "1" & strDocType == "2")
                {
                    sb.Remove(0, sb.Length);
                    sb.Append("SELECT 'CR' AS DOCCODE,");
                    sb.Append("       CMT.CUSTOMER_ID CUSTCODE,");
                    sb.Append("       '4000' AS IEINDICATOR,");
                    sb.Append("       '1' AS COSTCENTRECODE,");
                    sb.Append("       TO_CHAR(CCAIT.CREDIT_NOTE_DATE, 'ddmmyyyy') AS DDATE,");
                    sb.Append("       CCAIT.CREDIT_NOTE_REF_NR AS REFNUMBER,");
                    sb.Append("       JCAIT.JOBCARD_REF_NO AS JOBNO,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN GET_EX_RATE(CCAIT.CURRENCY_MST_FK, " + lngToCurr + ", CCAIT.CREDIT_NOTE_DATE) IS NULL THEN");
                    sb.Append("          ROUND(CCAIT.CRN_AMMOUNT, 2)");
                    sb.Append("         ELSE");
                    sb.Append("          ROUND((GET_EX_RATE(CCAIT.CURRENCY_MST_FK,");
                    sb.Append("                             " + lngToCurr + ",");
                    sb.Append("                             CCAIT.CREDIT_NOTE_DATE) * CCAIT.CRN_AMMOUNT),");
                    sb.Append("                2)");
                    sb.Append("       END AS CREDITVALUE,");
                    sb.Append("       '2' AS VATINDICATOR,");
                    sb.Append("       '0' AS VATAMOUNT");
                    sb.Append("  FROM CREDIT_NOTE_TBL        CCAIT,");
                    sb.Append("       CONSOL_INVOICE_TBL     ICAIT,");
                    sb.Append("       CREDIT_NOTE_TRN_TBL    CRTRN,");
                    sb.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
                    sb.Append("       JOB_CARD_TRN   JCAIT,");
                    sb.Append("       CUSTOMER_MST_TBL       CMT");
                    sb.Append(" WHERE INVTRN.JOB_CARD_FK = JCAIT.JOB_CARD_TRN_PK");
                    sb.Append("   AND CCAIT.CRN_TBL_PK = CRTRN.CRN_TBL_FK");
                    sb.Append("   AND INVTRN.CONSOL_INVOICE_FK = ICAIT.CONSOL_INVOICE_PK");

                    sb.Append("   AND CRTRN.CONSOL_INVOICE_TRN_FK = INVTRN.CONSOL_INVOICE_TRN_PK");

                    sb.Append("   AND ICAIT.BUSINESS_TYPE = 1");
                    sb.Append("   AND CCAIT.BIZ_TYPE = 1");
                    sb.Append("   AND ICAIT.PROCESS_TYPE = 2");
                    sb.Append("   AND CCAIT.PROCESS_TYPE = 2");
                    sb.Append("   AND CCAIT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                    sb.Append("   AND CCAIT.CREDIT_NOTE_DATE >= TO_DATE('" + strFDate + "','" + dateFormat + "')");
                    sb.Append("   AND CCAIT.CREDIT_NOTE_DATE <= TO_DATE('" + strTDate + "','" + dateFormat + "')");
                    strSQLI = sb.ToString();

                    if (!!string.IsNullOrEmpty(strReGenStatus))
                    {
                        strSQLI += " AND CCAIT.BATCH_MST_FK IS NULL";
                    }
                }
                else if (strCategory == "2" & strDocType == "1")
                {
                    strSQLI += "SELECT "  + "'SI' AS DOCCODE, "  + "AMT.AGENT_ID CUSTCODE,"  + "'4000' AS IEINDICATOR,  "  + "'1' AS COSTCENTRECODE, "  + "TO_CHAR(IAAIT.INVOICE_DATE, 'ddmmyyyy') AS DDATE, "  + "IAAIT.INVOICE_REF_NO AS REFNUMBER, "  + "JCAIT.JOBCARD_REF_NO AS JOBNO, "  + "CASE WHEN GET_EX_RATE(IAAIT.CURRENCY_MST_FK, " + lngToCurr + " , IAAIT.INVOICE_DATE) IS NULL THEN ROUND(IAAIT.NET_INV_AMT, 2) ELSE ROUND((GET_EX_RATE(IAAIT.CURRENCY_MST_FK, " + lngToCurr + " , IAAIT.INVOICE_DATE ) * IAAIT.NET_INV_AMT), 2) END AS INVVALUE, "  + "DECODE(IAAIT.VAT_AMT,null,2,1) AS VATINDICATOR,"  + "CASE WHEN GET_EX_RATE(IAAIT.CURRENCY_MST_FK, " + lngToCurr + " , IAAIT.INVOICE_DATE) IS NULL THEN ROUND(NVL(IAAIT.VAT_AMT, 0), 2) ELSE ROUND((GET_EX_RATE(IAAIT.CURRENCY_MST_FK, " + lngToCurr + " , IAAIT.INVOICE_DATE ) * NVL(IAAIT.VAT_AMT, 0)), 2) END AS VATAMOUNT "  + "FROM"  + "INV_AGENT_TBL IAAIT, "  + "JOB_CARD_TRN JCAIT, "  + "AGENT_MST_TBL AMT"  + "WHERE"  + "IAAIT.JOB_CARD_FK = JCAIT.JOB_CARD_TRN_PK"  + "AND JCAIT.CB_AGENT_MST_FK=AMT.AGENT_MST_PK"  + "AND IAAIT.INVOICE_DATE >= TO_DATE('" + strFDate + "','" + dateFormat + "')"  + "AND IAAIT.INVOICE_DATE <= TO_DATE('" + strTDate + "','" + dateFormat + "')";
                    if (!!string.IsNullOrEmpty(strReGenStatus))
                    {
                        strSQLI += " AND IAAIT.BATCH_MST_FK IS NULL";
                    }
                }
                else if (strCategory == "2" & strDocType == "2")
                {
                    strSQLI += "SELECT "  + "'CR' AS DOCCODE, "  + "AMT.AGENT_ID CUSTCODE,"  + "'4000' AS IEINDICATOR, "  + "'1' AS COSTCENTRECODE, "  + "TO_CHAR(CAAIT.CREDIT_NOTE_DATE, 'ddmmyyyy') AS DDATE, "  + "CAAIT.CREDIT_NOTE_REF_NO AS REFNUMBER,"  + "JCAIT.JOBCARD_REF_NO AS JOBNO, "  + "CASE WHEN GET_EX_RATE(CAAIT.CURRENCY_MST_FK, " + lngToCurr + " , CAAIT.CREDIT_NOTE_DATE) IS NULL THEN ROUND(CAAIT.CREDIT_NOTE_AMT, 2) ELSE ROUND((GET_EX_RATE(CAAIT.CURRENCY_MST_FK, " + lngToCurr + " , CAAIT.CREDIT_NOTE_DATE ) * CAAIT.CREDIT_NOTE_AMT), 2) END AS CREDITVALUE, "  + "'2' AS VATINDICATOR,"  + "'0' AS VATAMOUNT "  + "FROM "  + "CR_AGENT_TBL CAAIT,"  + "INV_AGENT_TBL IAAIT,"  + "JOB_CARD_TRN JCAIT, "  + "AGENT_MST_TBL AMT "  + "WHERE "  + "IAAIT.JOB_CARD_FK=JCAIT.JOB_CARD_TRN_PK "  + "AND CAAIT.INV_AGENT_FK=IAAIT.INV_AGENT_PK "  + "AND CAAIT.AGENT_MST_FK=AMT.AGENT_MST_PK "  + "AND CAAIT.CREDIT_NOTE_DATE >= TO_DATE('" + strFDate + "','" + dateFormat + "')"  + "AND CAAIT.CREDIT_NOTE_DATE <= TO_DATE('" + strTDate + "','" + dateFormat + "')";
                    if (!!string.IsNullOrEmpty(strReGenStatus))
                    {
                        strSQLI += " AND CAAIT.BATCH_MST_FK IS NULL ";
                    }
                }
                else if (strCategory == "3" & strDocType == "1")
                {
                    strSQLI += "SELECT "  + "'PI' AS DOCCODE, "  + "JTAIP.VENDOR_KEY CUSTCODE, "  + "'4000' AS IEINDICATOR, "  + "'1' AS COSTCENTRECODE, "  + "TO_CHAR(JTAIP.INVOICE_DATE, 'ddmmyyyy') AS DDATE, "  + "JTAIP.INVOICE_NUMBER AS REFNUMBER, "  + "JCAIT.JOBCARD_REF_NO AS JOBNO, "  + "CASE WHEN GET_EX_RATE(JTAIP.CURRENCY_MST_FK, " + lngToCurr + " , JTAIP.INVOICE_DATE) IS NULL THEN ROUND(JTAIP.INVOICE_AMT, 2) ELSE ROUND((GET_EX_RATE(JTAIP.CURRENCY_MST_FK, " + lngToCurr + " , JTAIP.INVOICE_DATE ) * JTAIP.INVOICE_AMT), 2) END AS INVVALUE "  + "FROM "  + "JOB_CARD_TRN JCAIT, "  + "JOB_TRN_PIA JTAIP"  + "WHERE "  + "JTAIP.JOB_CARD_TRN_FK = JCAIT.JOB_CARD_TRN_PK "  + "AND JTAIP.INVOICE_DATE >= TO_DATE('" + strFDate + "','" + dateFormat + "')"  + "AND JTAIP.INVOICE_DATE <= TO_DATE('" + strTDate + "','" + dateFormat + "')";
                    if (!!string.IsNullOrEmpty(strReGenStatus))
                    {
                        strSQLI += " AND JTAIP.BATCH_MST_FK IS NULL";
                    }
                }
            }
            if (Export | All)
            {
                if (strCategory == "1" & strDocType == "1")
                {
                    sb.Remove(0, sb.Length);
                    sb.Append("SELECT 'SI' AS DOCCODE,");
                    sb.Append("       CMT.CUSTOMER_ID CUSTCODE,");
                    sb.Append("       '4001' AS IEINDICATOR,");
                    sb.Append("       '1' AS COSTCENTRECODE,");
                    sb.Append("       TO_CHAR(ICAET.INVOICE_DATE, 'ddmmyyyy') AS DDATE,");
                    sb.Append("       ICAET.INVOICE_REF_NO AS REFNUMBER,");
                    sb.Append("       JCAET.JOBCARD_REF_NO AS JOBNO,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN GET_EX_RATE(ICAET.CURRENCY_MST_FK, " + lngToCurr + ", ICAET.INVOICE_DATE) IS NULL THEN");
                    sb.Append("          ROUND(ICAET.NET_RECEIVABLE, 2)");
                    sb.Append("         ELSE");
                    sb.Append("          ROUND((GET_EX_RATE(ICAET.CURRENCY_MST_FK, " + lngToCurr + ", ICAET.INVOICE_DATE) *");
                    sb.Append("                ICAET.NET_RECEIVABLE),");
                    sb.Append("                2)");
                    sb.Append("       END AS INVVALUE,");
                    sb.Append("       DECODE(TRN.TAX_AMT, NULL, 2, 1) AS VATINDICATOR,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN GET_EX_RATE(ICAET.CURRENCY_MST_FK, " + lngToCurr + ", ICAET.INVOICE_DATE) IS NULL THEN");
                    sb.Append("          ROUND(NVL(TRN.TAX_AMT, 0), 2)");
                    sb.Append("         ELSE");
                    sb.Append("          ROUND((GET_EX_RATE(ICAET.CURRENCY_MST_FK, " + lngToCurr + ", ICAET.INVOICE_DATE) *");
                    sb.Append("                NVL(TRN.TAX_AMT, 0)),");
                    sb.Append("                2)");
                    sb.Append("       END AS VATAMOUNT");
                    sb.Append("  FROM CONSOL_INVOICE_TBL     ICAET,");
                    sb.Append("       CONSOL_INVOICE_TRN_TBL TRN,");
                    sb.Append("       JOB_CARD_TRN   JCAET,");
                    sb.Append("       CUSTOMER_MST_TBL       CMT");
                    sb.Append(" WHERE TRN.JOB_CARD_FK = JCAET.JOB_CARD_TRN_PK");
                    sb.Append("   AND TRN.CONSOL_INVOICE_FK = ICAET.CONSOL_INVOICE_PK");
                    sb.Append("   AND ICAET.PROCESS_TYPE = 1");
                    sb.Append("   AND ICAET.BUSINESS_TYPE = 1");
                    sb.Append("   AND JCAET.SHIPPER_CUST_MST_FK = CMT.CUSTOMER_MST_PK");
                    sb.Append("   AND ICAET.INVOICE_DATE >= TO_DATE('" + strFDate + "','" + dateFormat + "')");
                    sb.Append("   AND ICAET.INVOICE_DATE <= TO_DATE('" + strTDate + "','" + dateFormat + "')");
                    strSQLE = sb.ToString();

                    if (!!string.IsNullOrEmpty(strReGenStatus))
                    {
                        strSQLE += " AND ICAET.BATCH_MST_FK IS NULL";
                    }
                }
                else if (strCategory == "1" & strDocType == "2")
                {
                    sb.Remove(0, sb.Length);
                    sb.Append("SELECT 'CR' AS DOCCODE,");
                    sb.Append("       CMT.CUSTOMER_ID CUSTCODE,");
                    sb.Append("       '4001' AS IEINDICATOR,");
                    sb.Append("       '1' AS COSTCENTRECODE,");
                    sb.Append("       TO_CHAR(CCAET.CREDIT_NOTE_DATE, 'ddmmyyyy') AS DDATE,");
                    sb.Append("       CCAET.CREDIT_NOTE_REF_NR AS REFNUMBER,");
                    sb.Append("       JCAET.JOBCARD_REF_NO AS JOBNO,");
                    sb.Append("       CASE");
                    sb.Append("         WHEN GET_EX_RATE(CCAET.CURRENCY_MST_FK, " + lngToCurr + ", CCAET.CREDIT_NOTE_DATE) IS NULL THEN");
                    sb.Append("          ROUND(CCAET.CRN_AMMOUNT, 2)");
                    sb.Append("         ELSE");
                    sb.Append("          ROUND((GET_EX_RATE(CCAET.CURRENCY_MST_FK,");
                    sb.Append("                             " + lngToCurr + ",");
                    sb.Append("                             CCAET.CREDIT_NOTE_DATE) * CCAET.CRN_AMMOUNT),");
                    sb.Append("                2)");
                    sb.Append("       END AS CREDITVALUE,");
                    sb.Append("       '2' AS VATINDICATOR,");
                    sb.Append("       '0' AS VATAMOUNT");
                    sb.Append("  FROM CREDIT_NOTE_TBL        CCAET,");
                    sb.Append("       CONSOL_INVOICE_TBL     ICAET,");
                    sb.Append("       CREDIT_NOTE_TRN_TBL    CRTRN,");
                    sb.Append("       CONSOL_INVOICE_TRN_TBL INVTRN,");
                    sb.Append("       JOB_CARD_TRN   JCAET,");
                    sb.Append("       CUSTOMER_MST_TBL       CMT");
                    sb.Append(" WHERE INVTRN.JOB_CARD_FK = JCAET.JOB_CARD_TRN_PK");
                    sb.Append("   AND CCAET.CRN_TBL_PK = CRTRN.CRN_TBL_FK");
                    sb.Append("   AND INVTRN.CONSOL_INVOICE_FK = ICAET.CONSOL_INVOICE_PK");
                    sb.Append("   AND CRTRN.CONSOL_INVOICE_TRN_FK = ICAET.CONSOL_INVOICE_PK");
                    sb.Append("   AND ICAET.BUSINESS_TYPE = 1");
                    sb.Append("   AND CCAET.BIZ_TYPE = 1");
                    sb.Append("   AND ICAET.PROCESS_TYPE = 1");
                    sb.Append("   AND CCAET.PROCESS_TYPE = 1");
                    sb.Append("   AND CCAET.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                    sb.Append("   AND CCAET.CREDIT_NOTE_DATE >= TO_DATE('" + strFDate + "','" + dateFormat + "')");
                    sb.Append("   AND CCAET.CREDIT_NOTE_DATE <= TO_DATE('" + strTDate + "','" + dateFormat + "')");
                    strSQLE = sb.ToString();

                    if (!!string.IsNullOrEmpty(strReGenStatus))
                    {
                        strSQLE += " AND CCAET.BATCH_MST_FK IS NULL";
                    }
                }
                else if (strCategory == "2" & strDocType == "1")
                {
                    strSQLE += "SELECT "  + "'SI' AS DOCCODE, "  + "AMT.AGENT_ID CUSTCODE,"  + "'4001' AS IEINDICATOR,  "  + "'1' AS COSTCENTRECODE, "  + "TO_CHAR(IAAET.INVOICE_DATE, 'ddmmyyyy') AS DDATE, "  + "IAAET.INVOICE_REF_NO AS REFNUMBER, "  + "JCAET.JOBCARD_REF_NO AS JOBNO, "  + "CASE WHEN GET_EX_RATE(IAAET.CURRENCY_MST_FK, " + lngToCurr + " , IAAET.INVOICE_DATE) IS NULL THEN ROUND(IAAET.NET_INV_AMT, 2) ELSE ROUND((GET_EX_RATE(IAAET.CURRENCY_MST_FK, " + lngToCurr + " , IAAET.INVOICE_DATE ) * IAAET.NET_INV_AMT), 2) END AS INVVALUE, "  + "DECODE(IAAET.VAT_AMT,null,2,1) AS VATINDICATOR,"  + "CASE WHEN GET_EX_RATE(IAAET.CURRENCY_MST_FK, " + lngToCurr + " , IAAET.INVOICE_DATE) IS NULL THEN ROUND(NVL(IAAET.VAT_AMT, 0), 2) ELSE ROUND((GET_EX_RATE(IAAET.CURRENCY_MST_FK, " + lngToCurr + " , IAAET.INVOICE_DATE ) * NVL(IAAET.VAT_AMT, 0)), 2) END AS VATAMOUNT "  + "FROM"  + "INV_AGENT_TBL IAAET, "  + "JOB_CARD_TRN JCAET, "  + "AGENT_MST_TBL AMT"  + "WHERE"  + "IAAET.JOB_CARD_FK = JCAET.JOB_CARD_TRN_PK"  + "AND JCAET.CB_AGENT_MST_FK=AMT.AGENT_MST_PK"  + "AND IAAET.INVOICE_DATE >= TO_DATE('" + strFDate + "','" + dateFormat + "')"  + "AND IAAET.INVOICE_DATE <= TO_DATE('" + strTDate + "','" + dateFormat + "')";
                    if (!!string.IsNullOrEmpty(strReGenStatus))
                    {
                        strSQLE += " AND IAAET.BATCH_MST_FK IS NULL";
                    }
                }
                else if (strCategory == "2" & strDocType == "2")
                {
                    strSQLE += "SELECT "  + "'CR' AS DOCCODE, "  + "AMT.AGENT_ID CUSTCODE,"  + "'4001' AS IEINDICATOR, "  + "'1' AS COSTCENTRECODE, "  + "TO_CHAR(CAAET.CREDIT_NOTE_DATE, 'ddmmyyyy') AS DDATE, "  + "CAAET.CREDIT_NOTE_REF_NO AS REFNUMBER,"  + "JCAET.JOBCARD_REF_NO AS JOBNO, "  + "CASE WHEN GET_EX_RATE(CAAET.CURRENCY_MST_FK, " + lngToCurr + " , CAAET.CREDIT_NOTE_DATE) IS NULL THEN ROUND(CAAET.CREDIT_NOTE_AMT, 2) ELSE ROUND((GET_EX_RATE(CAAET.CURRENCY_MST_FK, " + lngToCurr + " , CAAET.CREDIT_NOTE_DATE ) * CAAET.CREDIT_NOTE_AMT), 2) END AS CREDITVALUE, "  + "'2' AS VATINDICATOR,"  + "'0' AS VATAMOUNT "  + "FROM "  + "CR_AGENT_TBL CAAET,"  + "INV_AGENT_TBL IAAET,"  + "JOB_CARD_TRN JCAET, "  + "AGENT_MST_TBL AMT "  + "WHERE "  + "IAAET.JOB_CARD_FK=JCAET.JOB_CARD_TRN_PK "  + "AND CAAET.INV_AGENT_FK=IAAET.INV_AGENT_PK "  + "AND CAAET.AGENT_MST_FK=AMT.AGENT_MST_PK "  + "AND CAAET.CREDIT_NOTE_DATE >= TO_DATE('" + strFDate + "','" + dateFormat + "')"  + "AND CAAET.CREDIT_NOTE_DATE <= TO_DATE('" + strTDate + "','" + dateFormat + "')";
                    if (!!string.IsNullOrEmpty(strReGenStatus))
                    {
                        strSQLE += " AND CAAET.BATCH_MST_FK IS NULL ";
                    }
                }
                else if (strCategory == "3" & strDocType == "1")
                {
                    strSQLE += "SELECT "  + "'PI' AS DOCCODE, "  + "JTAEP.VENDOR_KEY CUSTCODE, "  + "'4001' AS IEINDICATOR, "  + "'1' AS COSTCENTRECODE, "  + "TO_CHAR(JTAEP.INVOICE_DATE, 'ddmmyyyy') AS DDATE, "  + "JTAEP.INVOICE_NUMBER AS REFNUMBER, "  + "JCAET.JOBCARD_REF_NO AS JOBNO, "  + "CASE WHEN GET_EX_RATE(JTAEP.CURRENCY_MST_FK, " + lngToCurr + " , JTAEP.INVOICE_DATE) IS NULL THEN ROUND(JTAEP.INVOICE_AMT, 2) ELSE ROUND((GET_EX_RATE(JTAEP.CURRENCY_MST_FK, " + lngToCurr + " , JTAEP.INVOICE_DATE ) * JTAEP.INVOICE_AMT), 2) END AS INVVALUE "  + "FROM "  + "JOB_CARD_TRN JCAET, "  + "JOB_TRN_PIA JTAEP"  + "WHERE "  + "JTAEP.JOB_CARD_TRN_FK = JCAET.JOB_CARD_TRN_PK "  + "AND JTAEP.INVOICE_DATE >= TO_DATE('" + strFDate + "','" + dateFormat + "')"  + "AND JTAEP.INVOICE_DATE <= TO_DATE('" + strTDate + "','" + dateFormat + "')";
                    if (!!string.IsNullOrEmpty(strReGenStatus))
                    {
                        strSQLE += " AND JTAEP.BATCH_MST_FK IS NULL";
                    }
                }
            }
            if (All)
            {
                strSQLA = strSQLI + " UNION " + strSQLE;
            }
            try
            {
                if (Import)
                {
                    return objWF.GetDataSet(strSQLI);
                }
                else if (Export)
                {
                    return objWF.GetDataSet(strSQLE);
                }
                else if (All)
                {
                    return objWF.GetDataSet(strSQLA);
                }
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
            return new DataSet();
        }
        #endregion

        #region "Regenerate CSV"
        public DataSet FunReCreateCSV(string strBatchNoPK)
        {
            string strSql = "";
            WorkFlow objWF = new WorkFlow();
            strSql = "SELECT "  + "BMT.FROM_DATE, "  + "BMT.TO_DATE, "  + "BMT.CATEGORY, "  + "BMT.DOCUMENT_TYPE "  + "FROM BATCH_MST_TBL BMT WHERE BMT.BATCH_MST_PK=" + strBatchNoPK;
            try
            {
                return objWF.GetDataSet(strSql);
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

        public DataSet FunReCreateLCSV(bool Import = false, bool Export = false, bool All = false)
        {
            string strSql = "";
            WorkFlow objWF = new WorkFlow();
            strSql = "SELECT "  + "BMT.FROM_DATE, "  + "BMT.TO_DATE, "  + "BMT.CATEGORY, "  + "BMT.DOCUMENT_TYPE, "  + "BMT.BATCH_REF_NO "  + "FROM BATCH_MST_TBL BMT WHERE "  + "BMT.BATCH_MST_PK = (SELECT MAX(BMTT.BATCH_MST_PK) FROM BATCH_MST_TBL BMTT ";
            if (Import)
            {
                strSql += " WHERE BMTT.MODE_EXP_IMP=2 AND BMTT.BUSINESS_TYPE=1 ";
            }
            else if (Export)
            {
                strSql += " WHERE BMTT.MODE_EXP_IMP=1 AND BMTT.BUSINESS_TYPE=1 ";
            }
            else if (All)
            {
                strSql += " WHERE BMTT.MODE_EXP_IMP=3 AND BMTT.BUSINESS_TYPE=1 ";
            }
            strSql += ")";
            try
            {
                return objWF.GetDataSet(strSql);
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

        #region "Enhance Search Functions"
        public string FetchForBATCHNoAir(string strCond)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = "";
            string strLOC_MST_IN = "";
            string strBusinessType = "";
            Int32 intImpExp = 0;
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBusinessType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                intImpExp = Convert.ToInt32(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_CSV_BATCH_PKG.GET_BATCH_NO";
                var _with2 = SCM.Parameters;
                _with2.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with2.Add("MODE_EXP_IMP_IN", intImpExp).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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

        #region " Supporting Function "
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
        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, DBNull.Value))
            {
                return "";
            }
            return col;
        }
        #endregion

    }
}