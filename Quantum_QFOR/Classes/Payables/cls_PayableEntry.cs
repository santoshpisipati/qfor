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

using Microsoft.VisualBasic;
using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsPayableEntry : CommonFeatures
    {
        /// <summary>
        /// The _ payment_ TBL_ pk
        /// </summary>
        private long _Payment_Tbl_Pk;

        #region "Property"

        /// <summary>
        /// Gets the payment MST fk.
        /// </summary>
        /// <value>
        /// The payment MST fk.
        /// </value>
        public long PaymentMstFk
        {
            get { return _Payment_Tbl_Pk; }
        }

        #endregion "Property"

        #region " Enhance Search Function for Job Card "

        /// <summary>
        /// Fetch_s the job_ for_ payments.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string Fetch_Job_For_Payments(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand cmd = new OracleCommand();
            string strReturn = null;
            string[] arr = null;
            string strSearchIn = "";
            short intBizType = 0;
            short intProcess = 0;
            int intParty = 0;
            long intLocPk = 0;
            string strReq = null;
            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSearchIn = arr[1];
            if (arr.Length > 2)
                intBizType = Convert.ToInt16(arr[2]);
            if (arr.Length > 3)
                intProcess = Convert.ToInt16(arr[3]);
            if (arr.Length > 4)
            {
                if (!string.IsNullOrEmpty(Convert.ToString(arr[4])))
                {
                    intParty = Convert.ToInt32(arr[4]);
                }
            }

            if (arr.Length > 5)
                intLocPk = Convert.ToInt64(arr[5]);

            try
            {
                objWF.OpenConnection();
                cmd.Connection = objWF.MyConnection;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = objWF.MyUserName + ".EN_JOB_FOR_PAYMENTS.GET_JOB_PAY";
                var _with1 = cmd.Parameters;
                _with1.Add("SEARCH_IN", getDefault(strSearchIn, "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", intBizType).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_IN", intProcess).Direction = ParameterDirection.Input;
                _with1.Add("PARTY_IN", intParty).Direction = ParameterDirection.Input;
                _with1.Add("LOC_IN", intLocPk).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                cmd.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                cmd.ExecuteNonQuery();
                strReturn = Convert.ToString(cmd.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                cmd.Connection.Close();
            }
        }

        #endregion " Enhance Search Function for Job Card "

        #region "Fetch Data"

        /// <summary>
        /// Fetches the unsettled invoices.
        /// </summary>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="intProcessType">Type of the int process.</param>
        /// <param name="lngSupplierPk">The LNG supplier pk.</param>
        /// <param name="strFromDate">The string from date.</param>
        /// <param name="strToDate">The string to date.</param>
        /// <param name="lngLocalCurrency">The LNG local currency.</param>
        /// <param name="Ex_Rate_Type">Type of the ex_ rate_.</param>
        /// <returns></returns>
        public DataTable FetchUnsettledInvoices(int intBizType, int intProcessType, long lngSupplierPk, string strFromDate, string strToDate, long lngLocalCurrency, int Ex_Rate_Type)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                var _with2 = objWF.MyCommand;
                _with2.Parameters.Add("BIZ_TYPE_IN", intBizType).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("PROCESS_TYPE_IN", intProcessType).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("VENDOR_MST_FK_IN", lngSupplierPk).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("FROM_DATE_IN", getDefault(strFromDate, "")).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("TO_DATE_IN", getDefault(strToDate, "")).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("PAYMENT_CURR_FK_IN", lngLocalCurrency).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("EX_RATE_TYPE_IN", Ex_Rate_Type).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("ACC_PAYBLE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataTable("PAYMENTS_TBL_PKG", "FETCH_ACCOUNTS_PAYABLE");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        /// <summary>
        /// Fetches the unsettled invoices.
        /// </summary>
        /// <param name="strJob">The string job.</param>
        /// <param name="lngLocalCurrency">The LNG local currency.</param>
        /// <param name="intBizType">Type of the int biz.</param>
        /// <param name="intProcessType">Type of the int process.</param>
        /// <param name="isJob">if set to <c>true</c> [is job].</param>
        /// <returns></returns>
        public DataTable FetchUnsettledInvoices(string strJob, long lngLocalCurrency, int intBizType, int intProcessType, bool isJob)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (isJob)
                {
                    var _with3 = objWF.MyCommand;
                    _with3.Parameters.Add("BIZ_TYPE_IN", intBizType).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("PROCESS_TYPE_IN", intProcessType).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("JOB_REF_NO_IN", getDefault(strJob, 0)).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("PAYMENT_CURR_FK_IN", lngLocalCurrency).Direction = ParameterDirection.Input;
                    _with3.Parameters.Add("ACC_PAYBLE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    return objWF.GetDataTable("PAYMENTS_TBL_PKG", "FETCH_ACCOUNTS_PAYABLE_FRMJOB");
                }
                else
                {
                    var _with4 = objWF.MyCommand;
                    _with4.Parameters.Add("BIZ_TYPE_IN", intBizType).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("PROCESS_TYPE_IN", intProcessType).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("VOUCHER_NO_IN", getDefault(strJob, 0)).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("PAYMENT_CURR_FK_IN", lngLocalCurrency).Direction = ParameterDirection.Input;
                    _with4.Parameters.Add("ACC_PAYBLE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    return objWF.GetDataTable("PAYMENTS_TBL_PKG", "FETCH_ACCOUNTS_PAYABLE_VOUCHER");
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        /// <summary>
        /// Fetches the cheque details.
        /// </summary>
        /// <param name="lPaymentMstFk">The l payment MST fk.</param>
        /// <returns></returns>
        public DataTable FetchChequeDetails(long lPaymentMstFk)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                var _with5 = objWF.MyCommand;
                _with5.Parameters.Add("PAYMENT_MST_FK_IN", lPaymentMstFk).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("CHEQUE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataTable("PAYMENTS_TBL_PKG", "FETCH_CHEQUE_DETAILS");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
                objWF = null;
            }
        }

        /// <summary>
        /// Fetches the payment details.
        /// </summary>
        /// <param name="lPaymentMstFk">The l payment MST fk.</param>
        /// <param name="lngLocalCurrency">The LNG local currency.</param>
        /// <returns></returns>
        public DataTable FetchPaymentDetails(long lPaymentMstFk, long lngLocalCurrency)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                var _with6 = objWF.MyCommand;
                _with6.Parameters.Add("PAYMENT_MST_FK_IN", lPaymentMstFk).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("PAYMENT_CURR_FK_IN", lngLocalCurrency).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("PAY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataTable("PAYMENTS_TBL_PKG", "FETCH_PAYMENT_DETAILS");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
                objWF = null;
            }
        }

        /// <summary>
        /// Fetches the header information.
        /// </summary>
        /// <param name="lPaymentMstFk">The l payment MST fk.</param>
        /// <returns></returns>
        public DataTable FetchHeaderInformation(long lPaymentMstFk)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                var _with7 = objWF.MyCommand;
                _with7.Parameters.Add("PAYMENT_MST_FK_IN", lPaymentMstFk).Direction = ParameterDirection.Input;
                _with7.Parameters.Add("HEADER_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWF.GetDataTable("PAYMENTS_TBL_PKG", "FETCH_HEADER_DETAILS");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
                objWF = null;
            }
        }

        #endregion "Fetch Data"

        #region "FetchVoucher"

        /// <summary>
        /// Fetchpayments the specified pk.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
        public DataSet fetchpayment(long pk)
        {
            string strSQL = null;
            strSQL = "SELECT COUNT(*)";
            strSQL += " From DOCUMENT_PREF_LOC_MST_TBL D, DOCUMENT_PREFERENCE_MST_TBL DP ";
            strSQL += "WHERE ";
            strSQL += " D.LOCATION_MST_FK = " + pk + " ";
            strSQL += "AND D.DOC_PREFERENCE_FK = DP.DOCUMENT_PREFERENCE_MST_PK";
            strSQL += "AND  DP.DOCUMENT_PREFERENCE_NAME='Payement'";

            WorkFlow objWF = new WorkFlow();
            DataSet objDS = null;
            try
            {
                objDS = objWF.GetDataSet(strSQL);
                return objDS;
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "FetchVoucher"

        #region "Payment details"

        /// <summary>
        /// Fetches the paymentetails.
        /// </summary>
        /// <param name="PaymentPk">The payment pk.</param>
        /// <returns></returns>
        public DataSet FetchPaymentetails(string PaymentPk)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DECODE(PMTT.PAYMENT_MODE, 1, 'Cheque', 2, 'Cash', 3, 'Bank Transfer',4,'DD',5,'PO') P_MODE,");
            sb.Append("     PMTT.CHEQUE_NUMBER,");
            sb.Append("     PMTT.CHEQUE_DATE,");
            sb.Append("     PMTT.BANK_NAME,");
            sb.Append("     ROUND(NVL(PMTT.PAID_AMOUNT,0)*NVL(PMTT.EXCHANGE_RATE,1),2) PAID_AMOUNT");
            sb.Append("  FROM PAYMENTS_MODE_TRN_TBL PMTT,");
            sb.Append("       BANK_MST_TBL          B,");
            sb.Append("       PAYMENTS_TBL          PAY");

            sb.Append(" WHERE PAY.PAYMENT_TBL_PK = PMTT.PAYMENTS_TBL_FK");
            sb.Append("   AND B.BANK_MST_PK(+) = PMTT.BANK_MST_FK");
            sb.Append("    AND PAY.PAYMENT_TBL_PK ='" + PaymentPk + "'");
            try
            {
                return objWK.GetDataSet(sb.ToString());
            }
            catch (OracleException SQLEX)
            {
                throw SQLEX;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Payment details"

        #region "Settlement Details"

        /// <summary>
        /// Fetches the settlementetails.
        /// </summary>
        /// <param name="PaymentPk">The payment pk.</param>
        /// <param name="CurrencyPK">The currency pk.</param>
        /// <returns></returns>
        public DataSet FetchSettlementetails(string PaymentPk, Int32 CurrencyPK = 0)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append("SELECT IST.SUPPLIER_INV_NO VOUCHER_NUMBER,");
            sb.Append("       IST.SUPPLIER_INV_DT VOUCHER_DATE,");
            sb.Append("       IST.SUPPLIER_DUE_DT VOUCHER_DUE_DATE,");
            sb.Append("       CEMT.COST_ELEMENT_NAME SERVICES,");
            sb.Append("       ROUND(ISTT.PAYABLE_AMT *");
            sb.Append("             GET_EX_RATE(IST.CURRENCY_MST_FK, " + Convert.ToString(CurrencyPK) + ", SYSDATE),");
            sb.Append("             2) AS ACTUAL_AMT");
            sb.Append("  FROM PAYMENTS_TBL PT,");
            sb.Append("       PAYMENT_TRN_TBL PTT,");
            sb.Append("       INV_SUPPLIER_TBL IST,");
            sb.Append("       INV_SUPPLIER_TRN_TBL ISTT,");
            sb.Append("       COST_ELEMENT_MST_TBL CEMT");
            sb.Append(" WHERE PT.PAYMENT_TBL_PK = PTT.PAYMENTS_TBL_FK");
            sb.Append("   AND PTT.INV_SUPPLIER_TBL_FK = ISTT.INV_SUPPLIER_TBL_FK");
            sb.Append("   AND IST.INV_SUPPLIER_PK = ISTT.INV_SUPPLIER_TBL_FK");
            sb.Append("   AND PT.PAYMENT_TBL_PK =  '" + PaymentPk + "'");
            sb.Append("   AND CEMT.COST_ELEMENT_MST_PK = ISTT.COST_ELEMENT_MST_FK");
            try
            {
                return objWK.GetDataSet(sb.ToString());
            }
            catch (OracleException SQLEX)
            {
                throw SQLEX;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Settlement Details"

        #region "Billing Address"

        /// <summary>
        /// Fetches the sup adr details.
        /// </summary>
        /// <param name="VendorPK">The vendor pk.</param>
        /// <returns></returns>
        public DataSet FetchSupADRDetails(Int32 VendorPK)
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT VMT.VENDOR_NAME,");
            sb.Append("       VCD.Bill_Address_1,");
            sb.Append("       VCD.BILL_ADDRESS_2,");
            sb.Append("       VCD.BILL_ADDRESS_3,");
            sb.Append("       VCD.BILL_ZIP_CODE,");
            sb.Append("       VCD.BILL_CITY,");
            sb.Append("       VCD.BILL_FAX_NO,");
            sb.Append("       VCD.BILL_EMAIL_ID,");
            sb.Append("       VCD.BILL_URL");
            sb.Append("  FROM VENDOR_MST_TBL VMT,");
            sb.Append("   VENDOR_CONTACT_DTLS VCD");
            sb.Append(" WHERE VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
            sb.Append("   AND VMT.VENDOR_MST_PK = '" + VendorPK + "'");

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

        #endregion "Billing Address"

        #region "To Fetch for jobcard closed"

        /// <summary>
        /// Fetchpayements the specified payementpk.
        /// </summary>
        /// <param name="payementpk">The payementpk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public object fetchpayement(long payementpk = 0, int BizType = 0, int Process = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("select distinct JOB_CARD_SEA_EXP_PK, JOBREFNR FROM(");
                sb.Append("select distinct J.JOB_CARD_TRN_PK JOB_CARD_SEA_EXP_PK,");
                sb.Append("                j.jobcard_ref_no JOBREFNR,");
                sb.Append("                p.payment_ref_no,");
                sb.Append("                INV.INVOICE_AMT,");
                sb.Append("                NVL(PM.PAID_AMOUNT, 0) PAID_AMOUNT,");
                sb.Append("               (INV.INVOICE_AMT - NVL(SUM(DISTINCT PM.PAID_AMOUNT), 0))Payement");
                sb.Append("  from PAYMENT_TRN_TBL       PAY,");
                sb.Append("       PAYMENTS_TBL          P,");
                sb.Append("       INV_SUPPLIER_TBL      INV,");
                sb.Append("       INV_SUPPLIER_TRN_TBL  INVTR,");
                sb.Append("       PAYMENTS_MODE_TRN_TBL Pm,");
                sb.Append("       JOB_CARD_TRN  J,");
                sb.Append("       JOB_TRN_COST  JTC");
                sb.Append(" where P.PAYMENT_TBL_PK = PAY.PAYMENTS_TBL_FK");
                sb.Append("   AND PAY.INV_SUPPLIER_TBL_FK = INV.INV_SUPPLIER_PK");
                sb.Append("   AND PM.PAYMENTS_TBL_FK = P.PAYMENT_TBL_PK");
                sb.Append("   AND INV.INV_SUPPLIER_PK = INVTR.INV_SUPPLIER_TBL_FK");
                sb.Append("   and JTC.INV_SUPPLIER_FK = INV.INV_SUPPLIER_PK");
                sb.Append("   AND j.JOB_CARD_TRN_PK=JTC.JOB_CARD_TRN_FK");
                sb.Append("   AND p.payment_tbl_pk='" + payementpk + "'");
                sb.Append("   AND J.BUSINESS_TYPE=" + BizType);
                sb.Append("   AND J.PROCESS_TYPE=" + Process);
                sb.Append("   group by JOB_CARD_TRN_PK,");
                sb.Append("            j.jobcard_ref_no ,");
                sb.Append("             payment_ref_no,");
                sb.Append("             INVOICE_AMT,");
                sb.Append("             paid_amount)");
                sb.Append("            where payement=0");

                return objWF.GetDataSet(sb.ToString());
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

        /// <summary>
        /// Checkstatuses the specified ds agt collection.
        /// </summary>
        /// <param name="dsAgtCollection">The ds agt collection.</param>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public object checkstatus(DataSet dsAgtCollection, int JobPk = 0, int BizType = 0, int Process = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            Int32 i = default(Int32);
            bool IsDPAgent = false;
            bool IsCBAgent = false;
            try
            {
                if (dsAgtCollection.Tables.Count > 0)
                {
                    if (dsAgtCollection.Tables[0].Rows.Count > 0)
                    {
                        for (i = 0; i <= dsAgtCollection.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!string.IsNullOrEmpty(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"].ToString()))
                            {
                                if (Convert.ToInt32(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"]) == 2)
                                {
                                    IsDPAgent = true;
                                }
                                else if (Convert.ToInt32(dsAgtCollection.Tables[0].Rows[i]["AGENTTYPE"]) == 1)
                                {
                                    IsCBAgent = true;
                                }
                            }
                        }
                    }
                }

                if (IsDPAgent == true & IsCBAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append("  where JOB.JOB_CARD_TRN_PK=" + JobPk);
                    sb.Append("  AND JOB.COLLECTION_STATUS=1");
                    sb.Append("  AND JOB.PAYEMENT_STATUS=1");
                    sb.Append("   AND JOB.BUSINESS_TYPE=" + BizType);
                    sb.Append("   AND JOB.PROCESS_TYPE=" + Process);
                    if (Process == 1)
                    {
                        sb.Append("  AND JOB.HBL_RELEASED_STATUS=1");
                    }
                    else
                    {
                        sb.Append("  AND JOB.DO_STATUS=1");
                    }
                    sb.Append("  AND JOB.DPAGENT_STATUS=1");
                    sb.Append("  AND JOB.CBAGENT_STATUS=1");
                }
                else if (IsDPAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append("  where JOB.JOB_CARD_TRN_PK=" + JobPk);
                    sb.Append("  AND JOB.COLLECTION_STATUS=1");
                    sb.Append("  AND JOB.PAYEMENT_STATUS=1");
                    sb.Append("   AND JOB.BUSINESS_TYPE=" + BizType);
                    sb.Append("   AND JOB.PROCESS_TYPE=" + Process);
                    if (Process == 1)
                    {
                        sb.Append("  AND JOB.HBL_RELEASED_STATUS=1");
                    }
                    else
                    {
                        sb.Append("  AND JOB.DO_STATUS=1");
                    }
                    sb.Append("  AND JOB.DPAGENT_STATUS=1");
                }
                else if (IsCBAgent == true)
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append("  where JOB.JOB_CARD_TRN_PK=" + JobPk);
                    sb.Append("  AND JOB.COLLECTION_STATUS=1");
                    sb.Append("  AND JOB.PAYEMENT_STATUS=1");
                    sb.Append("   AND JOB.BUSINESS_TYPE=" + BizType);
                    sb.Append("   AND JOB.PROCESS_TYPE=" + Process);
                    if (Process == 1)
                    {
                        sb.Append("  AND JOB.HBL_RELEASED_STATUS=1");
                    }
                    else
                    {
                        sb.Append("  AND JOB.DO_STATUS=1");
                    }
                    sb.Append("  AND JOB.CBAGENT_STATUS=1");
                }
                else
                {
                    sb.Append("select * FROM  JOB_CARD_TRN JOB");
                    sb.Append(" where JOB.JOB_CARD_TRN_PK=" + JobPk);
                    sb.Append(" AND JOB.COLLECTION_STATUS=1");
                    sb.Append(" AND JOB.PAYEMENT_STATUS=1");
                    sb.Append("   AND JOB.BUSINESS_TYPE=" + BizType);
                    sb.Append("   AND JOB.PROCESS_TYPE=" + Process);
                    if (Process == 1)
                    {
                        sb.Append(" AND JOB.HBL_RELEASED_STATUS=1");
                    }
                    else
                    {
                        sb.Append("  AND JOB.DO_STATUS=1");
                    }
                }
                if (Process == 2)
                {
                    sb.Replace("DPAGENT_STATUS", "LOADAGENT_STATUS");
                }
                return objWF.GetDataSet(sb.ToString());
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

        /// <summary>
        /// Updatejobcarddates the specified job pk.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public ArrayList updatejobcarddate(int JobPk = 0, int BizType = 0, int Process = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            string str = null;
            Int16 intIns = default(Int16);
            try
            {
                updCmdUser.Transaction = TRAN;
                str = "UPDATE JOB_CARD_TRN  j SET ";
                str += "   j.JOB_CARD_STATUS = 2, j.JOB_CARD_CLOSED_ON = SYSDATE";
                str += " WHERE j.JOB_CARD_TRN_PK=" + JobPk;

                //If BizType = 2 And Process = 1 Then
                //    str = "UPDATE job_card_sea_exp_tbl  j SET "
                //    str &= "   j.JOB_CARD_STATUS = 2, j.JOB_CARD_CLOSED_ON = SYSDATE"
                //    str &= " WHERE j.job_card_sea_exp_pk=" & JobPk
                //ElseIf BizType = 2 And Process = 2 Then
                //    str = "UPDATE job_card_sea_imp_tbl  JA SET "
                //    str &= "   JA.JOB_CARD_STATUS = 2, JA.JOB_CARD_CLOSED_ON =SYSDATE "
                //    str &= " WHERE JA.Job_Card_Sea_Imp_Pk=" & JobPk
                //ElseIf BizType = 1 And Process = 1 Then
                //    str = "UPDATE job_card_air_exp_tbl JAE SET "
                //    str &= "   JAE.JOB_CARD_STATUS = 2, JAE.JOB_CARD_CLOSED_ON =SYSDATE"
                //    str &= " WHERE JAE.JOB_CARD_AIR_EXP_PK=" & JobPk
                //ElseIf BizType = 1 And Process = 2 Then
                //    str = "UPDATE job_card_air_imp_tbl JAI SET "
                //    str &= "   JAI.JOB_CARD_STATUS = 2, JAI.JOB_CARD_CLOSED_ON =SYSDATE"
                //    str &= " WHERE  JAI.JOB_CARD_AIR_IMP_PK=" & JobPk
                //End If

                var _with8 = updCmdUser;
                _with8.Connection = objWK.MyConnection;
                _with8.Transaction = TRAN;
                _with8.CommandType = CommandType.Text;
                _with8.CommandText = str;
                intIns = Convert.ToInt16(_with8.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("Protocol Generated Succesfully");
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        /// <summary>
        /// Updates the payement jobcard jobcard.
        /// </summary>
        /// <param name="JobPk">The job pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public ArrayList UpdatePayementJobcardJobcard(int JobPk = 0, int BizType = 0, int Process = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand updCmdUser = new OracleCommand();
            string str = null;
            Int16 intIns = default(Int16);
            try
            {
                updCmdUser.Transaction = TRAN;
                str = "UPDATE JOB_CARD_TRN  j SET ";
                str += "   j.Payement_Status = 1,j.payement_date = SYSDATE";
                str += " WHERE j.JOB_CARD_TRN_PK=" + JobPk;

                var _with9 = updCmdUser;
                _with9.Connection = objWK.MyConnection;
                _with9.Transaction = TRAN;
                _with9.CommandType = CommandType.Text;
                _with9.CommandText = str;
                intIns = Convert.ToInt16(_with9.ExecuteNonQuery());
                if (intIns > 0)
                {
                    TRAN.Commit();
                    arrMessage.Add("Protocol Generated Succesfully");
                    return arrMessage;
                }
            }
            catch (OracleException OraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(OraEx.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        #endregion "To Fetch for jobcard closed"

        #region "Save Data"

        /// <summary>
        /// Saves the data.
        /// </summary>
        /// <param name="dsAccPayables">The ds acc payables.</param>
        /// <param name="PaymentCollection">The payment collection.</param>
        /// <param name="OperationMode">The operation mode.</param>
        /// <param name="txtRefNo">The text reference no.</param>
        /// <param name="Base">The base.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="ApprStatus">The appr status.</param>
        /// <param name="CrLimit">The cr limit.</param>
        /// <param name="VendPK">The vend pk.</param>
        /// <param name="NetAmt">The net amt.</param>
        /// <param name="PaymentDate">The payment date.</param>
        /// <param name="CurrencyPK">The currency pk.</param>
        /// <returns></returns>
        public ArrayList SaveData(DataSet dsAccPayables, Collection PaymentCollection, string OperationMode, System.Web.UI.HtmlControls.HtmlInputText txtRefNo, string Base, int BizType = 0, int Process = 0, int ApprStatus = 0, double CrLimit = 0, int VendPK = 0,
        double NetAmt = 0, string PaymentDate = "", int CurrencyPK = 0)
        {
            WorkFlow objWf = new WorkFlow();
            OracleTransaction oraTran = null;
            string strResult = null;
            string strRefNo = null;
            bool bCanCommit = false;
            //Dim objWK As New WorkFlow
            //objWK.OpenConnection()
            OracleTransaction TRAN = null;
            try
            {
                var _with10 = objWf;
                _with10.OpenConnection();
                oraTran = _with10.MyConnection.BeginTransaction();
                _with10.MyCommand.Connection = _with10.MyConnection;
                _with10.MyCommand.Transaction = oraTran;
                _with10.MyCommand.CommandType = CommandType.StoredProcedure;
                strResult = SavePayments(PaymentCollection, OperationMode, objWf, strRefNo);
                if (strResult.ToLower() == "saved")
                {
                    strResult = string.Empty;
                    strResult = SavePayments_Trn(dsAccPayables.Tables["Payments_Trn"], OperationMode, objWf);
                    if (strResult.ToLower() == "saved")
                    {
                        strResult = "saved";
                        if (!(Base == "Approval"))
                        {
                            strResult = SavePayments_Mode(dsAccPayables.Tables["Payments_Mode"], OperationMode, objWf, oraTran);
                        }
                        if (strResult.ToLower() == "saved")
                        {
                            bCanCommit = true;
                        }
                        else
                        {
                            bCanCommit = false;
                        }
                    }
                }
                if (bCanCommit)
                {
                    if (ApprStatus == 1)
                    {
                        if (CrLimit > 0)
                        {
                            SaveCreditLimit(NetAmt, VendPK, oraTran, CurrencyPK, Convert.ToDateTime(PaymentDate));
                        }
                    }
                    oraTran.Commit();

                    //Push to financial system if realtime is selected
                    if (ApprStatus == 1)
                    {
                        if (_Payment_Tbl_Pk > 0)
                        {
                            if (Convert.ToBoolean(ConfigurationSettings.AppSettings["QFINGeneral"]) == true)
                            {
                                try
                                {
                                    //objWK.MyCommand.Parameters.Clear()
                                    oraTran = objWf.MyConnection.BeginTransaction();
                                    objWf.MyCommand.Transaction = oraTran;
                                    objWf.MyCommand.Parameters.Clear();
                                    objWf.MyCommand.CommandText = objWf.MyUserName + ".ACCOUNTING_INTEGREATION_PKG.DATA_PUSH_PAYMENT_APPROVE";
                                    objWf.MyCommand.Parameters.Add("PAYMENT_PK_IN", _Payment_Tbl_Pk).Direction = ParameterDirection.Input;
                                    objWf.MyCommand.Parameters.Add("LOCAL_CUR_FK_IN", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                                    objWf.MyCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                    objWf.MyCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                                    objWf.MyCommand.ExecuteNonQuery();
                                    oraTran.Commit();
                                }
                                catch (Exception ex)
                                {
                                }
                            }

                            Quantum_QFOR.cls_Scheduler objSch = new Quantum_QFOR.cls_Scheduler();
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
                                //    objPush.UpdateTransaction(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, _Payment_Tbl_Pk);
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                                //    }
                                //}
                                //catch (Exception ex)
                                //{
                                //    if (ConfigurationSettings.AppSettings["EVENTVIEWER"])
                                //    {
                                //        objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
                                //    }
                                //}
                            }
                        }
                    }
                    //*****************************************************************

                    txtRefNo.Value = strRefNo;
                    if (!(Base == "Approval"))
                    {
                        //Air - Export
                        if (BizType == 1 & Process == 1)
                        {
                            SaveTrackAndTraceForPay(oraTran, Convert.ToInt32(_Payment_Tbl_Pk), 1, 1, "Payment", "PAY-AIR-EXP", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), objWf, "INS", Convert.ToInt64(PaymentCollection["LOGGED_IN_USER"]),
                            "O");
                            //Air - Import
                        }
                        else if (BizType == 1 & Process == 2)
                        {
                            SaveTrackAndTraceForPay(oraTran, Convert.ToInt32(_Payment_Tbl_Pk), 1, 2, "Payment", "PAY-AIR-IMP", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), objWf, "INS", Convert.ToInt64(PaymentCollection["LOGGED_IN_USER"]),
                            "O");
                            //Sea - Export
                        }
                        else if (BizType == 2 & Process == 1)
                        {
                            SaveTrackAndTraceForPay(oraTran, Convert.ToInt32(_Payment_Tbl_Pk), 2, 1, "Payment", "PAY-SEA-EXP", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), objWf, "INS", Convert.ToInt64(PaymentCollection["LOGGED_IN_USER"]),
                            "O");
                            //Sea - Import
                        }
                        else if (BizType == 2 & Process == 2)
                        {
                            SaveTrackAndTraceForPay(oraTran, Convert.ToInt32(_Payment_Tbl_Pk), 2, 2, "Payment", "PAY-SEA-IMP", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), objWf, "INS", Convert.ToInt64(PaymentCollection["LOGGED_IN_USER"]),
                            "O");
                        }
                    }
                }
                else
                {
                    oraTran.Rollback();
                }
                arrMessage.Add(strResult);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                oraTran.Rollback();
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWf.MyCommand.Cancel();
                objWf = null;
            }
            return arrMessage;
        }

        /// <summary>
        /// Saves the payments_ TRN.
        /// </summary>
        /// <param name="dtPayments_Trn">The dt payments_ TRN.</param>
        /// <param name="OperationMode">The operation mode.</param>
        /// <param name="objWf">The object wf.</param>
        /// <returns></returns>
        private string SavePayments_Trn(DataTable dtPayments_Trn, string OperationMode, WorkFlow objWf)
        {
            try
            {
                objWf.MyCommand.Parameters.Clear();
                int intRowAffected = 0;
                var _with11 = objWf.MyCommand;
                _with11.Parameters.Add("PAID_AMOUNT_HDR_CURR_IN", OracleDbType.Double, 10, "CURRENT_PAYMENT_IN_LOCAL_CURR").Direction = ParameterDirection.Input;
                _with11.Parameters["PAID_AMOUNT_HDR_CURR_IN"].SourceVersion = DataRowVersion.Current;
                if (OperationMode == "Add")
                {
                    _with11.CommandText = objWf.MyUserName + ".PAYMENTS_TBL_PKG.PAYMENTS_TRN_TBL_INS";
                    _with11.Parameters.Add("PAYMENTS_TBL_FK_IN", _Payment_Tbl_Pk).Direction = ParameterDirection.Input;
                    _with11.Parameters.Add("INV_SUPPLIER_TBL_FK_IN", OracleDbType.Int32, 10, "INV_SUPPLIER_TBL_FK").Direction = ParameterDirection.Input;
                    _with11.Parameters["INV_SUPPLIER_TBL_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with11.Parameters.Add("EXISTING_PAID_AMT_HDR_CURR_IN", OracleDbType.Int32, 10, "EXISTING_PAID_AMT_HDR_CURR").Direction = ParameterDirection.Input;
                    _with11.Parameters["EXISTING_PAID_AMT_HDR_CURR_IN"].SourceVersion = DataRowVersion.Current;
                    _with11.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    var _with12 = objWf.MyDataAdapter;
                    _with12.InsertCommand = objWf.MyCommand;
                    intRowAffected = _with12.Update(dtPayments_Trn);
                }
                else
                {
                    _with11.CommandText = objWf.MyUserName + ".PAYMENTS_TBL_PKG.PAYMENTS_TRN_TBL_UPD";
                    _with11.Parameters.Add("PAYMENTS_TRN_PK_IN", OracleDbType.Int32, 10, "SEL").Direction = ParameterDirection.Input;
                    _with11.Parameters["PAYMENTS_TRN_PK_IN"].SourceVersion = DataRowVersion.Current;
                    _with11.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    var _with13 = objWf.MyDataAdapter;
                    _with13.UpdateCommand = objWf.MyCommand;
                    intRowAffected = _with13.Update(dtPayments_Trn);
                }
                return "saved";
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

        /// <summary>
        /// Saves the payments_ mode.
        /// </summary>
        /// <param name="dtPayments_Mode">The dt payments_ mode.</param>
        /// <param name="OperationMode">The operation mode.</param>
        /// <param name="objWf">The object wf.</param>
        /// <param name="TRAN">The tran.</param>
        /// <returns></returns>
        private string SavePayments_Mode(DataTable dtPayments_Mode, string OperationMode, WorkFlow objWf, OracleTransaction TRAN)
        {
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            try
            {
                objWf.MyCommand.Parameters.Clear();
                int intRowAffected = 0;
                var _with14 = insCommand;
                _with14.Connection = objWf.MyConnection;
                _with14.CommandType = CommandType.StoredProcedure;
                _with14.CommandText = objWf.MyUserName + ".PAYMENTS_TBL_PKG.PAYMENTS_MODE_TRN_TBL_INS";

                _with14.Parameters.Add("PAYMENTS_TBL_FK_IN", _Payment_Tbl_Pk).Direction = ParameterDirection.Input;
                _with14.Parameters.Add("PAYMENT_MODE_IN", OracleDbType.Int32, 1, "PAYMENT_MODE").Direction = ParameterDirection.Input;
                _with14.Parameters["PAYMENT_MODE_IN"].SourceVersion = DataRowVersion.Current;
                _with14.Parameters.Add("CHEQUE_NUMBER_IN", OracleDbType.Int32, 10, "CHEQUE_NUMBER").Direction = ParameterDirection.Input;
                _with14.Parameters["CHEQUE_NUMBER_IN"].SourceVersion = DataRowVersion.Current;
                _with14.Parameters.Add("CHEQUE_DATE_IN", OracleDbType.Varchar2, 15, "CHEQUE_DATE").Direction = ParameterDirection.Input;
                _with14.Parameters["CHEQUE_DATE_IN"].SourceVersion = DataRowVersion.Current;
                _with14.Parameters.Add("BANK_MST_FK_IN", OracleDbType.Int32, 10, "BANK_MST_FK").Direction = ParameterDirection.Input;
                _with14.Parameters["BANK_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with14.Parameters.Add("BANK_NAME_IN", OracleDbType.Varchar2, 100, "BANK_NAME").Direction = ParameterDirection.Input;
                _with14.Parameters["BANK_NAME_IN"].SourceVersion = DataRowVersion.Current;
                _with14.Parameters.Add("PAID_AMOUNT_IN", OracleDbType.Double, 10, "PAID_AMOUNT").Direction = ParameterDirection.Input;
                _with14.Parameters["PAID_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                _with14.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Double, 10, "EXCHANGE_RATE").Direction = ParameterDirection.Input;
                _with14.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;
                _with14.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                _with14.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with14.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                var _with15 = updCommand;
                _with15.Connection = objWf.MyConnection;
                _with15.CommandType = CommandType.StoredProcedure;
                _with15.CommandText = objWf.MyUserName + ".PAYMENTS_TBL_PKG.PAYMENTS_MODE_TRN_TBL_UPD";

                _with15.Parameters.Add("PAYMENTS_MODE_TRN_PK_IN", OracleDbType.Int32, 10, "PAYMENTS_MODE_TRN_PK").Direction = ParameterDirection.Input;
                _with15.Parameters["PAYMENTS_MODE_TRN_PK_IN"].SourceVersion = DataRowVersion.Current;
                _with15.Parameters.Add("PAYMENT_MODE_IN", OracleDbType.Int32, 1, "PAYMENT_MODE").Direction = ParameterDirection.Input;
                _with15.Parameters["PAYMENT_MODE_IN"].SourceVersion = DataRowVersion.Current;
                _with15.Parameters.Add("CHEQUE_NUMBER_IN", OracleDbType.Int32, 10, "CHEQUE_NUMBER").Direction = ParameterDirection.Input;
                _with15.Parameters["CHEQUE_NUMBER_IN"].SourceVersion = DataRowVersion.Current;
                _with15.Parameters.Add("CHEQUE_DATE_IN", OracleDbType.Varchar2, 15, "CHEQUE_DATE").Direction = ParameterDirection.Input;
                _with15.Parameters["CHEQUE_DATE_IN"].SourceVersion = DataRowVersion.Current;
                _with15.Parameters.Add("BANK_MST_FK_IN", OracleDbType.Int32, 10, "BANK_MST_FK").Direction = ParameterDirection.Input;
                _with15.Parameters["BANK_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with15.Parameters.Add("BANK_NAME_IN", OracleDbType.Varchar2, 100, "BANK_NAME").Direction = ParameterDirection.Input;
                _with15.Parameters["BANK_NAME_IN"].SourceVersion = DataRowVersion.Current;
                _with15.Parameters.Add("PAID_AMOUNT_IN", OracleDbType.Double, 10, "PAID_AMOUNT").Direction = ParameterDirection.Input;
                _with15.Parameters["PAID_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                _with15.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Double, 10, "EXCHANGE_RATE").Direction = ParameterDirection.Input;
                _with15.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;
                _with15.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                _with15.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with15.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

                var _with16 = objWf.MyDataAdapter;
                _with16.InsertCommand = insCommand;
                _with16.InsertCommand.Transaction = TRAN;
                _with16.UpdateCommand = updCommand;
                _with16.UpdateCommand.Transaction = TRAN;
                intRowAffected = _with16.Update(dtPayments_Mode);

                if (arrMessage.Count > 0)
                {
                    return "error";
                }
                else
                {
                    return "saved";
                }
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

        /// <summary>
        /// Saves the payments.
        /// </summary>
        /// <param name="PaymentCollection">The payment collection.</param>
        /// <param name="OperationMode">The operation mode.</param>
        /// <param name="objWf">The object wf.</param>
        /// <param name="strRefNo">The string reference no.</param>
        /// <returns></returns>
        private string SavePayments(Collection PaymentCollection, string OperationMode, WorkFlow objWf, string strRefNo)
        {
            try
            {
                objWf.MyCommand.Parameters.Clear();
                int intRowAffected = 0;
                if (OperationMode == "Add")
                {
                    strRefNo = GenerateProtocolKey("PAYMENT", Convert.ToInt64(PaymentCollection["LOGGED_IN_LOC"]), Convert.ToInt64(PaymentCollection["EMP_PK"]), DateTime.Now, "", "", "", Convert.ToInt64(PaymentCollection["LOGGED_IN_USER"]), objWf);
                    var _with17 = objWf.MyCommand;
                    _with17.CommandText = objWf.MyUserName + ".PAYMENTS_TBL_PKG.PAYMENTS_TBL_INS";
                    _with17.Parameters.Clear();
                    _with17.Parameters.Add("PROCESS_TYPE_IN", PaymentCollection["PROCESS_TYPE"]).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("BUSINESS_TYPE_IN", PaymentCollection["BUSINESS_TYPE"]).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("JOB_TYPE_IN", PaymentCollection["JOB_TYPE"]).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("PAYMENT_REF_NO_IN", strRefNo).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("PAYMENT_DATE_IN", PaymentCollection["PAYMENT_DATE"]).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("REMARKS_IN", PaymentCollection["REMARKS"]).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("APPROVED_IN", PaymentCollection["APPROVED"]).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("APPROVED_BY_IN", PaymentCollection["APPROVED_BY"]).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("APPROVED_DATE_IN", PaymentCollection["APPROVED_DATE"]).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("PAYMENT_AMT_IN", Convert.ToDouble(PaymentCollection["PAYMENT_AMT"].ToString().Replace(",", ""))).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("CURRENCY_MST_FK_IN", PaymentCollection["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("VENDOR_MST_FK_IN", PaymentCollection["VENDOR_MST_FK"]).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("CREATED_BY_FK_IN", PaymentCollection["LOGGED_IN_USER"]).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("CONFIG_MST_PK_IN", PaymentCollection["CONFIG_MST_PK"]).Direction = ParameterDirection.Input;
                    _with17.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with17.ExecuteNonQuery();
                }
                else
                {
                    var _with18 = objWf.MyCommand;
                    _with18.CommandText = objWf.MyUserName + ".PAYMENTS_TBL_PKG.PAYMENTS_TBL_UPD";
                    _with18.Parameters.Clear();
                    _with18.Parameters.Add("PAYMENTS_TBL_PK_IN", PaymentCollection["PAYMENT_TBL_PK"]).Direction = ParameterDirection.Input;
                    _with18.Parameters.Add("REMARKS_IN", PaymentCollection["REMARKS"]).Direction = ParameterDirection.Input;
                    _with18.Parameters.Add("APPROVED_IN", PaymentCollection["APPROVED"]).Direction = ParameterDirection.Input;
                    _with18.Parameters.Add("PAYMENT_AMT_IN", Convert.ToDouble(PaymentCollection["PAYMENT_AMT"].ToString().Replace(",", ""))).Direction = ParameterDirection.Input;
                    _with18.Parameters.Add("LAST_MODIFIED_BY_FK_IN", PaymentCollection["LOGGED_IN_USER"]).Direction = ParameterDirection.Input;
                    _with18.Parameters.Add("VERSION_NO_IN", PaymentCollection["VERSION_NO"]).Direction = ParameterDirection.Input;
                    _with18.Parameters.Add("CONFIG_MST_PK_IN", PaymentCollection["CONFIG_MST_PK"]).Direction = ParameterDirection.Input;
                    _with18.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with18.ExecuteNonQuery();
                }
                _Payment_Tbl_Pk = Convert.ToInt64(objWf.MyCommand.Parameters["RETURN_VALUE"].Value);
                return "saved";
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

        #endregion "Save Data"

        #region "save TrackAndTrace"

        /// <summary>
        /// Saves the track and trace for pay.
        /// </summary>
        /// <param name="TRAN">The tran.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="Status">The status.</param>
        /// <param name="OnStatus">The on status.</param>
        /// <param name="Locationfk">The locationfk.</param>
        /// <param name="objWF2">The object w f2.</param>
        /// <param name="flagInsUpd">The flag ins upd.</param>
        /// <param name="lngCreatedby">The LNG createdby.</param>
        /// <param name="PkStatus">The pk status.</param>
        /// <returns></returns>
        public ArrayList SaveTrackAndTraceForPay(OracleTransaction TRAN, int PkValue, int BizType, int Process, string Status, string OnStatus, int Locationfk, WorkFlow objWF2, string flagInsUpd, long lngCreatedby,
        string PkStatus)
        {
            Int32 retVal = default(Int32);
            Int32 RecAfct = default(Int32);
            WorkFlow objWf1 = new WorkFlow();
            objWf1.OpenConnection();

            OracleTransaction TRAN1 = null;
            TRAN1 = objWf1.MyConnection.BeginTransaction();
            objWf1.MyCommand.Transaction = TRAN1;
            try
            {
                var _with19 = objWf1.MyCommand;
                _with19.CommandType = CommandType.StoredProcedure;
                _with19.CommandText = objWf1.MyUserName + ".TRACK_N_TRACE_PKG.TRACK_N_TRACE_INS";
                _with19.Transaction = TRAN1;
                _with19.Parameters.Clear();
                _with19.Parameters.Add("Key_fk_in", PkValue).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("status_in", Status).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("locationfk_in", Locationfk).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("OnStatus_in", OnStatus).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("pkStatus_in", PkStatus).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("flagInsUpd_in", flagInsUpd).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("Container_Data_in", "").Direction = ParameterDirection.Input;
                _with19.Parameters.Add("CreatedUser_in", lngCreatedby).Direction = ParameterDirection.Input;
                _with19.Parameters.Add("Return_value", OracleDbType.NVarchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with19.ExecuteNonQuery();
                TRAN1.Commit();
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
                objWf1.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }

        #endregion "save TrackAndTrace"

        /// <summary>
        /// Fetches the job count.
        /// </summary>
        /// <param name="SupInvFK">The sup inv fk.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <returns></returns>
        public DataSet FetchJobCount(string SupInvFK = "", int JobType = 1)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow ObjWF = new WorkFlow();

            try
            {
                if (JobType == 1)
                {
                    sb.Append(" SELECT DISTINCT JC.VOYAGE_TRN_FK");
                    sb.Append("  FROM INV_SUPPLIER_TBL INVSUP,INV_SUPPLIER_TRN_TBL INVTRN,JOB_CARD_TRN           JC");
                    sb.Append("   WHERE INVSUP.INV_SUPPLIER_PK = INVTRN.INV_SUPPLIER_TBL_FK ");
                    sb.Append(" AND INVTRN.JOBCARD_REF_NO = JC.JOBCARD_REF_NO  AND INVSUP.JOB_TYPE = 1 AND INVSUP.INV_SUPPLIER_PK  IN( " + SupInvFK + ")");
                }
                else if (JobType == 2)
                {
                    sb.Append(" SELECT DISTINCT JC.VOYAGE_TRN_FK");
                    sb.Append("  FROM INV_SUPPLIER_TBL INVSUP,INV_SUPPLIER_TRN_TBL INVTRN,CBJC_TBL           JC");
                    sb.Append("   WHERE INVSUP.INV_SUPPLIER_PK = INVTRN.INV_SUPPLIER_TBL_FK ");
                    sb.Append(" AND INVTRN.JOBCARD_REF_NO = JC.CBJC_NO  AND INVSUP.JOB_TYPE = 1 AND INVSUP.INV_SUPPLIER_PK  IN( " + SupInvFK + ")");
                }
                else
                {
                    sb.Append("  SELECT DISTINCT JC.VSL_VOY_FK");
                    sb.Append("  FROM INV_SUPPLIER_TBL INVSUP,INV_SUPPLIER_TRN_TBL INVTRN,TRANSPORT_INST_SEA_TBL           JC");
                    sb.Append("   WHERE INVSUP.INV_SUPPLIER_PK = INVTRN.INV_SUPPLIER_TBL_FK ");
                    sb.Append(" AND INVTRN.JOBCARD_REF_NO = JC.TRANS_INST_REF_NO  AND INVSUP.JOB_TYPE = 1 AND INVSUP.INV_SUPPLIER_PK  IN( " + SupInvFK + ")");
                }

                return ObjWF.GetDataSet(sb.ToString());
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
        /// Saves the credit limit.
        /// </summary>
        /// <param name="NetAmt">The net amt.</param>
        /// <param name="VendPK">The vend pk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="InvCurPk">The inv current pk.</param>
        /// <param name="invdate">The invdate.</param>
        public void SaveCreditLimit(double NetAmt, int VendPK, OracleTransaction TRAN, int InvCurPk, System.DateTime invdate)
        {
            WorkFlow objWK1 = new WorkFlow();
            Int16 exe = default(Int16);
            double ROE = 0;
            OracleCommand cmd = new OracleCommand();
            string strSQL = null;
            int VenCurPK = 0;
            //Dim Invdate As String
            strSQL = " select nvl(vmt.credit_cur_fk,0) from vendor_mst_tbl vmt where vmt.vendor_mst_pk=" + VendPK;
            VenCurPK = Convert.ToInt32(objWK1.ExecuteScaler(strSQL));
            //invdate = invdate.s
            strSQL = "select get_ex_rate(" + InvCurPk + "," + VenCurPK + ",to_date('" + invdate.ToString("dd/MM/yyyy") + "',DATEFORMAT)) from dual";
            ROE = Convert.ToInt32(objWK1.ExecuteScaler(strSQL));

            try
            {
                cmd.CommandType = CommandType.Text;
                cmd.Connection = TRAN.Connection;
                cmd.Transaction = TRAN;

                cmd.Parameters.Clear();
                strSQL = "update VENDOR_MST_TBL a set a.credit_used = nvl(a.credit_used,0) - round(" + ROE * NetAmt + ",2)";
                strSQL = strSQL + " where a.VENDOR_MST_PK =" + VendPK;
                cmd.CommandText = strSQL;
                cmd.ExecuteNonQuery();
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
    }
}