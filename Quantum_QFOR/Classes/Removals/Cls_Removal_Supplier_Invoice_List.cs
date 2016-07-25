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
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class Cls_Removal_Supplier_Invoice : CommonFeatures
    {
        private DataSet ObjTrn_DataSet;

        public DataSet M_dataset
        {
            set { ObjTrn_DataSet = value; }
        }

        #region "Grid Header"

        public enum Header
        {
            SrNo = 0,
            JobCardRefNo = 1,
            JobCardSeaExpPk = 2,
            JobCardPiaPK = 3,
            CostElementId = 4,
            CostElementPK = 5,
            InvoiceSupplierFk = 6,
            EstimatedCost = 7,
            ActualCost = 8,
            Difference = 9,
            Check = 10
        }

        #endregion "Grid Header"

        #region "Fetch JobCardInvoice"

        public DataSet FetchJobCardInv(Int32 VendorPK, string FromDt = "", string ToDt = "", Int32 CostElementPk = 0, Int32 CurrencyPK = 0, Int32 TradePK = 0, string Vsl = "", string INVOICENO = "", Int64 lblInvSupplierPK = 0, string General = "")
        {
            StringBuilder strSql = new StringBuilder();
            string strCondition = null;

            string BusinessProcess = null;
            string VslFlight = null;

            if ((FromDt != null) & (ToDt != null))
            {
                strCondition = " AND JOB_EXP.JOB_CARD_DATE BETWEEN TO_DATE('" + FromDt + "','" + dateFormat + "')  AND TO_DATE('" + ToDt + "','" + dateFormat + "')  ";
            }
            else if ((FromDt != null))
            {
                strCondition = " AND JOB_EXP.JOB_CARD_DATE >= TO_DATE('" + FromDt + "','" + dateFormat + "') ";
            }
            else if ((ToDt != null))
            {
                strCondition = " AND JOB_EXP.JOB_CARD_DATE >= TO_DATE('" + ToDt + "','" + dateFormat + "') ";
            }

            if (!string.IsNullOrEmpty(INVOICENO))
            {
                strCondition += " AND INV_SUP.INVOICE_REF_NO ='" + INVOICENO + "'";
            }

            if (CostElementPk != 0)
            {
                strCondition += " AND COST_ELE.COST_ELEMENT_MST_PK =" + CostElementPk;
            }

            if (lblInvSupplierPK != 0)
            {
                strSql.Append("SELECT ROWNUM SrNO,");
                strSql.Append("JOB_EXP.JOB_CARD_REF AS REF_NR,");
                strSql.Append("JOB_EXP.JOB_CARD_PK AS JOB_CARD_SEA_EXP_PK,");
                strSql.Append(" JOB_TRN_PIA.JC_COST_DTL_PK AS JOB_TRN_SEA_EXP_PIA_PK,");
                strSql.Append("INV_SUP.REM_INV_SUPPLIER_PK  INV_SUPPLIER_FK ,");
                strSql.Append("INV_TRN.REM_INV_SUPPLIER_TRN_PK INV_SUPPLIER_TRN_FK ,");
                strSql.Append("COST_ELE.COST_ELEMENT_ID AS COST_ELEMENT_ID,");
                strSql.Append("COST_ELE.COST_ELEMENT_MST_PK AS COST_ELEMENT_MST_PK,");
                strSql.Append(" JOB_TRN_PIA.VENDOR_MST_FK AS VENDOR_MST_FK,");
                if (Convert.ToInt32(General) == 2)
                {
                    strSql.Append(" '' ESTIMATED_AMT,");
                }
                else
                {
                    strSql.Append("ROUND(INV_TRN.ESTIMATED_AMT * GET_EX_RATE(INV_SUP.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",SYSDATE),2) AS ESTIMATED_AMT,");
                }

                strSql.Append("ROUND(INV_TRN.ACTUAL_Amt * GET_EX_RATE(INV_SUP.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",SYSDATE),2) AS ACTUAL_AMT,");
                if (Convert.ToInt32(General) == 2)
                {
                    strSql.Append(" '' DIFFERENCE_AMT,");
                }
                else
                {
                    strSql.Append(" (CASE WHEN INV_TRN.ESTIMATED_AMT IS NOT NULL AND INV_TRN.ACTUAL_AMT IS NOT NULL THEN ");
                    strSql.Append(" NVL(ROUND(INV_TRN.ESTIMATED_AMT * GET_EX_RATE(INV_SUP.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",SYSDATE),2),0)- NVL(ROUND(INV_TRN.ACTUAL_AMT * GET_EX_RATE(INV_SUP.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",SYSDATE),2),0) ");
                    strSql.Append(" ELSE NULL END) DIFFERENCE_AMT, ");
                }
                strSql.Append(" 'true' Sel");
                strSql.Append(" FROM REM_INV_SUPPLIER_TBL INV_SUP,");
                strSql.Append("REM_INV_SUPPLIER_TRN_TBL INV_TRN,");
                strSql.Append("REM_T_JC_COST_DTL_TBL JOB_TRN_PIA,");
                strSql.Append("rem_m_job_card_mst_tbl JOB_EXP,");
                strSql.Append(" COST_ELEMENT_MST_TBL COST_ELE");
                strSql.Append(" WHERE INV_SUP.REM_INV_SUPPLIER_PK = INV_TRN.REM_INV_SUPPLIER_FK");
                //strSql.Append(" AND INV_SUP.VENDOR_MST_FK=" & VendorPK)
                strSql.Append(" AND INV_TRN.JC_COST_DTL_FK=JOB_TRN_PIA.JC_COST_DTL_PK(+)");
                strSql.Append(" AND JOB_TRN_PIA.JOB_CARD_FK = JOB_EXP.JOB_CARD_PK(+)");
                strSql.Append(" AND COST_ELE.COST_ELEMENT_MST_PK=INV_TRN.COST_ELEMENT_MST_FK");
            }
            else
            {
                strSql.Append("SELECT ROWNUM SrNO ,");
                strSql.Append("JOB_EXP.JOB_CARD_REF AS REF_NR,");
                strSql.Append("JOB_EXP.JOB_CARD_PK AS JOB_CARD_SEA_EXP_PK,");
                strSql.Append("JOB_TRN_PIA.JC_COST_DTL_PK AS JOB_TRN_SEA_EXP_PIA_PK,");
                strSql.Append("JOB_TRN_PIA.INV_REF_PK ,");
                strSql.Append("'' INV_SUPPLIER_TRN_FK ,");
                strSql.Append("COST_ELE.COST_ELEMENT_ID AS COST_ELEMENT_ID,");
                strSql.Append("COST_ELE.COST_ELEMENT_MST_PK AS COST_ELEMENT_MST_PK,");
                strSql.Append("JOB_TRN_PIA.VENDOR_MST_FK AS VENDOR_MST_FK,");
                strSql.Append("ROUND(JOB_TRN_PIA.ESTIMATED_AMT * GET_EX_RATE(JOB_TRN_PIA.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",SYSDATE),2) AS ESTIMATED_AMT,");
                strSql.Append("ROUND(JOB_TRN_PIA.Invoice_Amt * GET_EX_RATE(JOB_TRN_PIA.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",SYSDATE),2) AS ACTUAL_AMT,");
                strSql.Append(" (CASE WHEN JOB_TRN_PIA.ESTIMATED_AMT IS NOT NULL AND JOB_TRN_PIA.Invoice_Amt IS NOT NULL THEN ");
                strSql.Append(" NVL(ROUND(JOB_TRN_PIA.ESTIMATED_AMT * GET_EX_RATE(JOB_TRN_PIA.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",SYSDATE),2),0) - NVL(ROUND(JOB_TRN_PIA.Invoice_Amt * GET_EX_RATE(JOB_TRN_PIA.Currency_Mst_Fk," + Convert.ToString(CurrencyPK) + ",SYSDATE),2),0)");
                strSql.Append(" ELSE NULL END) DIFFERENCE_AMT, ");
                strSql.Append(" '' Sel");
                strSql.Append(" FROM ");
                strSql.Append("COST_ELEMENT_MST_TBL COST_ELE,");
                strSql.Append("CURRENCY_TYPE_MST_TBL CURR, ");
                strSql.Append("rem_m_job_card_mst_tbl JOB_EXP,  REM_T_JC_COST_DTL_TBL JOB_TRN_PIA ");
                strSql.Append("WHERE");
                strSql.Append(" JOB_TRN_PIA.JOB_CARD_FK = JOB_EXP.JOB_CARD_PK");
                strSql.Append(" AND JOB_TRN_PIA.COST_ELEMENT_MST_FK = COST_ELE.COST_ELEMENT_MST_PK");
                strSql.Append(" AND JOB_TRN_PIA.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK ");
                strSql.Append(" AND JOB_TRN_PIA.INV_REF_PK IS NULL");
                strSql.Append(" AND JOB_TRN_PIA.VENDOR_MST_FK=" + VendorPK);
            }

            strSql.Append(strCondition);
            try
            {
                Int32 Count = 0;
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(strSql.ToString());
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

        #endregion "Fetch JobCardInvoice"

        #region "ROE Count"

        public int ROECount(Int32 CurrencyPK = 0, string InvoiceDt = "")
        {
            string strSQL = null;
            Int32 totRecords = 0;
            WorkFlow objTotRecCount = new WorkFlow();
            try
            {
                strSQL = "select count(*) from exchange_rate_trn ex, currency_type_mst_tbl cur";
                strSQL += "where cur.currency_mst_pk =" + CurrencyPK;
                strSQL += " and ex.currency_mst_fk=cur.currency_mst_pk";
                strSQL += " and ex.exch_rate_type_fk = 1 ";
                strSQL += "AND TO_DATE('" + InvoiceDt + "','" + dateFormat + "') BETWEEN ex.from_date AND ex.to_date";

                totRecords = Convert.ToInt32(objTotRecCount.ExecuteScaler(strSQL.ToString()));
                return totRecords;
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

        #endregion "ROE Count"

        #region "Save SupplierInvoice"

        public ArrayList SaveSupplierInvoice(DataSet M_DataSet, long Inv_Supplier_Pk, Int32 Approved, OracleCommand SelectCommand, OracleTransaction TRAN)
        {
            WorkFlow objWK = new WorkFlow();
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();

            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int nRowCnt = 0;

                for (nRowCnt = 0; nRowCnt <= M_DataSet.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    if (Convert.ToInt32(M_DataSet.Tables[0].Rows[nRowCnt]["INV_SUPPLIER_TRN_FK"]) == 0)
                    {
                        var _with1 = SelectCommand;
                        _with1.CommandType = CommandType.StoredProcedure;
                        _with1.CommandText = objWK.MyUserName + ".REM_DA_VOUCHER_PKG.REM_INV_SUPPLIER_TRN_TBL_INS";

                        SelectCommand.Parameters.Clear();
                        _with1.Parameters.Add("REM_INV_SUPPLIER_FK_IN", Inv_Supplier_Pk).Direction = ParameterDirection.Input;

                        _with1.Parameters.Add("JC_COST_DTL_FK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["JOB_TRN_SEA_EXP_PIA_PK"]).Direction = ParameterDirection.Input;
                        _with1.Parameters["JC_COST_DTL_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with1.Parameters.Add("JOBCARD_REF_NO_IN", M_DataSet.Tables[0].Rows[nRowCnt]["REF_NR"]).Direction = ParameterDirection.Input;
                        _with1.Parameters["JOBCARD_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                        _with1.Parameters.Add("COST_ELEMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                        _with1.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with1.Parameters.Add("ESTIMATED_AMT_IN", M_DataSet.Tables[0].Rows[nRowCnt]["ESTIMATED_AMT"]).Direction = ParameterDirection.Input;
                        _with1.Parameters["ESTIMATED_AMT_IN"].SourceVersion = DataRowVersion.Current;

                        _with1.Parameters.Add("ACTUAL_AMT_IN", M_DataSet.Tables[0].Rows[nRowCnt]["ACTUAL_AMT"]).Direction = ParameterDirection.Input;
                        _with1.Parameters["ACTUAL_AMT_IN"].SourceVersion = DataRowVersion.Current;

                        _with1.Parameters.Add("ELEMENT_APPROVED_IN", Approved).Direction = ParameterDirection.Input;

                        _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with1.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                        _with1.ExecuteNonQuery();

                        ///arrMessage.Add("All data saved successfully")' Commented by sivachandran To avoid the loop
                    }
                    else
                    {
                        var _with2 = SelectCommand;

                        _with2.CommandType = CommandType.StoredProcedure;
                        _with2.CommandText = objWK.MyUserName + ".REM_DA_VOUCHER_PKG.REM_INV_SUPPLIER_TRN_TBL_UPD";
                        SelectCommand.Parameters.Clear();

                        _with2.Parameters.Add("REM_INV_SUPPLIER_TRN_PK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["INV_SUPPLIER_TRN_FK"]).Direction = ParameterDirection.Input;
                        _with2.Parameters["REM_INV_SUPPLIER_TRN_PK_IN"].SourceVersion = DataRowVersion.Current;

                        _with2.Parameters.Add("REM_INV_SUPPLIER_FK_IN", Inv_Supplier_Pk).Direction = ParameterDirection.Input;

                        _with2.Parameters.Add("JC_COST_DTL_FK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["JOB_TRN_SEA_EXP_PIA_PK"]).Direction = ParameterDirection.Input;
                        _with2.Parameters["JC_COST_DTL_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with2.Parameters.Add("JOBCARD_REF_NO_IN", M_DataSet.Tables[0].Rows[nRowCnt]["REF_NR"]).Direction = ParameterDirection.Input;
                        _with2.Parameters["JOBCARD_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                        _with2.Parameters.Add("COST_ELEMENT_MST_FK_IN", M_DataSet.Tables[0].Rows[nRowCnt]["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                        _with2.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with2.Parameters.Add("ESTIMATED_AMT_IN", M_DataSet.Tables[0].Rows[nRowCnt]["ESTIMATED_AMT"]).Direction = ParameterDirection.Input;
                        _with2.Parameters["ESTIMATED_AMT_IN"].SourceVersion = DataRowVersion.Current;

                        _with2.Parameters.Add("ACTUAL_AMT_IN", M_DataSet.Tables[0].Rows[nRowCnt]["ACTUAL_AMT"]).Direction = ParameterDirection.Input;
                        _with2.Parameters["ACTUAL_AMT_IN"].SourceVersion = DataRowVersion.Current;

                        _with2.Parameters.Add("ELEMENT_APPROVED_IN", Approved).Direction = ParameterDirection.Input;

                        _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with2.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                        _with2.ExecuteNonQuery();

                        ///arrMessage.Add("All data saved successfully") 'Commented by sivachandran To avoid the loop
                    }
                }
                arrMessage.Add("All data saved successfully");
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

        #endregion "Save SupplierInvoice"

        #region "Save Supplier Invoice Header"

        public ArrayList SaveHeader(string InvoiceNo, string InvoiceDate, string SupplierInvNo, string SupplierInvDate, string SupplierDueDate, Int32 Internal_Ref, Int32 VendorPK, Int32 CurrencyPK, string SupplierInvAmt, int Status,
        Int32 Version_No, int Inv_Supplier_Pk, string Remarks = "", Int32 jobpk = 0, Int32 ApprovedBy = 3, string ApprovedDate = "")
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction insertTrans = null;
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            int intPkValue = 0;
            bool isUpdate = false;
            int Afct = 0;
            int SupplierPkValue = 0;
            //adding by thiyagarajan on 27/1/09:TrackNTrace Task:VEK Req.
            string Vou_Status = null;
            Int32 doctype = 8;

            objWK.OpenConnection();
            insertTrans = objWK.MyConnection.BeginTransaction();
            try
            {
                //On Insert New Record

                if (Inv_Supplier_Pk == 0)
                {
                    isUpdate = false;
                    var _with3 = objWK.MyCommand;
                    _with3.Transaction = insertTrans;
                    _with3.Connection = objWK.MyConnection;
                    _with3.CommandType = CommandType.StoredProcedure;
                    _with3.CommandText = objWK.MyUserName + ".REM_DA_VOUCHER_PKG.REM_INV_SUPPLIER_TBL_INS";
                    _with3.Parameters.Clear();
                    var _with4 = _with3.Parameters;
                    _with4.Add("INVOICE_REF_NO_IN", InvoiceNo).Direction = ParameterDirection.Input;
                    _with4.Add("INVOICE_DATE_IN", Convert.ToDateTime(InvoiceDate)).Direction = ParameterDirection.Input;
                    _with4.Add("SUPPLIER_INV_NO_IN", SupplierInvNo).Direction = ParameterDirection.Input;
                    _with4.Add("SUPPLIER_INV_DT_IN", Convert.ToDateTime(SupplierInvDate)).Direction = ParameterDirection.Input;
                    _with4.Add("SUPPLIER_DUE_DT_IN", Convert.ToDateTime(SupplierDueDate)).Direction = ParameterDirection.Input;
                    //Added By Prakash Chandra on 22/5/2008
                    _with4.Add("INTERNAL_REF_IN", Internal_Ref).Direction = ParameterDirection.Input;
                    _with4.Add("VENDOR_MST_FK_IN", VendorPK).Direction = ParameterDirection.Input;
                    _with4.Add("CURRENCY_MST_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                    _with4.Add("INVOICE_AMT_IN", Convert.ToDouble(SupplierInvAmt)).Direction = ParameterDirection.Input;
                    _with4.Add("REMARKS_IN", (!string.IsNullOrEmpty(Remarks) ? Remarks : "")).Direction = ParameterDirection.Input;
                    _with4.Add("APPROVED_IN", Status).Direction = ParameterDirection.Input;
                    _with4.Add("APPROVED_BY_FK_IN", ApprovedBy).Direction = ParameterDirection.Input;
                    _with4.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with4.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    Afct = _with3.ExecuteNonQuery();
                    if (Afct > 0)
                    {
                        //ViewState["SupplierPkValue"] = Afct;
                        intPkValue = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                        //adding by thiyagarajan on 27/1/09:TrackNTrace Task:VEK Req.
                        if (jobpk > 0)
                        {
                            Vou_Status = "Disbursement Voucher Generated against " + ObjTrn_DataSet.Tables[0].Rows[0]["REF_NR"];
                            objWK.MyCommand.Connection = objWK.MyConnection;
                            objWK.MyCommand.Transaction = insertTrans;
                            objWK.MyCommand.CommandType = CommandType.StoredProcedure;
                            objWK.MyCommand.CommandText = objWK.MyUserName + ".REM_TRACK_N_TRACE_PKG.REM_TRACK_N_TRACE_INS";
                            var _with5 = objWK.MyCommand.Parameters;
                            _with5.Clear();
                            _with5.Add("REF_NO_IN", InvoiceNo).Direction = ParameterDirection.Input;
                            _with5.Add("REF_FK_IN", jobpk).Direction = ParameterDirection.Input;
                            _with5.Add("LOC_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                            _with5.Add("STATUS_IN", Vou_Status).Direction = ParameterDirection.Input;
                            _with5.Add("CREATED_BY_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                            _with5.Add("DOCTYPE_IN", doctype).Direction = ParameterDirection.Input;
                            objWK.MyCommand.ExecuteNonQuery();
                        }
                        //end
                    }
                    else
                    {
                        insertTrans.Rollback();
                    }
                }
                else
                {
                    isUpdate = true;
                    var _with6 = objWK.MyCommand;
                    _with6.Transaction = insertTrans;
                    _with6.Connection = objWK.MyConnection;
                    _with6.CommandType = CommandType.StoredProcedure;
                    _with6.CommandText = objWK.MyUserName + ".REM_DA_VOUCHER_PKG.REM_INV_SUPPLIER_TBL_UPD";
                    _with6.Parameters.Clear();
                    var _with7 = _with6.Parameters;
                    _with7.Add("REM_INV_SUPPLIER_PK_IN", Inv_Supplier_Pk).Direction = ParameterDirection.Input;
                    _with7.Add("INVOICE_REF_NO_IN", InvoiceNo).Direction = ParameterDirection.Input;
                    _with7.Add("INVOICE_DATE_IN", Convert.ToDateTime(InvoiceDate)).Direction = ParameterDirection.Input;
                    _with7.Add("SUPPLIER_INV_NO_IN", SupplierInvNo).Direction = ParameterDirection.Input;
                    _with7.Add("SUPPLIER_INV_DT_IN", Convert.ToDateTime(SupplierInvDate)).Direction = ParameterDirection.Input;
                    _with7.Add("SUPPLIER_DUE_DT_IN", Convert.ToDateTime(SupplierDueDate)).Direction = ParameterDirection.Input;
                    //Added By Prakash Chandra on 22/5/2008
                    _with7.Add("INTERNAL_REF_IN", Internal_Ref).Direction = ParameterDirection.Input;
                    _with7.Add("VENDOR_MST_FK_IN", VendorPK).Direction = ParameterDirection.Input;
                    _with7.Add("CURRENCY_MST_FK_IN", CurrencyPK).Direction = ParameterDirection.Input;
                    _with7.Add("INVOICE_AMT_IN", Convert.ToDouble(SupplierInvAmt)).Direction = ParameterDirection.Input;
                    _with7.Add("REMARKS_IN", (!string.IsNullOrEmpty(Remarks) ? Remarks : "")).Direction = ParameterDirection.Input;
                    _with7.Add("APPROVED_IN", Status).Direction = ParameterDirection.Input;
                    _with7.Add("APPROVED_BY_FK_IN", ApprovedBy).Direction = ParameterDirection.Input;
                    _with7.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with7.Add("VERSION_NO_IN", Version_No).Direction = ParameterDirection.Input;
                    _with7.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with7.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    Afct = _with6.ExecuteNonQuery();
                    if (Afct > 0)
                    {
                        intPkValue = Convert.ToInt32(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                    }
                    else
                    {
                        insertTrans.Rollback();
                    }
                }
                if (intPkValue > 0)
                {
                    arrMessage = SaveSupplierInvoice(ObjTrn_DataSet, intPkValue, Status, objWK.MyCommand, insertTrans);
                }
                if (arrMessage.Count > 0)
                {
                    insertTrans.Commit();
                    arrMessage.Add(intPkValue);
                    return arrMessage;
                }
                else
                {
                    //added by surya prasad for protocol roll back
                    if (isUpdate == false)
                    {
                        RollbackProtocolKey("REMOVAL SUPPLIER INVOICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), InvoiceNo, System.DateTime.Now);
                    }
                    insertTrans.Rollback();

                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                insertTrans.Rollback();
                if (isUpdate == false)
                {
                    RollbackProtocolKey("REMOVAL SUPPLIER INVOICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), InvoiceNo, System.DateTime.Now);
                }
                throw oraexp;
                arrMessage.Add(oraexp.Message);
            }
            catch (Exception ex)
            {
                insertTrans.Rollback();
                if (isUpdate == false)
                {
                    RollbackProtocolKey("REMOVAL SUPPLIER INVOICE", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), InvoiceNo, System.DateTime.Now);
                }
                arrMessage.Add(ex.Message);
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Save Supplier Invoice Header"

        #region "GenerateKey"

        public string GenerateKey(string strName, long nLocPK, long nEmpPK, System.DateTime dtDate, long nUserID)
        {
            try
            {
                return GenerateProtocolKey(strName, nLocPK, nEmpPK, dtDate, "", "", "", nUserID);
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

        #endregion "GenerateKey"

        #region "Fetch JobCardListing"

        public DataSet FetchJobCardListing(Int32 VendorPK, Int16 Status, string FromDt = "", string ToDt = "", Int32 TradePK = 0, int JobPK = 0, string VendorInvNr = "", string InvDt = "", string SupplierRefNr = "", Int32 CurrentPage = 0,
        Int32 TotalPage = 0, string SearchType = "", Int32 flag = 0)
        {
            StringBuilder strSql = new StringBuilder();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string BusinessProcess = null;
            string VslFlight = null;
            if (JobPK != 0)
            {
                strCondition += " AND RJOB.JOB_CARD_PK=" + JobPK;
            }

            strCondition += " AND RJOB.JOB_CARD_REF=INVTRNTBL.JOBCARD_REF_NO ";
            if ((FromDt != null) & (ToDt != null))
            {
                if (FromDt.Length > 0 & ToDt.Length > 0)
                {
                    strCondition += " AND RJOB.JOB_CARD_DATE BETWEEN TO_DATE('" + FromDt + "','" + dateFormat + "')  AND TO_DATE('" + ToDt + "','" + dateFormat + "')  ";
                }
            }
            else if ((FromDt != null))
            {
                if (FromDt.Length > 0)
                {
                    strCondition += " AND RJOB.JOB_CARD_DATE >= TO_DATE('" + FromDt + "','" + dateFormat + "') ";
                }
            }
            else if ((ToDt != null))
            {
                if (ToDt.Length > 0)
                {
                    strCondition += " AND RJOB.JOB_CARD_DATE <= TO_DATE('" + ToDt + "','" + dateFormat + "') ";
                }
            }

            if (VendorPK != 0 & SearchType == "C")
            {
                strCondition += " AND VMST.VENDOR_MST_PK =" + VendorPK;
            }
            else if (VendorPK != 0 & SearchType == "S")
            {
                strCondition += " AND VMST.VENDOR_MST_PK =" + VendorPK;
            }

            if (VendorInvNr.Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '%" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            else if (VendorInvNr.Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%')";
            }

            if ((InvDt != null))
            {
                strCondition += " AND INVTBL.INVOICE_DATE = TO_DATE('" + InvDt + "','" + dateFormat + "')";
            }
            if (SupplierRefNr.Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " AND  LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '%" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'";
            }
            else if (SupplierRefNr.Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " AND  LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'";
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2 ";
            }
            strSql.Append("select count(*) from(");
            strSql.Append("SELECT ROWNUM SrNO ,Q.*FROM ( SELECT DISTINCT ");
            strSql.Append(" INVTBL.REM_INV_SUPPLIER_PK,");
            strSql.Append("INVTBL.INVOICE_REF_NO VOUCHERNO,");
            strSql.Append("TO_DATE(INVTBL.INVOICE_DATE,dateformat) VOUCHERDATE,");
            strSql.Append("INVTBL.SUPPLIER_INV_NO SUPPLIER_REF_NR,");
            strSql.Append("VMST.VENDOR_ID VENDOR,");
            ///strSql.Append(" ROUND(SUM(INVTRNTBL.ACTUAL_AMT),2) AMOUNT,")
            strSql.Append(" INVTBL.INVOICE_AMT AMOUNT ,");
            strSql.Append("  CURR.CURRENCY_ID CUR,");
            strSql.Append(" DECODE(INVTRNTBL.ELEMENT_APPROVED,1,'Approved',2,'Reject', 0,'Pending') STATUS");
            strSql.Append(" FROM ");
            strSql.Append(" REM_INV_SUPPLIER_TBL INVTBL,");
            strSql.Append("REM_INV_SUPPLIER_TRN_TBL INVTRNTBL,");
            strSql.Append("VENDOR_MST_TBL VMST,");
            strSql.Append("CURRENCY_TYPE_MST_TBL CURR,");
            strSql.Append("USER_MST_TBL USRTBL,REM_M_JOB_CARD_MST_TBL RJOB");
            strSql.Append(" WHERE INVTBL.REM_INV_SUPPLIER_PK = INVTRNTBL.REM_INV_SUPPLIER_FK");
            strSql.Append(" AND   VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
            strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
            strSql.Append(" AND USRTBL.USER_MST_PK = INVTBL.CREATED_BY_FK");
            ///strSql.Append(" AND RJOB.JOB_CARD_REF = INVTRNTBL.JOBCARD_REF_NO")
            ///strSql.Append(" AND USRTBL.DEFAULT_LOCATION_FK = " & LoggedIn_Loc_FK)'Commented By sivachandran - Its not Location specific
            if (Status < 3)
            {
                if (SearchType == "C")
                {
                    strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
                }
                else if (SearchType == "S")
                {
                    strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
                }
            }

            strSql.Append(strCondition);
            ///strSql.Append(" GROUP BY INVTBL.INVOICE_REF_NO ,")
            ///strSql.Append("INVTBL.INVOICE_DATE,")
            ///strSql.Append(" VMST.VENDOR_ID ,")
            ///strSql.Append("INVTRNTBL.ELEMENT_APPROVED,")
            ///strSql.Append(" INVTBL.REM_INV_SUPPLIER_PK,")
            ///strSql.Append("CURR.CURRENCY_ID,")
            ///strSql.Append("INVTBL.SUPPLIER_INV_NO,RJOB.JOB_CARD_REF ")

            if (JobPK == 0 & string.IsNullOrEmpty(FromDt) & string.IsNullOrEmpty(ToDt))
            {
                strSql.Append(" UNION");

                strSql.Append(" SELECT distinct INVTBL.REM_INV_SUPPLIER_PK,");
                strSql.Append(" INVTBL.INVOICE_REF_NO VOUCHERNO,");
                strSql.Append(" TO_DATE(INVTBL.INVOICE_DATE, dateformat) VOUCHERDATE,");
                strSql.Append("INVTBL.SUPPLIER_INV_NO SUPPLIER_REF_NR,");
                strSql.Append(" VMST.VENDOR_ID VENDOR,");
                ///strSql.Append(" ROUND(SUM(INVTRNTBL.ACTUAL_AMT),2) AMOUNT,") Commented by sivachandran To avoid duplicate in parent Table while doing Data relation
                strSql.Append(" INVTBL.INVOICE_AMT AMOUNT ,");
                strSql.Append("  CURR.CURRENCY_ID CUR,");
                strSql.Append(" DECODE(INVTRNTBL.ELEMENT_APPROVED,");
                strSql.Append(" 1,'Approved', 2,'Reject',0,'Pending') STATUS");
                strSql.Append(" FROM REM_INV_SUPPLIER_TBL     INVTBL,");
                strSql.Append(" REM_INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                strSql.Append("CURRENCY_TYPE_MST_TBL CURR,");
                strSql.Append(" VENDOR_MST_TBL VMST,");
                strSql.Append(" USER_MST_TBL USRTBL");
                strSql.Append(" WHERE INVTBL.REM_INV_SUPPLIER_PK = INVTRNTBL.REM_INV_SUPPLIER_FK");
                strSql.Append(" AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                strSql.Append(" AND USRTBL.USER_MST_PK = INVTBL.CREATED_BY_FK");
                //'strSql.Append(" AND USRTBL.DEFAULT_LOCATION_FK = " & LoggedIn_Loc_FK)'Commented By sivachandran - Its not Location specific

                if (Status < 3)
                {
                    if (SearchType == "C")
                    {
                        strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
                    }
                    else if (SearchType == "S")
                    {
                        strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
                    }
                }

                if (VendorPK != 0 & SearchType == "C")
                {
                    strSql.Append(" AND VMST.VENDOR_MST_PK = " + VendorPK);
                }
                else if (VendorPK != 0 & SearchType == "S")
                {
                    strSql.Append(" AND VMST.VENDOR_MST_PK = " + VendorPK);
                }

                if (VendorInvNr.Trim().Length > 0 & SearchType == "C")
                {
                    strSql.Append(" AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '%" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'");
                }
                else if (VendorInvNr.Trim().Length > 0 & SearchType == "S")
                {
                    strSql.Append(" AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'");
                }
                if ((InvDt != null))
                {
                    strSql.Append(" AND INVTBL.INVOICE_DATE = TO_DATE('" + InvDt + "','" + dateFormat + "')");
                }

                if (SupplierRefNr.Trim().Length > 0 & SearchType == "C")
                {
                    strSql.Append(" AND LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '%" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'");
                }
                else if (SupplierRefNr.Trim().Length > 0 & SearchType == "S")
                {
                    strSql.Append(" AND LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'");
                }
                ///strSql.Append(" GROUP BY INVTBL.INVOICE_REF_NO ,")
                ///strSql.Append("INVTBL.INVOICE_DATE,")
                ///strSql.Append(" VMST.VENDOR_ID ,")
                ///strSql.Append("INVTRNTBL.ELEMENT_APPROVED,")
                ///strSql.Append(" INVTBL.REM_INV_SUPPLIER_PK,")
                ///strSql.Append("CURR.CURRENCY_ID,")
                ///strSql.Append("INVTBL.SUPPLIER_INV_NO")
            }

            strSql.Append(")Q)");

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSql.ToString()));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
            {
                CurrentPage = 1;
            }
            if (TotalRecords == 0)
            {
                CurrentPage = 0;
            }
            strSql = new StringBuilder();
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            strSql.Append("SELECT * FROM(");
            strSql.Append("SELECT ROWNUM SrNO ,Q.*FROM(SELECT * FROM ( SELECT Distinct ");
            strSql.Append(" INVTBL.REM_INV_SUPPLIER_PK,");
            strSql.Append("INVTBL.INVOICE_REF_NO VOUCHERNO,");
            strSql.Append("TO_DATE(INVTBL.INVOICE_DATE,'DD/MM/RRRR') VOUCHERDATE,");
            strSql.Append("INVTBL.SUPPLIER_INV_NO SUPPLIER_REF_NR,");
            strSql.Append("VMST.VENDOR_ID VENDOR,");
            ///strSql.Append(" ROUND(SUM(INVTRNTBL.ACTUAL_AMT),2) AMOUNT,")'Commented by sivachandran to avoid duplication while data relation
            strSql.Append(" INVTBL.INVOICE_AMT AMOUNT ,");
            strSql.Append("  CURR.CURRENCY_ID CUR,");
            strSql.Append(" DECODE(INVTRNTBL.ELEMENT_APPROVED ,1,'Approved',2,'Reject',0,'Pending') STATUS");
            strSql.Append(" FROM ");
            strSql.Append("REM_INV_SUPPLIER_TBL INVTBL,");
            strSql.Append("REM_INV_SUPPLIER_TRN_TBL INVTRNTBL,");
            strSql.Append("VENDOR_MST_TBL VMST,");
            strSql.Append("CURRENCY_TYPE_MST_TBL CURR,");
            strSql.Append("USER_MST_TBL USRTBL,REM_M_JOB_CARD_MST_TBL RJOB");
            strSql.Append(" WHERE INVTBL.REM_INV_SUPPLIER_PK = INVTRNTBL.REM_INV_SUPPLIER_FK");
            strSql.Append(" AND   VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
            ///strSql.Append(" AND RJOB.JOB_CARD_REF=INVTRNTBL.JOBCARD_REF_NO")
            strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");

            if (Status < 3)
            {
                if (SearchType == "C")
                {
                    strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
                }
                else if (SearchType == "S")
                {
                    strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
                }
            }

            strSql.Append(" AND USRTBL.USER_MST_PK = INVTBL.CREATED_BY_FK");
            ///strSql.Append(" AND USRTBL.DEFAULT_LOCATION_FK = " & LoggedIn_Loc_FK)''Commented By sivachandran - Its not Location specific
            strSql.Append(strCondition);
            ///strSql.Append(" GROUP BY INVTBL.INVOICE_REF_NO ,")
            ///strSql.Append("INVTBL.INVOICE_DATE,")
            ///strSql.Append(" VMST.VENDOR_ID ,")
            ///strSql.Append("INVTRNTBL.ELEMENT_APPROVED,")
            ///strSql.Append(" INVTBL.REM_INV_SUPPLIER_PK,")
            ///strSql.Append("CURR.CURRENCY_ID,")
            ///strSql.Append("INVTBL.SUPPLIER_INV_NO,RJOB.JOB_CARD_REF")

            if (JobPK == 0 & string.IsNullOrEmpty(FromDt) & string.IsNullOrEmpty(ToDt))
            {
                strSql.Append(" UNION");
                strSql.Append(" SELECT distinct INVTBL.REM_INV_SUPPLIER_PK,");
                strSql.Append(" INVTBL.INVOICE_REF_NO VOUCHERNO,");
                strSql.Append(" TO_DATE(INVTBL.INVOICE_DATE, 'DD/MM/RRRR') VOUCHERDATE,");
                strSql.Append("INVTBL.SUPPLIER_INV_NO SUPPLIER_REF_NR,");
                strSql.Append(" VMST.VENDOR_ID VENDOR,");
                ///strSql.Append(" ROUND(SUM(INVTRNTBL.ACTUAL_AMT),2) AMOUNT,")
                strSql.Append(" INVTBL.INVOICE_AMT AMOUNT ,");
                strSql.Append("  CURR.CURRENCY_ID CUR,");
                strSql.Append(" DECODE(INVTRNTBL.ELEMENT_APPROVED,");
                strSql.Append(" 1,'Approved', 2,'Reject',0,'Pending') STATUS");
                strSql.Append(" FROM REM_INV_SUPPLIER_TBL     INVTBL,");
                strSql.Append(" REM_INV_SUPPLIER_TRN_TBL INVTRNTBL,");
                strSql.Append(" VENDOR_MST_TBL VMST,");
                strSql.Append("CURRENCY_TYPE_MST_TBL CURR,");
                strSql.Append("USER_MST_TBL USRTBL");
                strSql.Append(" WHERE INVTBL.REM_INV_SUPPLIER_PK = INVTRNTBL.REM_INV_SUPPLIER_FK");
                strSql.Append(" AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
                strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
                strSql.Append(" AND USRTBL.USER_MST_PK = INVTBL.CREATED_BY_FK");
                ///strSql.Append(" AND USRTBL.DEFAULT_LOCATION_FK = " & LoggedIn_Loc_FK)'Commented By sivachandran - Its not Location specific
                if (BlankGrid == 0)
                {
                    strSql.Append(" AND 1=2 ");
                }
                if (Status < 3)
                {
                    if (SearchType == "C")
                    {
                        strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
                    }
                    else if (SearchType == "S")
                    {
                        strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
                    }
                }

                if (VendorPK != 0 & SearchType == "C")
                {
                    strSql.Append(" AND VMST.VENDOR_MST_PK = " + VendorPK);
                }
                else if (VendorPK != 0 & SearchType == "S")
                {
                    strSql.Append(" AND VMST.VENDOR_MST_PK = " + VendorPK);
                }

                if (VendorInvNr.Trim().Length > 0 & SearchType == "C")
                {
                    strSql.Append(" AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '%" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'");
                }
                else if (VendorInvNr.Trim().Length > 0 & SearchType == "S")
                {
                    strSql.Append(" AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'");
                }

                if ((InvDt != null))
                {
                    strSql.Append(" AND INVTBL.INVOICE_DATE = TO_DATE('" + InvDt + "','" + dateFormat + "')");
                }

                if (SupplierRefNr.Trim().Length > 0 & SearchType == "C")
                {
                    strSql.Append(" AND LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '%" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'");
                }
                else if (SupplierRefNr.Trim().Length > 0 & SearchType == "S")
                {
                    strSql.Append(" AND LOWER(INVTBL.SUPPLIER_INV_NO) LIKE '" + SupplierRefNr.Trim().ToLower().Replace("'", "''") + "%'");
                }

                ///strSql.Append(" GROUP BY INVTBL.INVOICE_REF_NO ,")
                ///strSql.Append("INVTBL.INVOICE_DATE,")
                ///strSql.Append(" VMST.VENDOR_ID ,")
                ///strSql.Append("INVTRNTBL.ELEMENT_APPROVED,")
                ///strSql.Append(" INVTBL.REM_INV_SUPPLIER_PK,")
                ///strSql.Append("CURR.CURRENCY_ID,")
                ///strSql.Append("INVTBL.SUPPLIER_INV_NO")
            }

            strSql.Append(")ORDER BY  VOUCHERDATE  DESC , VOUCHERNO  DESC)q)");
            strSql.Append("WHERE SrNO  Between " + start + " and " + last);
            string sql = null;
            sql = strSql.ToString();

            DataSet DS = null;
            try
            {
                DS = objWF.GetDataSet(sql);
                DataRelation CONTRel = null;

                DS.Tables.Add(Fetchchildlist(AllMasterPKs(DS), VendorPK, Status, 1, 1, FromDt, ToDt, JobPK, "", VendorInvNr,
                InvDt, SearchType));

                CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["REM_INV_SUPPLIER_PK"], DS.Tables[1].Columns["REM_INV_SUPPLIER_PK"], true);
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

        #endregion "Fetch JobCardListing"

        #region " All Master Supplier PKs "

        private string AllMasterPKs(DataSet ds)
        {
            try
            {
                Int16 RowCnt = default(Int16);
                Int16 ln = default(Int16);
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                strBuilder.Append("-1,");
                for (RowCnt = 0; RowCnt <= ds.Tables[0].Rows.Count - 1; RowCnt++)
                {
                    strBuilder.Append(Convert.ToString(ds.Tables[0].Rows[RowCnt]["REM_INV_SUPPLIER_PK"]).Trim() + ",");
                }
                strBuilder.Remove(strBuilder.Length - 1, 1);
                return strBuilder.ToString();
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

        #endregion " All Master Supplier PKs "

        #region "Child Table"

        private DataTable Fetchchildlist(string SUPPLIERPKs = "", Int32 VendorPK = 0, Int16 Status = 0, Int16 Business_Type = 0, Int16 Process_Type = 0, string FromDt = "", string ToDt = "", int JobPK = 0, string Vsl = "", string VendorInvNr = "",
        string InvDt = "", string SearchType = "")
        {
            StringBuilder strSql = new StringBuilder();
            string strCondition = null;
            DataTable dt = null;
            WorkFlow objWF = new WorkFlow();
            int RowCnt = 0;
            int Rno = 0;
            int pk = 0;
            string BusinessProcess = null;
            string VslFlight = null;

            ///If Business_Type = 2 Then
            ///    BusinessProcess = "SEA"
            ///    VslFlight = "VESSEL_NAME"
            ///Else
            ///    BusinessProcess = "AIR"
            ///    VslFlight = "FLIGHT_NO"
            ///End If
            ///If Process_Type = 1 Then
            ///    BusinessProcess &= "_EXP"
            ///Else
            ///    BusinessProcess &= "_IMP"
            ///End If
            if (JobPK != 0)
            {
                if ((FromDt != null) & (ToDt != null))
                {
                    strCondition += " AND RJOB.JOB_CARD_DATE BETWEEN TO_DATE('" + FromDt + "','" + dateFormat + "')  AND TO_DATE('" + ToDt + "','" + dateFormat + "')  ";
                }
                else if ((FromDt != null))
                {
                    strCondition += " AND RJOB.JOB_CARD_DATE >= TO_DATE('" + FromDt + "','" + dateFormat + "') ";
                }
                else if ((ToDt != null))
                {
                    strCondition += " AND RJOB.JOB_CARD_DATE <= TO_DATE('" + ToDt + "','" + dateFormat + "') ";
                }
            }
            if (VendorPK != 0 & SearchType == "C")
            {
                strCondition += " AND VMST.VENDOR_MST_PK =" + VendorPK;
            }
            else if (VendorPK != 0 & SearchType == "S")
            {
                strCondition += " AND VMST.VENDOR_MST_PK =" + VendorPK;
            }

            if (VendorInvNr.Trim().Length > 0 & SearchType == "C")
            {
                strCondition += " AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '%" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            else if (VendorInvNr.Trim().Length > 0 & SearchType == "S")
            {
                strCondition += " AND UPPER(INVTBL.INVOICE_REF_NO) LIKE '" + VendorInvNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }

            if (JobPK != 0)
            {
                strCondition += " AND RJOB.JOB_CARD_PK=" + JobPK;
            }

            if ((InvDt != null))
            {
                strCondition += " AND INVTBL.INVOICE_DATE = TO_DATE('" + InvDt + "','" + dateFormat + "')";
            }
            strSql.Append("select * from(");
            strSql.Append("SELECT ROWNUM SrNO ,Q.*FROM ( SELECT");
            strSql.Append(" INVTBL.REM_INV_SUPPLIER_PK,");
            ///strSql.Append("JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK,")
            ///strSql.Append("JOB_EXP.JOBCARD_REF_NO JOBNO,")
            strSql.Append(" RJOB.JOB_CARD_PK,");
            strSql.Append(" RJOB.JOB_CARD_REF JOBNO,");

            ///If Business_Type = 2 Then
            ///    strSql.Append("JOB_EXP.VESSEL_NAME ||" & "'-'" & " || JOB_EXP.VOYAGE VSL_FLIGHT,")
            ///Else
            ///    strSql.Append("JOB_EXP.FLIGHT_NO VSL_FLIGHT,")
            ///End If
            strSql.Append(" SUM(INVTRNTBL.ACTUAL_AMT) AMOUNT,");
            strSql.Append("  CURR.CURRENCY_ID CUR");
            strSql.Append(" FROM ");
            strSql.Append("REM_INV_SUPPLIER_TBL INVTBL,");
            strSql.Append("REM_INV_SUPPLIER_TRN_TBL INVTRNTBL,");
            strSql.Append("VENDOR_MST_TBL VMST,");
            /// strSql.Append("JOB_CARD_" & BusinessProcess & "_TBL  JOB_EXP,")

            /// strSql.Append("JOB_TRN_" & BusinessProcess & "_PIA   JOB_TRN_PIA,")
            strSql.Append("REM_M_JOB_CARD_MST_TBL RJOB,");
            strSql.Append("CURRENCY_TYPE_MST_TBL CURR");

            strSql.Append(" WHERE INVTBL.REM_INV_SUPPLIER_PK = INVTRNTBL.REM_INV_SUPPLIER_FK");
            strSql.Append(" AND   VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
            ///strSql.Append(" AND   INVTRNTBL.JC_COST_DTL_FK= JOB_TRN_PIA.JOB_TRN_" & BusinessProcess & "_PIA_PK")
            ///strSql.Append(" AND   JOB_TRN_PIA.JOB_CARD_" & BusinessProcess & "_FK=JOB_EXP.JOB_CARD_" & BusinessProcess & "_PK")
            strSql.Append(" AND RJOB.JOB_CARD_REF=INVTRNTBL.JOBCARD_REF_NO ");
            strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
            if (Status < 3)
            {
                strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
            }
            if (SUPPLIERPKs.Trim().Length > 0 & SearchType == "C")
            {
                strSql.Append(" AND INVTBL.REM_INV_SUPPLIER_PK  in (" + SUPPLIERPKs + ") ");
            }
            else if (SUPPLIERPKs.Trim().Length > 0 & SearchType == "S")
            {
                strSql.Append(" AND INVTBL.REM_INV_SUPPLIER_PK  in (" + SUPPLIERPKs + ") ");
            }

            strSql.Append(strCondition);
            strSql.Append(" GROUP BY RJOB.JOB_CARD_PK, ");
            strSql.Append(" RJOB.JOB_CARD_REF ,");
            ///If Business_Type = 2 Then
            ///    strSql.Append("JOB_EXP.VESSEL_NAME ||" & "'-'" & " || JOB_EXP.VOYAGE,")
            ///Else
            ///    strSql.Append("JOB_EXP.FLIGHT_NO,")
            ///End If
            strSql.Append(" INVTBL.REM_INV_SUPPLIER_PK,");
            strSql.Append("CURR.CURRENCY_ID");

            strSql.Append(" UNION");

            strSql.Append(" SELECT distinct INVTBL.REM_INV_SUPPLIER_PK,");
            strSql.Append(" 0 JOB_CARD_PK,");
            strSql.Append(" '' JOBNO,");
            ///strSql.Append(" '' VSL_FLIGHT,")
            strSql.Append(" SUM(INVTRNTBL.ACTUAL_AMT) AMOUNT,");
            strSql.Append("  CURR.CURRENCY_ID CUR");
            strSql.Append(" FROM REM_INV_SUPPLIER_TBL     INVTBL,");
            strSql.Append(" REM_INV_SUPPLIER_TRN_TBL INVTRNTBL,");
            strSql.Append(" VENDOR_MST_TBL VMST,");
            strSql.Append(" CURRENCY_TYPE_MST_TBL CURR");
            strSql.Append(" WHERE INVTBL.REM_INV_SUPPLIER_PK = INVTRNTBL.REM_INV_SUPPLIER_FK");
            strSql.Append(" AND VMST.VENDOR_MST_PK = INVTBL.VENDOR_MST_FK");
            strSql.Append(" AND CURR.CURRENCY_MST_PK = INVTBL.CURRENCY_MST_FK");
            if (Status < 3)
            {
                strSql.Append(" AND INVTRNTBL.ELEMENT_APPROVED = " + Status);
            }
            strSql.Append(" and invtrntbl.JC_COST_DTL_FK=0");
            if (SUPPLIERPKs.Trim().Length > 0 | SUPPLIERPKs != "-1" & SearchType == "C")
            {
                strSql.Append(" AND INVTBL.REM_INV_SUPPLIER_PK  in (" + SUPPLIERPKs + ") ");
            }
            else if (SUPPLIERPKs.Trim().Length > 0 | SUPPLIERPKs != "-1" & SearchType == "S")
            {
                strSql.Append(" AND INVTBL.REM_INV_SUPPLIER_PK  in (" + SUPPLIERPKs + ") ");
            }

            strSql.Append(" GROUP BY INVTBL.REM_INV_SUPPLIER_PK,");
            strSql.Append(" CURR.CURRENCY_ID");
            strSql.Append(" ORDER BY REM_INV_SUPPLIER_PK DESC, JOBNO DESC)Q)");
            string sql = null;
            sql = strSql.ToString();
            try
            {
                pk = -1;
                dt = objWF.GetDataTable(sql);
                for (RowCnt = 0; RowCnt <= dt.Rows.Count - 1; RowCnt++)
                {
                    if (Convert.ToInt32(dt.Rows[RowCnt]["REM_INV_SUPPLIER_PK"]) != pk)
                    {
                        pk = Convert.ToInt32(dt.Rows[RowCnt]["REM_INV_SUPPLIER_PK"]);
                        Rno = 0;
                    }
                    Rno += 1;
                    dt.Rows[RowCnt]["SrNO"] = Rno;
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

        #region "Fetch JobCardListing"

        public DataSet FetchExistingInvoice(string InvPK)
        {
            StringBuilder strSql = new StringBuilder();

            strSql.Append("SELECT INVS.REM_INV_SUPPLIER_PK,");
            strSql.Append("INVS.INVOICE_REF_NO,");
            strSql.Append("INVS.INVOICE_DATE,");
            strSql.Append("INVS.SUPPLIER_INV_NO,");
            strSql.Append("INVS.SUPPLIER_INV_DT,");
            strSql.Append("INVS.SUPPLIER_DUE_DT,");
            strSql.Append("INVS.INVOICE_AMT,");
            strSql.Append("INVS.REMARKS,");
            strSql.Append("VMST.VENDOR_MST_PK,");
            strSql.Append("VMST.VENDOR_ID,");
            strSql.Append("VMST.VENDOR_NAME,");
            strSql.Append("CURR.CURRENCY_MST_PK,");
            strSql.Append("CURR.CURRENCY_ID,");
            strSql.Append("CURR.CURRENCY_NAME,");
            strSql.Append("INVS.INTERNAL_REF,INVS.VERSION_NO,INVS.APPROVED");
            strSql.Append(" FROM REM_INV_SUPPLIER_TBL INVS,");
            strSql.Append(" REM_INV_SUPPLIER_TRN_TBL INVTRN,");
            strSql.Append(" VENDOR_MST_TBL VMST,");
            strSql.Append(" CURRENCY_TYPE_MST_TBL CURR");
            strSql.Append(" WHERE INVS.REM_INV_SUPPLIER_PK =" + InvPK);
            strSql.Append(" AND   INVS.REM_INV_SUPPLIER_PK=INVTRN.REM_INV_SUPPLIER_FK");
            strSql.Append(" AND   INVS.VENDOR_MST_FK =VMST.VENDOR_MST_PK");
            strSql.Append(" AND   CURR.CURRENCY_MST_PK=INVS.CURRENCY_MST_FK");

            try
            {
                WorkFlow objWF = new WorkFlow();
                return objWF.GetDataSet(strSql.ToString());
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

        #endregion "Fetch JobCardListing"

        #region "GetCorpCurrency"

        public DataSet GetCorpCurrency()
        {
            string strSQL = null;
            strSQL = "SELECT CMT.CURRENCY_MST_FK,CUMT.CURRENCY_ID,CUMT.CURRENCY_NAME FROM CORPORATE_MST_TBL CMT,CURRENCY_TYPE_MST_TBL CUMT";
            strSQL += "WHERE CMT.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
            try
            {
                DataSet DS = null;
                DS = (new WorkFlow()).GetDataSet(strSQL);
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

        #endregion "GetCorpCurrency"

        #region "Enhance-Search Removals for job card "

        public string FetchJobcardNumber(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand cmd = new OracleCommand();
            string strReturn = null;
            string[] arr = null;
            string strSearchIn = "";
            short intBizType = 0;
            short intProcess = 0;
            //Dim intParty As Integer
            string strParty = "";
            long intLocPk = 0;
            string strReq = null;
            string FROM = null;
            arr = strCond.Split(Convert.ToChar("~"));
            strReq = arr[0];
            strSearchIn = arr[1];
            if (arr.Length > 2)
            {
                if (!string.IsNullOrEmpty(Convert.ToString(arr[2])))
                {
                    //intParty = CInt(arr(2))
                    strParty = arr[2];
                }
            }
            if (arr.Length > 3)
                intLocPk = Convert.ToInt64(arr[3]);
            if (arr.Length > 4)
                FROM = arr[4];
            try
            {
                objWF.OpenConnection();
                cmd.Connection = objWF.MyConnection;
                cmd.CommandType = CommandType.StoredProcedure;
                if (FROM == "INVOICE")
                {
                    cmd.CommandText = objWF.MyUserName + ".EN_JOBCARD_PKG.GET_REMOVAL_JOB_INV";
                }
                else
                {
                    cmd.CommandText = objWF.MyUserName + ".EN_JOBCARD_PKG.GET_REMOVAL_JOB_COLL";
                }
                //cmd.CommandText = objWF.MyUserName & ".EN_JOBCARD_PKG.GET_REMOVAL_JOB_COLL"
                var _with8 = cmd.Parameters;
                _with8.Add("SEARCH_IN", getDefault(strSearchIn, "")).Direction = ParameterDirection.Input;
                _with8.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                //.Add("PARTY_IN", intParty).Direction = ParameterDirection.Input
                _with8.Add("PARTY_IN", getDefault(strParty, "")).Direction = ParameterDirection.Input;
                _with8.Add("LOC_IN", intLocPk).Direction = ParameterDirection.Input;
                _with8.Add("RETURN_VALUE", OracleDbType.NVarchar2, 2000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                cmd.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                cmd.ExecuteNonQuery();
                strReturn = Convert.ToString(cmd.Parameters["RETURN_VALUE"].Value).Trim();
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
                cmd.Connection.Close();
            }
        }

        #endregion "Enhance-Search Removals for job card "
    }
}