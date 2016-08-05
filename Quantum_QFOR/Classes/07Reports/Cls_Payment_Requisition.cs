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

using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class Cls_Payment_Requisition : CommonFeatures
    {
        string Strsql;

        WorkFlow ObjWF = new WorkFlow();
        #region " FetchPaymentGridData"
        public string FetchSeaExpPaymentGridData(string JobNo = "", string Party = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, string invNr = "", double PageTotal = 0, double GrandTotal = 0)
        {

            string objStr = "";
            try
            {
                objStr = FetchAllPaymentGridData(JobNo, Party, CurrentPage, TotalPage, flag, invNr, PageTotal, GrandTotal, 2, 1);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            
            return objStr;
        }
        public string FetchSeaImpPaymentGridData(string JobNo = "", string Party = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, Int32 LocFk = 0, string invNr = "", double PageTotal = 0, double GrandTotal = 0)
        {

            string objStr = "";
            try
            {
                objStr = FetchAllPaymentGridData(JobNo, Party, CurrentPage, TotalPage, flag, invNr, PageTotal, GrandTotal, 2, 2);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
           
            return objStr;
        }
        public string FetchAirExpPaymentGridData(string JobNo = "", string Party = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, string invNr = "", double PageTotal = 0, double GrandTotal = 0)
        {

            string objStr = "";
            try
            {
                objStr = FetchAllPaymentGridData(JobNo, Party, CurrentPage, TotalPage, flag, invNr, PageTotal, GrandTotal, 1, 1);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            return JsonConvert.SerializeObject(objStr, Formatting.Indented);
        }
        public string FetchAirImpPaymentGridData(string JobNo = "", string Party = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, Int32 LocFk = 0, string invNr = "", double PageTotal = 0, double GrandTotal = 0)
        {


            string objStr = "";
            try
            {
                objStr = FetchAllPaymentGridData(JobNo, Party, CurrentPage, TotalPage, flag, invNr, PageTotal, GrandTotal, 1, 2);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }

            return JsonConvert.SerializeObject(objStr, Formatting.Indented);
        }
        public string FetchBothExpPaymentGridData(string JobNo = "", string Party = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, string invNr = "", double PageTotal = 0, double GrandTotal = 0)
        {

            string objStr = "";
            try
            {
                objStr = FetchAllPaymentGridData(JobNo, Party, CurrentPage, TotalPage, flag, invNr, PageTotal, GrandTotal, 3, 1);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }

            return JsonConvert.SerializeObject(objStr, Formatting.Indented);
        }
        public string FetchBothIMPPaymentGridData(string JobNo = "", string Party = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, Int32 LocFk = 0, string invNr = "", double PageTotal = 0, double GrandTotal = 0)
        {

            string objStr = "";
            try
            {
                objStr = FetchAllPaymentGridData(JobNo, Party, CurrentPage, TotalPage, flag, invNr, PageTotal, GrandTotal, 3, 2);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }

            return JsonConvert.SerializeObject(objStr, Formatting.Indented);
        }

        //--------------------------------------------------------------------------
        public string FetchAllPaymentGridData(string JobNo = "", string Party = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, string invNr = "", double PageTotal = 0, double GrandTotal = 0, short BizType = 3, short ProcessType = 3)
        {

            //BizType-3:Both,2:Sea,1:Air
            //ProcessType-3-Both,2:Imp,1-Exp
            string strCondition = null;
            string strCount = null;
            string strMain = null;
            string strTotal = null;
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            int BaseCurrFk = 1418;

            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }

            if (!(string.IsNullOrEmpty(JobNo)))
            {
                strCondition = strCondition + " AND JSE.JOBCARD_REF_NO = '" + JobNo + "'" ;
            }
            if (Party.Trim().Length > 0)
            {
                strCondition = strCondition + " AND VMT.VENDOR_NAME = '" + Party + "'" ;
            }
            if (!(string.IsNullOrEmpty(invNr) | invNr == null))
            {
                strCondition = strCondition + " AND IST.INVOICE_REF_NO = '" + invNr + "'" ;
            }
            if (BizType != 3)
            {
                strCondition = strCondition + " AND JSE.BUSINESS_TYPE = " + BizType ;
            }
            if (ProcessType != 3)
            {
                strCondition = strCondition + " AND JSE.PROCESS_TYPE = " + ProcessType ;
            }
            Strsql = "SELECT DISTINCT 0 JOBPPK,";
            //Strsql &= vbCrLf & " JSE.JOB_CARD_TRN_PK JOBPK,"
            Strsql += "(SELECT ROWTOCOL(' SELECT DISTINCT JCT.JOB_CARD_TRN_PK FROM JOB_CARD_TRN JCT, INV_SUPPLIER_TBL SUP, JOB_TRN_PIA PIA WHERE";
            Strsql += "SUP.INV_SUPPLIER_PK=PIA.INV_SUPPLIER_FK";
            Strsql += "AND SUP.INVOICE_REF_NO =''' || IST.INVOICE_REF_NO || '''";
            Strsql += "AND PIA.JOB_CARD_TRN_FK=JCT.JOB_CARD_TRN_PK(+)') FROM DUAL) AS JOBPK,";

            //Strsql &= vbCrLf & " JSE.JOBCARD_REF_NO JOBNUMBER,"
            Strsql += "(SELECT ROWTOCOL(' SELECT DISTINCT JCT.JOBCARD_REF_NO FROM JOB_CARD_TRN JCT,INV_SUPPLIER_TBL SUP, JOB_TRN_PIA PIA WHERE";
            Strsql += "SUP.INV_SUPPLIER_PK=PIA.INV_SUPPLIER_FK";
            Strsql += "AND SUP.INVOICE_REF_NO =''' || IST.INVOICE_REF_NO || '''";
            Strsql += "AND PIA.JOB_CARD_TRN_FK=JCT.JOB_CARD_TRN_PK(+)') FROM DUAL) AS JOBNUMBER,";

            Strsql += " DECODE(JSE.BUSINESS_TYPE,2,'SEA',1,'AIR') BIZTYPE,";
            Strsql += " VMT.VENDOR_NAME   VENDORNAME,";
            Strsql += " IST.INVOICE_REF_NO INVOICENO,";
            Strsql += " TO_DATE(IST.INVOICE_DATE,DATEFORMAT) INVOICEDATE,";
            Strsql += " CMST.CURRENCY_ID CURRENCY,";
            Strsql += " IST.INVOICE_AMT INVOICE_AMT,";
            Strsql += " PAYMENTS_TBL_PKG.CAL_PAID_AMOUNT(IST.INV_SUPPLIER_PK) AS PAID_AMT,";
            Strsql += " (IST.INVOICE_AMT-PAYMENTS_TBL_PKG.CAL_PAID_AMOUNT(IST.INV_SUPPLIER_PK)) AS BAL_AMT,";
            Strsql += " GET_EX_RATE(CMST.CURRENCY_MST_PK," + BaseCurrFk + ", IST.INVOICE_DATE) ROE,";
            Strsql += " ((IST.INVOICE_AMT-PAYMENTS_TBL_PKG.CAL_PAID_AMOUNT(IST.INV_SUPPLIER_PK)) * GET_EX_RATE(CMST.CURRENCY_MST_PK," + BaseCurrFk + ", IST.INVOICE_DATE)) FNL_AMT,";
            Strsql += " IST.INVOICE_DATE";
            Strsql += " FROM JOB_CARD_TRN JSE,";
            Strsql += " INV_SUPPLIER_TBL IST,";
            Strsql += " VENDOR_MST_TBL VMT,";
            Strsql += " CURRENCY_TYPE_MST_TBL CMST,";
            Strsql += " JOB_TRN_PIA JSEP,";
            Strsql += " USER_MST_TBL UMT";
            Strsql += " WHERE IST.INV_SUPPLIER_PK = JSEP.INV_SUPPLIER_FK";
            Strsql += " AND JSEP.JOB_CARD_TRN_FK=JSE.JOB_CARD_TRN_PK(+)";
            Strsql += " AND IST.VENDOR_MST_FK=VMT.VENDOR_MST_PK(+)";
            Strsql += " AND IST.CURRENCY_MST_FK=CMST.CURRENCY_MST_PK";
            Strsql += strCondition;
            Strsql += " AND (IST.INVOICE_AMT-PAYMENTS_TBL_PKG.CAL_PAID_AMOUNT(IST.INV_SUPPLIER_PK))>0";
            Strsql += " AND IST.APPROVED=1";
            Strsql += " AND UMT.USER_MST_PK=IST.CREATED_BY_FK";
            Strsql += " AND UMT.DEFAULT_LOCATION_FK IN";
            Strsql += " (SELECT LMT.LOCATION_MST_PK FROM LOCATION_MST_TBL LMT START WITH LMT.LOCATION_MST_PK = " + 1841 + " CONNECT BY PRIOR LMT.LOCATION_MST_PK = LMT.REPORTING_TO_FK)";
            Strsql += " GROUP BY IST.INV_SUPPLIER_PK,JSE.Job_Card_Trn_Pk,JSE.JOBCARD_REF_NO,VMT.VENDOR_NAME,";
            Strsql += " IST.INVOICE_REF_NO, IST.INVOICE_DATE, CMST.CURRENCY_ID, IST.INVOICE_AMT, CMST.CURRENCY_MST_PK,JSE.BUSINESS_TYPE ";

            strCount = "select count(*) from (";
            strCount = strCount + Strsql + ")";

            TotalRecords = Convert.ToInt32(ObjWF.ExecuteScaler(strCount));
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
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            strMain = "SELECT * FROM (";
            strMain += " SELECT ROWNUM AS SLNO,Q.JOBPPK,Q.JOBPK,Q.JOBNUMBER,Q.BIZTYPE,Q.VENDORNAME,Q.INVOICENO,Q.INVOICEDATE,Q.CURRENCY,Q.INVOICE_AMT,Q.PAID_AMT,Q.BAL_AMT,Q.ROE,Q.FNL_AMT FROM (";
            strMain += Strsql;
            //strMain &= " ORDER BY IST.INVOICE_DATE DESC,JSE.JOBCARD_REF_NO DESC)q) WHERE SLNO  Between " & start & " and " & last
            strMain += " ORDER BY IST.INVOICE_DATE DESC)q) WHERE SLNO  Between " + start + " and " + last;

            if (TotalRecords > 0)
            {
                strTotal = "select sum(FNL_AMT) from (";
                strTotal += Strsql + ")";
                GrandTotal = Convert.ToDouble(ObjWF.ExecuteScaler(strTotal));

                strTotal = "SELECT SUM(FNL_AMT) FROM (";
                strTotal += " SELECT ROWNUM AS SLNO,Q.JOBPPK,Q.JOBPK,Q.JOBNUMBER,Q.BIZTYPE,Q.VENDORNAME,Q.INVOICENO,Q.INVOICEDATE,Q.CURRENCY,Q.INVOICE_AMT,Q.PAID_AMT,Q.BAL_AMT,Q.ROE,Q.FNL_AMT FROM (";
                strTotal += Strsql;
                //strTotal &= " ORDER BY IST.INVOICE_DATE DESC,JSE.JOBCARD_REF_NO DESC)q) WHERE SLNO  Between " & start & " and " & last
                strTotal += " ORDER BY IST.INVOICE_DATE DESC)q) WHERE SLNO  Between " + start + " and " + last;
                PageTotal = Convert.ToDouble(ObjWF.ExecuteScaler(strTotal));

            }
            else
            {
                GrandTotal = 0;
                PageTotal = 0;
            }
            try
            {
                DataSet DS = ObjWF.GetDataSet(strMain);
                return JsonConvert.SerializeObject(DS, Newtonsoft.Json.Formatting.Indented);
            }
            catch (OracleException sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Report data"
        public DataSet FetchAllPaymentReport(string InvRefNo, short BizType = 3, short ProcessType = 3)
        {
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            Strsql = "SELECT 0 JOBPPK,";
            //Strsql &= vbCrLf & " JSE.JOB_CARD_TRN_PK JOBPK,"
            Strsql += "(SELECT ROWTOCOL(' SELECT DISTINCT JCT.JOB_CARD_TRN_PK FROM JOB_CARD_TRN JCT, INV_SUPPLIER_TBL SUP, JOB_TRN_PIA PIA WHERE";
            Strsql += "SUP.INV_SUPPLIER_PK=PIA.INV_SUPPLIER_FK";
            Strsql += "AND SUP.INVOICE_REF_NO =''' || IST.INVOICE_REF_NO || '''";
            Strsql += "AND PIA.JOB_CARD_TRN_FK=JCT.JOB_CARD_TRN_PK(+)') FROM DUAL) AS JOBPK,";

            //Strsql &= vbCrLf & " JSE.JOBCARD_REF_NO JOBNUMBER,"
            Strsql += "(SELECT ROWTOCOL(' SELECT DISTINCT JCT.JOBCARD_REF_NO FROM JOB_CARD_TRN JCT,INV_SUPPLIER_TBL SUP, JOB_TRN_PIA PIA WHERE";
            Strsql += "SUP.INV_SUPPLIER_PK=PIA.INV_SUPPLIER_FK";
            Strsql += "AND SUP.INVOICE_REF_NO =''' || IST.INVOICE_REF_NO || '''";
            Strsql += "AND PIA.JOB_CARD_TRN_FK=JCT.JOB_CARD_TRN_PK(+)') FROM DUAL) AS JOBNUMBER,";

            Strsql += " VMT.VENDOR_NAME   VENDOENAME,";
            Strsql += " IST.INVOICE_REF_NO INVOICENO,";
            Strsql += " IST.INVOICE_DATE INVOICEDATE,";
            Strsql += " IST.INVOICE_AMT INVOICE_AMT,";
            Strsql += " 0 AS TAX_AMT,";
            Strsql += " ((IST.INVOICE_AMT-PAYMENTS_TBL_PKG.CAL_PAID_AMOUNT(IST.INV_SUPPLIER_PK)) * GET_EX_RATE(CMST.CURRENCY_MST_PK," + BaseCurrFk + ", IST.INVOICE_DATE)) AS TOTAL_AMT,";
            Strsql += " CMST.CURRENCY_ID CURRENCY";
            Strsql += " FROM JOB_CARD_TRN JSE,";
            Strsql += " INV_SUPPLIER_TBL IST,";
            Strsql += " VENDOR_MST_TBL VMT,";
            Strsql += " CURRENCY_TYPE_MST_TBL CMST,";
            Strsql += " JOB_TRN_PIA JSEP";
            Strsql += " WHERE IST.INV_SUPPLIER_PK = JSEP.INV_SUPPLIER_FK";
            Strsql += " AND JSEP.JOB_CARD_TRN_FK=JSE.JOB_CARD_TRN_PK(+)";
            Strsql += " AND IST.VENDOR_MST_FK=VMT.VENDOR_MST_PK(+)";
            Strsql += " AND IST.CURRENCY_MST_FK=CMST.CURRENCY_MST_PK";
            Strsql += " AND IST.INVOICE_REF_NO IN (" + InvRefNo + ")";
            Strsql += " ORDER BY IST.INVOICE_DATE DESC";

            try
            {
                return ObjWF.GetDataSet(Strsql);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
           
        }
        public DataSet FetchAllPaymentReportPrintManager(string JobPk, short BizType = 3, short ProcessType = 3)
        {
            Strsql = "SELECT 0 JOBPPK,";
            Strsql += "JSE.JOB_CARD_TRN_PK JOBPK,";
            Strsql += "JSE.JOBCARD_REF_NO JOBNUMBER,";
            Strsql += "VM.VENDOR_NAME VENDOENAME,";
            Strsql += "JSEP.INVOICE_NUMBER INVOICENO,";
            Strsql += "JSEP.INVOICE_DATE INVOICEDATE,";
            Strsql += "SUM(JSEP.INVOICE_AMT) INVOICE_AMT,";
            Strsql += "SUM(JSEP.TAX_AMT) TAX_AMT,";
            Strsql += "SUM((JSEP.INVOICE_AMT+JSEP.TAX_AMT)) TOTAL_AMT, ";
            Strsql += "CMST.CURRENCY_ID CURRENCY";
            Strsql += "FROM JOB_TRN_PIA JSEP,";
            Strsql += "CURRENCY_TYPE_MST_TBL CMST,";
            Strsql += "JOB_CARD_TRN JSE,VENDOR_MST_TBL VM";
            Strsql += "WHERE CMST.CURRENCY_MST_PK(+)=JSEP.CURRENCY_MST_FK ";
            Strsql += "AND JSEP.JOB_CARD_TRN_FK=JSE.JOB_CARD_TRN_PK(+)";
            Strsql += "AND VM.VENDOR_MST_PK=JSEP.VENDOR_MST_FK";
            Strsql += "AND JSE.JOB_CARD_TRN_PK IN (" + JobPk + ")";
            Strsql += "GROUP BY JSE.JOB_CARD_TRN_PK ,";
            Strsql += "JSE.JOBCARD_REF_NO ,";
            Strsql += "VM.VENDOR_NAME ,";
            Strsql += "JSEP.INVOICE_NUMBER ,";
            Strsql += "JSEP.INVOICE_DATE,";
            Strsql += "CMST.CURRENCY_ID ";
            Strsql += "ORDER BY JSEP.INVOICE_DATE DESC,JSE.JOBCARD_REF_NO DESC";

            try
            {
                return ObjWF.GetDataSet(Strsql);
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
            
        }
        //***************************************************************************************
        public DataSet FetchSeaExpPaymentReport(string JobPk)
        {
            return FetchAllPaymentReport(JobPk, 2, 1);
        }
        public DataSet FetchSeaExpPaymentReportPrintManager(string JobPk)
        {
            return FetchAllPaymentReportPrintManager(JobPk, 2, 1);
        }
        public DataSet FetchSeaImpPaymentReport(string JobPk)
        {
            return FetchAllPaymentReport(JobPk, 2, 2);
        }
        public DataSet FetchSeaImpPaymentReportPrintManager(string JobPk)
        {
            return FetchAllPaymentReportPrintManager(JobPk, 2, 2);
        }
        public DataSet FetchAirExpPaymentReport(string JobPk)
        {
            return FetchAllPaymentReport(JobPk, 1, 1);
        }
        public DataSet FetchAirExpPaymentReportPrintManager(string JobPk)
        {
            return FetchAllPaymentReportPrintManager(JobPk, 1, 1);
        }
        public DataSet FetchAirImpPaymentReport(string JobPk)
        {
            return FetchAllPaymentReport(JobPk, 1, 2);
        }
        public DataSet FetchAirImpPaymentReportPrintManager(string JobPk)
        {
            return FetchAllPaymentReportPrintManager(JobPk, 1, 2);
        }
        public DataSet FetchBothExpPaymentReport(string JobPk)
        {
            return FetchAllPaymentReport(JobPk, 3, 1);
        }
        public DataSet FetchBothImpPaymentReport(string JobPk)
        {
            return FetchAllPaymentReport(JobPk, 3, 2);
        }
        #endregion

        #region "Enhance Search & Lookup Search For Vendor"
        public string FetchVendorName(string strCond)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strLOC_MST_IN = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strLoc = null;
            string strProcessType = null;
            string strReq = null;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            if (!string.IsNullOrEmpty(arr.GetValue(1).ToString()))
            {
                strSERACH_IN =  Convert.ToString(arr.GetValue(1));
            }
            else
            {
                strSERACH_IN = "";
            }
            strBizType =  Convert.ToString(arr.GetValue(2));
            strProcessType =  Convert.ToString(arr.GetValue(3));
            if ((arr.Length > 4))
            {
                strLoc =  Convert.ToString(arr.GetValue(4));
            }
            else
            {
                strLoc = "0";
            }
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_PAYMENT_REQUISITION.GET_VENDOR_NAME";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_TYPE_IN", strProcessType).Direction = ParameterDirection.Input;
                _with1.Add("LOC_FK_IN", strLoc).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
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
                selectCommand.Connection.Close();
            }
        }
        #endregion

        #region "Fill BizType Dropdown"
        public DataSet FetchBizType(string ConfigID)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            DataSet ds = new DataSet();
            try
            {
                sb.Append("SELECT DD.DD_VALUE, DD.DD_ID");
                sb.Append("  FROM QFOR_DROP_DOWN_TBL DD");
                sb.Append(" WHERE DD.DD_FLAG = 'BIZ_TYPE'");
                sb.Append("   AND DD.CONFIG_ID = '" + ConfigID + "'");
                sb.Append(" ORDER BY DD.DD_VALUE");
                ds = objWF.GetDataSet(sb.ToString());
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

    }
}