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
using Oracle.ManagedDataAccess.Types;
using System;
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class cls_EmailTracking : CommonFeatures
    {

        WorkFlow objWF = new WorkFlow();

        #region "Fetch Function"
        public DataSet FetchData(int MailType, int DocType, string DocRefNr, string Subject, string SendFrom, string SendTo, string FromDate, string ToDate, ref Int32 CurrentPage, ref Int32 TotalPage,Int32 flag, Int32 Exportflg)
        {

            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with1 = objWF.MyDataAdapter;
                _with1.SelectCommand = new OracleCommand();
                _with1.SelectCommand.Connection = objWF.MyConnection;
                _with1.SelectCommand.CommandText = objWF.MyUserName + ".MAIL_SEND_STATUS_TBL_PKG.FETCH_DATA";
                _with1.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with1.SelectCommand.Parameters.Add("MAIL_TYPE_IN", MailType).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("DOC_TYPE_IN", (DocType == 0 ? 0 : DocType)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("DOC_REF_NR_IN", (string.IsNullOrEmpty(DocRefNr) ? "" : DocRefNr)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("SUBJECT_IN", (string.IsNullOrEmpty(Subject) ? "" : Subject)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("SEND_FROM_IN", (string.IsNullOrEmpty(SendFrom) ? "" : SendFrom)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("SEND_TO_IN", (string.IsNullOrEmpty(SendTo) ? "" : SendTo)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDate) ? "" : FromDate)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDate) ? "" : ToDate)).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("CHK_LOAD_IN", flag).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("EXPORT_FLG_IN", Exportflg).Direction = ParameterDirection.Input;

                _with1.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", OracleDbType.Int32).Value = RecordsPerPage;
                _with1.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with1.SelectCommand.Parameters.Add("MAIL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with1.Fill(ds);
                TotalPage = Convert.ToInt32(_with1.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(_with1.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);

                return ds;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
            finally
            {
                objWF.CloseConnection();
            }

        }
        #endregion

        #region "Fetch Doc Ref Nr"
        public string FetchDocRef(string strcond, string loc = "0")
        {


            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strReq = null;
            string strSERACH_IN = null;
            int MailType = 0;
            int DocType = 0;

            var strNull = DBNull.Value;
            arr = strcond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                MailType = Convert.ToInt32(arr.GetValue(2));
            if (arr.Length > 3)
                DocType = Convert.ToInt32(Convert.ToString(arr.GetValue(3)));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EMAILLOG_PKG.GET_DOCREF";

                var _with2 = selectCommand.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("MAIL_TYPE_IN", MailType).Direction = ParameterDirection.Input;
                _with2.Add("DOC_TYPE_IN", (DocType == 0 ? 0 : DocType)).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Clob, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #region "Fetch Doc Type"
        public string FetchDocType(string strcond, string loc = "0")
        {

            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strReq = null;
            string strSERACH_IN = null;
            int MailType = 0;
            int DocType = 0;

            var strNull = DBNull.Value;
            arr = strcond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                MailType = Convert.ToInt32(Convert.ToString(arr.GetValue(2)));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EMAILLOG_PKG.GET_DOCTYPE";

                var _with3 = selectCommand.Parameters;
                _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("MAIL_TYPE_IN", MailType).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.Clob, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #region "Fetch Send To"
        public string FetchSendTo(string strcond, string loc = "0")
        {

            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strReq = null;
            string strSERACH_IN = null;
            int MailType = 0;
            int DocType = 0;
            string DocRefNr = "";

            var strNull = DBNull.Value;
            arr = strcond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                MailType = Convert.ToInt32(Convert.ToString(arr.GetValue(2)));
            if (arr.Length > 3)
                DocType = Convert.ToInt32(Convert.ToString(arr.GetValue(3)));
            if (arr.Length > 4)
                DocRefNr = Convert.ToString(arr.GetValue(4));;
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EMAILLOG_PKG.GET_SENDTO";

                var _with4 = selectCommand.Parameters;
                _with4.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("MAIL_TYPE_IN", MailType).Direction = ParameterDirection.Input;
                _with4.Add("DOC_TYPE_IN", (DocType == 0 ? 0 : DocType)).Direction = ParameterDirection.Input;
                _with4.Add("DOC_NR_IN", (string.IsNullOrEmpty(DocRefNr) ? "" : DocRefNr)).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.Clob, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #region "Fetch Send From"
        public string FetchSendFrom(string strcond, string loc = "0")
        {

            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strReq = null;
            string strSERACH_IN = null;
            int MailType = 0;
            int DocType = 0;
            string DocRefNr = "";

            var strNull = DBNull.Value;
            arr = strcond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));;
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));;
            if (arr.Length > 2)
                MailType = Convert.ToInt32(Convert.ToString(arr.GetValue(2)));
            if (arr.Length > 3)
                DocType = Convert.ToInt32(Convert.ToString(arr.GetValue(3)));
            if (arr.Length > 4)
                DocRefNr = Convert.ToString(arr.GetValue(4));;
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_EMAILLOG_PKG.GET_SENDFROM";

                var _with5 = selectCommand.Parameters;
                _with5.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with5.Add("MAIL_TYPE_IN", MailType).Direction = ParameterDirection.Input;
                _with5.Add("DOC_TYPE_IN", (DocType == 0 ? 0 : DocType)).Direction = ParameterDirection.Input;
                _with5.Add("DOC_NR_IN", (string.IsNullOrEmpty(DocRefNr) ? "" : DocRefNr)).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", OracleDbType.Clob, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #region "Fetch Popup"
        public DataSet FetchDtls(string DocRefNr, string DocType, int MailCount, int PK)
        {

            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with6 = objWF.MyDataAdapter;
                _with6.SelectCommand = new OracleCommand();
                _with6.SelectCommand.Connection = objWF.MyConnection;
                _with6.SelectCommand.CommandText = objWF.MyUserName + ".MAIL_SEND_STATUS_TBL_PKG.FETCH_DTLS";
                _with6.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with6.SelectCommand.Parameters.Add("DOC_REF_NR_IN", ((string.IsNullOrEmpty(DocRefNr) | DocRefNr == "null") ? "" : DocRefNr)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("DOC_TYPE_IN", ((string.IsNullOrEmpty(DocType) | DocType == "null") ? "" : DocType)).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("MAIL_COUNT_IN", MailCount).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("PK_IN", PK).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("MAIL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with6.Fill(ds);

                return ds;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }
        public DataSet FetchDtlsSub(string DocRefNr, string DocType, int MailCount, int PK)
        {

            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with7 = objWF.MyDataAdapter;
                _with7.SelectCommand = new OracleCommand();
                _with7.SelectCommand.Connection = objWF.MyConnection;
                _with7.SelectCommand.CommandText = objWF.MyUserName + ".MAIL_SEND_STATUS_TBL_PKG.FETCH_DTLS_SUB";
                _with7.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with7.SelectCommand.Parameters.Add("DOC_REF_NR_IN", ((string.IsNullOrEmpty(DocRefNr) | DocRefNr == "null") ? "" : DocRefNr)).Direction = ParameterDirection.Input;
                _with7.SelectCommand.Parameters.Add("DOC_TYPE_IN", ((string.IsNullOrEmpty(DocType) | DocType == "null") ? "" : DocType)).Direction = ParameterDirection.Input;
                _with7.SelectCommand.Parameters.Add("MAIL_COUNT_IN", MailCount).Direction = ParameterDirection.Input;
                _with7.SelectCommand.Parameters.Add("PK_IN", PK).Direction = ParameterDirection.Input;
                _with7.SelectCommand.Parameters.Add("MAIL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with7.Fill(ds);

                return ds;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }
        #endregion
    }
}