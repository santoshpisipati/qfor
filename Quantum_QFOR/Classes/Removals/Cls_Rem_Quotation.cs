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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_Rem_Quotation : CommonFeatures
    {
        /// <summary>
        /// The qt refno
        /// </summary>
        public string QtRefno;
        /// <summary>
        /// </summary>
        public string QTPK = "";

        /// <summary>
        /// Fetches the main.
        /// </summary>
        /// <param name="QTPk">The qt pk.</param>
        /// <returns></returns>
        public DataSet FetchMain(string QTPk)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            try
            {
                var _with1 = objWF.MyCommand.Parameters;
                //Fetch  details for report
                _with1.Clear();
                _with1.Add("QTPK_IN", QTPk).Direction = ParameterDirection.Input;
                _with1.Add("QTCUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("FETCH_REM_QTN_DETAILS_PKG", "FETCH_REM_PRINT_DTLS");
                return dsAll;
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

        /// <summary>
        /// Fetches the custumer.
        /// </summary>
        /// <param name="QTPk">The qt pk.</param>
        /// <returns></returns>
        public DataSet FetchCustumer(string QTPk)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            try
            {
                var _with2 = objWF.MyCommand.Parameters;
                //Fetch customer details for report
                _with2.Clear();
                _with2.Add("QTPK_IN", QTPk).Direction = ParameterDirection.Input;
                _with2.Add("QTCUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("FETCH_REM_QTN_DETAILS_PKG", "FETCH_REM_CUST_DTS");
                return dsAll;
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

        /// <summary>
        /// Fetches the modes.
        /// </summary>
        /// <param name="QTPk">The qt pk.</param>
        /// <returns></returns>
        public DataSet FetchModes(string QTPk)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            try
            {
                var _with3 = objWF.MyCommand.Parameters;
                //Fetch Modes details for report
                _with3.Clear();
                _with3.Add("QTPK_IN", QTPk).Direction = ParameterDirection.Input;
                _with3.Add("QTCUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("FETCH_REM_QTN_DETAILS_PKG", "FETCH_REM_MODES");
                return dsAll;
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

        /// <summary>
        /// Fetches the FRTDTLS.
        /// </summary>
        /// <param name="QTPk">The qt pk.</param>
        /// <returns></returns>
        public DataSet FetchFrtdtls(string QTPk)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            try
            {
                var _with4 = objWF.MyCommand.Parameters;
                //Fetch frt. details for report
                _with4.Clear();
                _with4.Add("QTPK_IN", QTPk).Direction = ParameterDirection.Input;
                _with4.Add("CURRPK_IN", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with4.Add("QTCUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("FETCH_REM_QTN_DETAILS_PKG", "FETCH_REM_FRT_DTLS");
                return dsAll;
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

        /// <summary>
        /// Fetches the oth CHRG.
        /// </summary>
        /// <param name="QTPk">The qt pk.</param>
        /// <returns></returns>
        public DataSet FetchOthChrg(string QTPk)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            try
            {
                var _with5 = objWF.MyCommand.Parameters;
                //Fetch oth.chrg details for report
                _with5.Clear();
                _with5.Add("QTPK_IN", QTPk).Direction = ParameterDirection.Input;
                _with5.Add("QTCUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("FETCH_REM_QTN_DETAILS_PKG", "FETCH_REM_OTH_CHRG");
                return dsAll;
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

        /// <summary>
        /// Fetches the enquiry.
        /// </summary>
        /// <param name="strcond">The strcond.</param>
        /// <returns></returns>
        public string FetchEnquiry(string strcond)
        {
            WorkFlow objwf = new WorkFlow();
            objwf.OpenConnection();
            OracleTransaction TRAN = null;
            string strReturn = null;
            Array arr = null;
            Int32 flags = default(Int32);
            string strsearch = null;
            string LookUpValue = null;
            arr = strcond.Split('~');
            LookUpValue = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strsearch = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 3)
                flags = Convert.ToInt32(arr.GetValue(3));
            try
            {
                var _with6 = objwf.MyCommand;
                _with6.Connection = objwf.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objwf.MyUserName + ".FETCH_REM_QTN_DETAILS_PKG.FETCH_REM_ENQ_DETAILS";
                _with6.Parameters.Add("SEARCH_IN", getDefault(strsearch, "")).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("LOOKUP_VALUE_IN", LookUpValue).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("FLAG", flags).Direction = ParameterDirection.Input;
                _with6.Parameters.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                objwf.MyCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(objwf.MyCommand.Parameters["RETURN_VALUE"].Value).Trim();
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
        }

        /// <summary>
        /// Deletesurvies the specified surpk.
        /// </summary>
        /// <param name="surpk">The surpk.</param>
        public void deletesurvy(string surpk)
        {
            string[] survypk = null;
            WorkFlow objwf = new WorkFlow();
            objwf.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objwf.MyConnection.BeginTransaction();
            survypk = surpk.Split(',');
            Int32 i = default(Int32);
            try
            {
                var _with7 = objwf.MyCommand;
                _with7.Connection = objwf.MyConnection;
                _with7.Transaction = TRAN;
                for (i = 0; i <= survypk.Length - 1; i++)
                {
                    if (survypk[i].Length > 0)
                    {
                        objwf.MyCommand.CommandType = CommandType.StoredProcedure;
                        objwf.MyCommand.CommandText = objwf.MyUserName + ".FETCH_REM_QTN_DETAILS_PKG.DEL_SURVEY_DTLS";
                        objwf.MyCommand.Parameters.Clear();
                        objwf.MyCommand.Parameters.Add("SURPK_IN", Convert.ToInt32(survypk[i])).Direction = ParameterDirection.Input;
                        objwf.MyCommand.ExecuteNonQuery();
                    }
                }
                TRAN.Commit();
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
                objwf.MyCommand.Cancel();
                objwf.MyConnection.Close();
            }
        }

        /// <summary>
        /// Fetches the enq for listing.
        /// </summary>
        /// <param name="strcond">The strcond.</param>
        /// <returns></returns>
        public string FetchEnqForListing(string strcond)
        {
            WorkFlow objwf = new WorkFlow();
            objwf.OpenConnection();
            OracleTransaction TRAN = null;
            string strReturn = null;
            Array arr = null;
            Int32 flags = default(Int32);
            string strsearch = null;
            string LookUpValue = null;
            arr = strcond.Split('~');
            LookUpValue = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strsearch = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 3)
                flags = Convert.ToInt32(arr.GetValue(3));
            try
            {
                var _with8 = objwf.MyCommand;
                _with8.Connection = objwf.MyConnection;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objwf.MyUserName + ".FETCH_REM_QTN_DETAILS_PKG.FETCH_REM_ENQ_LISTING";
                _with8.Parameters.Add("SEARCH_IN", getDefault(strsearch, "")).Direction = ParameterDirection.Input;
                _with8.Parameters.Add("LOOKUP_VALUE_IN", LookUpValue).Direction = ParameterDirection.Input;
                _with8.Parameters.Add("FLAG", flags).Direction = ParameterDirection.Input;
                _with8.Parameters.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                objwf.MyCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(objwf.MyCommand.Parameters["RETURN_VALUE"].Value).Trim();
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
        }

        //This is used for fetching Enq or survey details Before Saving
        /// <summary>
        /// Fetches the details.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="dsheader">The dsheader.</param>
        /// <param name="dsgrid">The dsgrid.</param>
        public void FetchDetails(long pk, Int32 flag, DataSet dsheader, DataSet dsgrid)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            try
            {
                var _with9 = objWF.MyCommand.Parameters;
                //Fetch Header details only
                _with9.Clear();
                _with9.Add("EnqPK_IN", pk).Direction = ParameterDirection.Input;
                _with9.Add("Flag_IN", flag).Direction = ParameterDirection.Input;
                _with9.Add("ENQCUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsheader = objWF.GetDataSet("FETCH_REM_QTN_DETAILS_PKG", "FETCH_REM_ENQ");

                var _with10 = objWF.MyCommand.Parameters;
                //Fetch Parent details only
                _with10.Clear();
                _with10.Add("EnqPK_IN", pk).Direction = ParameterDirection.Input;
                _with10.Add("Flag_IN", flag).Direction = ParameterDirection.Input;
                _with10.Add("FETCHD", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsgrid.Tables.Add(objWF.GetDataTable("FETCH_REM_QTN_DETAILS_PKG", "FETCH_REM_ENQ_Header"));

                var _with11 = objWF.MyCommand.Parameters;
                //Fetch Child details only
                _with11.Clear();
                _with11.Add("EnqPK_IN", pk).Direction = ParameterDirection.Input;
                _with11.Add("Flag_IN", flag).Direction = ParameterDirection.Input;
                _with11.Add("FETCCHILD", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsgrid.Tables.Add(objWF.GetDataTable("FETCH_REM_QTN_DETAILS_PKG", "FETCH_REM_ENQ_CHILD"));

                //Making relation between tables
                DataRelation CONTRel = null;
                CONTRel = new DataRelation("CONTRelation", dsgrid.Tables[0].Columns["ENQPK"], dsgrid.Tables[1].Columns["ENQFK"], true);
                CONTRel.Nested = true;
                dsgrid.Relations.Add(CONTRel);
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

        //This is used for fetching Enq or survey details after Saving
        /// <summary>
        /// Saveds the quotation.
        /// </summary>
        /// <param name="Qtpk">The QTPK.</param>
        /// <param name="status">The status.</param>
        /// <param name="dsheader">The dsheader.</param>
        /// <param name="dsgrid">The dsgrid.</param>
        public void SavedQuotation(Int32 Qtpk, Int32 status, DataSet dsheader, DataSet dsgrid)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            try
            {
                var _with12 = objWF.MyCommand.Parameters;
                //Fetch Header details only
                _with12.Clear();
                _with12.Add("QTPK_IN", Qtpk).Direction = ParameterDirection.Input;
                _with12.Add("QTCUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsheader = objWF.GetDataSet("FETCH_REM_QTN_DETAILS_PKG", "FETCH_REM_QUT");

                var _with13 = objWF.MyCommand.Parameters;
                //Fetch Parent details only
                _with13.Clear();
                _with13.Add("QTPK_IN", Qtpk).Direction = ParameterDirection.Input;
                _with13.Add("FETCHD", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsgrid.Tables.Add(objWF.GetDataTable("FETCH_REM_QTN_DETAILS_PKG", "FETCH_QUT_HEADER"));

                var _with14 = objWF.MyCommand.Parameters;
                //Fetch Child details only
                _with14.Clear();
                _with14.Add("QTPK_IN", Qtpk).Direction = ParameterDirection.Input;
                _with14.Add("STATUS_IN", status).Direction = ParameterDirection.Input;
                _with14.Add("FETCCHILD", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsgrid.Tables.Add(objWF.GetDataTable("FETCH_REM_QTN_DETAILS_PKG", "FETCH_QUT_CHILD"));

                //Making relation between tables
                DataRelation CONTRel = null;
                CONTRel = new DataRelation("CONTRelation", dsgrid.Tables[0].Columns["QTPK"], dsgrid.Tables[1].Columns["QTFK"], true);
                CONTRel.Nested = true;
                dsgrid.Relations.Add(CONTRel);
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

        /// <summary>
        /// Fetches the type of the payment.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchPaymentType()
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            StringBuilder strsql = new StringBuilder();
            try
            {
                strsql.Append(" SELECT 'Prepaid' PAYMNETTYPE, 1 VAL FROM DUAL UNION ");
                strsql.Append(" SELECT 'Collect' PAYMNETTYPE, 2 VAL FROM DUAL order by val");
                return objWF.GetDataSet(strsql.ToString());
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

        /// <summary>
        /// Fetches the hf.
        /// </summary>
        /// <param name="QTPk">The qt pk.</param>
        /// <returns></returns>
        public DataSet FetchHF(Int32 QTPk)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            StringBuilder strsql = new StringBuilder();
            try
            {
                strsql.Append(" SELECT QT.QUOT_HEADER header_content,QT.QUOT_FOOTER footer_content FROM REM_M_QUOT_MST_TBL QT WHERE QT.QUOT_PK=" + QTPk);
                return objWF.GetDataSet(strsql.ToString());
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

        /// <summary>
        /// Gets the structure.
        /// </summary>
        /// <param name="QTPK">The QTPK.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public DataSet GetStructure(string QTPK, Int32 flag)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = null;
            StringBuilder strsql = new StringBuilder();
            StringBuilder sql = new StringBuilder();
            Int32 trnpk = default(Int32);
            try
            {
                if (flag == 1)
                {
                    strsql.Append(" SELECT  * from REM_M_QUOT_MST_TBL WHERE 1=2");
                }
                else if (flag == 2)
                {
                    strsql.Append(" SELECT  * from REM_T_QUOT_TRN_HRD_TBL HDR WHERE ");
                    if (QTPK.Length > 0)
                    {
                        strsql.Append("HDR.QUOT_FK = " + QTPK);
                    }
                    else
                    {
                        strsql.Append(" 1=2");
                    }
                }
                else
                {
                    strsql.Append(" SELECT  * from REM_T_QUOT_TRN_FRT_DTL_TBL FRT ");
                    if (QTPK.Length > 0)
                    {
                        sql.Append(" SELECT HDR.QUOT_TRN_HDR_PK from REM_T_QUOT_TRN_HRD_TBL HDR WHERE HDR.QUOT_FK = " + QTPK);
                        trnpk = Convert.ToInt32(objWF.ExecuteScaler(sql.ToString()));
                        strsql.Append(" WHERE FRT.QUOT_TRN_HDR_FK=" + trnpk);
                    }
                    else
                    {
                        strsql.Append(" WHERE 1=2");
                    }
                }
                return objWF.GetDataSet(strsql.ToString());
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary>
        /// Saves the quotation.
        /// </summary>
        /// <param name="DsQuot">The ds quot.</param>
        /// <param name="dsrate">The dsrate.</param>
        /// <param name="dsfreight">The dsfreight.</param>
        /// <returns></returns>
        public string SaveQuotation(DataSet DsQuot, DataSet dsrate, DataSet dsfreight)
        {
            WorkFlow objWK = new WorkFlow();
            string RefNr = null;
            OracleTransaction TRANS = null;
            OracleCommand insCommand = new OracleCommand();
            OracleCommand upCommand = new OracleCommand();
            string Traspk = null;
            string res = null;
            DataSet dssurvey = null;
            objWK.OpenConnection();
            TRANS = objWK.MyConnection.BeginTransaction();
            try
            {
                if (string.IsNullOrEmpty(getDefault(QTPK, "").ToString()))
                {
                    RefNr = GenerateProtocolKey("REMOVAL QUOTATION", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), DateTime.Today.Date, Convert.ToString(HttpContext.Current.Session["USER_PK"]));
                }
                if (string.IsNullOrEmpty(getDefault(QTPK, "").ToString()))
                {
                    var _with15 = insCommand;
                    _with15.Connection = objWK.MyConnection;
                    _with15.CommandType = CommandType.StoredProcedure;
                    _with15.CommandText = objWK.MyUserName + ".FETCH_REM_QTN_DETAILS_PKG.REM_QUT_MAS_INS";
                    insCommand.Parameters.Clear();
                    insCommand.Parameters.Add("QUOT_REF_IN", RefNr).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_REF_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_DATE_IN", DsQuot.Tables[0].Rows[0]["QUOT_DATE"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_SURVEY_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_SURVEY_FK"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_SURVEY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_ENQ_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_ENQ_FK"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_ENQ_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_MOVE_DT_IN", DsQuot.Tables[0].Rows[0]["QUOT_MOVE_DT"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_MOVE_DT_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_DEL_DT_IN", DsQuot.Tables[0].Rows[0]["QUOT_DEL_DT"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_DEL_DT_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_STATUS_IN", DsQuot.Tables[0].Rows[0]["QUOT_STATUS"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_VALID_FROM_DT_IN", DsQuot.Tables[0].Rows[0]["QUOT_VALID_FROM_DT"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_VALID_FROM_DT_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_VALID_TO_DT_IN", DsQuot.Tables[0].Rows[0]["QUOT_VALID_TO_DT"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_VALID_TO_DT_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_MOVE_TYPE_IN", DsQuot.Tables[0].Rows[0]["QUOT_MOVE_TYPE"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_MOVE_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_MOVE_SRV_PKG_IN", DsQuot.Tables[0].Rows[0]["QUOT_MOVE_SRV_PKG"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_MOVE_SRV_PKG_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_MOVE_SRV_MVG_IN", DsQuot.Tables[0].Rows[0]["QUOT_MOVE_SRV_MVG"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_MOVE_SRV_MVG_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_MOVE_SRV_UNPKG_IN", DsQuot.Tables[0].Rows[0]["QUOT_MOVE_SRV_UNPKG"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_MOVE_SRV_UNPKG_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_PARTY_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_PARTY_FK"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_PARTY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_PLR_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_PLR_FK"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_PLR_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_PLR_ADDR1_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PLR_ADDR1"], "")).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_PLR_ADDR1_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_PLR_ADDR2_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PLR_ADDR2"], "")).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_PLR_ADDR2_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_PLR_CITY_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PLR_CITY"], "")).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_PLR_CITY_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_PLR_ZIP_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PLR_ZIP"], "")).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_PLR_ZIP_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_PLR_COUNTRY_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_PLR_COUNTRY_FK"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_PLR_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_PFD_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_PFD_FK"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_PFD_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_PFD_ADDR1_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PFD_ADDR1"], "")).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_PFD_ADDR1_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_PFD_ADDR2_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PFD_ADDR2"], "")).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_PFD_ADDR2_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_PFD_CITY_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PFD_CITY"], "")).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_PFD_CITY_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_PFD_ZIP_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PFD_ZIP"], "")).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_PFD_ZIP_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_PFD_COUNTRY_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_PFD_COUNTRY_FK"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_PFD_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("QUOT_NOTES_IN", DsQuot.Tables[0].Rows[0]["QUOT_NOTES"]).Direction = ParameterDirection.Input;
                    insCommand.Parameters["QUOT_NOTES_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    insCommand.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("CREATED_DT_IN", DateTime.Now.Date).Direction = ParameterDirection.Input;
                    insCommand.Parameters["CREATED_DT_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("HEADER_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_HEADER"], "")).Direction = ParameterDirection.Input;
                    insCommand.Parameters["HEADER_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("FOOTER_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_FOOTER"], "")).Direction = ParameterDirection.Input;
                    insCommand.Parameters["FOOTER_IN"].SourceVersion = DataRowVersion.Current;

                    insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with16 = objWK.MyDataAdapter;
                    _with16.InsertCommand = insCommand;
                    _with16.InsertCommand.Transaction = TRANS;
                    _with16.InsertCommand.ExecuteNonQuery();
                    QTPK = Convert.ToString(insCommand.Parameters["RETURN_VALUE"].Value);
                    Traspk = SaveQtdtl(QTPK, TRANS, dsrate);
                    res = SaveFrts(Traspk, TRANS, dsfreight);
                    dssurvey = (DataSet)HttpContext.Current.Session["SURVEY"];
                    if ((dssurvey != null))
                    {
                        if (dssurvey.Tables[0].Rows.Count > 0)
                        {
                            res = SaveSurveydtl(QTPK, TRANS, dssurvey);
                        }
                    }
                    //adding by thiyagarajan on 22/1/09:TrackNTrace Task:VEK Req.
                    SaveInTrackNTrace(RefNr, Convert.ToInt32(DsQuot.Tables[0].Rows[0]["QUOT_ENQ_FK"]), "Quotation Generated", 3, TRANS);
                    //end
                    TRANS.Commit();
                    QtRefno = RefNr;
                    // objWK.MyConnection.Close()
                    return "Saved";
                }
                else
                {
                    var _with17 = upCommand;
                    _with17.Connection = objWK.MyConnection;
                    _with17.CommandType = CommandType.StoredProcedure;
                    _with17.CommandText = objWK.MyUserName + ".FETCH_REM_QTN_DETAILS_PKG.REM_QUT_MAS_UPN";
                    upCommand.Parameters.Clear();

                    upCommand.Parameters.Add("QUOT_PK_IN", Convert.ToInt64(QTPK)).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PK_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_SURVEY_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_SURVEY_FK"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_SURVEY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_ENQ_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_ENQ_FK"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_ENQ_FK_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_MOVE_DT_IN", DsQuot.Tables[0].Rows[0]["QUOT_MOVE_DT"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_MOVE_DT_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_DEL_DT_IN", DsQuot.Tables[0].Rows[0]["QUOT_DEL_DT"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_DEL_DT_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_STATUS_IN", DsQuot.Tables[0].Rows[0]["QUOT_STATUS"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_STATUS_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_VALID_FROM_DT_IN", DsQuot.Tables[0].Rows[0]["QUOT_VALID_FROM_DT"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_VALID_FROM_DT_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_VALID_TO_DT_IN", DsQuot.Tables[0].Rows[0]["QUOT_VALID_TO_DT"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_VALID_TO_DT_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_MOVE_TYPE_IN", DsQuot.Tables[0].Rows[0]["QUOT_MOVE_TYPE"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_MOVE_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_MOVE_SRV_PKG_IN", DsQuot.Tables[0].Rows[0]["QUOT_MOVE_SRV_PKG"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_MOVE_SRV_PKG_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_MOVE_SRV_MVG_IN", DsQuot.Tables[0].Rows[0]["QUOT_MOVE_SRV_MVG"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_MOVE_SRV_MVG_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_MOVE_SRV_UNPKG_IN", DsQuot.Tables[0].Rows[0]["QUOT_MOVE_SRV_UNPKG"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_MOVE_SRV_UNPKG_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_PARTY_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_PARTY_FK"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PARTY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_PLR_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_PLR_FK"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PLR_FK_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_PLR_ADDR1_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PLR_ADDR1"], "")).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PLR_ADDR1_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_PLR_ADDR2_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PLR_ADDR2"], "")).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PLR_ADDR2_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_PLR_CITY_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PLR_CITY"], "")).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PLR_CITY_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_PLR_ZIP_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PLR_ZIP"], "")).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PLR_ZIP_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_PLR_COUNTRY_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_PLR_COUNTRY_FK"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PLR_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_PFD_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_PFD_FK"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PFD_FK_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_PFD_ADDR1_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PFD_ADDR1"], "")).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PFD_ADDR1_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_PFD_ADDR2_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PFD_ADDR2"], "")).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PFD_ADDR2_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_PFD_CITY_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PFD_CITY"], "")).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PFD_CITY_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_PFD_ZIP_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_PFD_ZIP"], "")).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PFD_ZIP_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_PFD_COUNTRY_FK_IN", DsQuot.Tables[0].Rows[0]["QUOT_PFD_COUNTRY_FK"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_PFD_COUNTRY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("QUOT_NOTES_IN", DsQuot.Tables[0].Rows[0]["QUOT_NOTES"]).Direction = ParameterDirection.Input;
                    upCommand.Parameters["QUOT_NOTES_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    upCommand.Parameters["MODIFIED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("MODIFIED_DT_IN", DateTime.Now.Date).Direction = ParameterDirection.Input;
                    upCommand.Parameters["MODIFIED_DT_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("HEADER_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_HEADER"], "")).Direction = ParameterDirection.Input;
                    upCommand.Parameters["HEADER_IN"].SourceVersion = DataRowVersion.Current;

                    upCommand.Parameters.Add("FOOTER_IN", getDefault(DsQuot.Tables[0].Rows[0]["QUOT_FOOTER"], "")).Direction = ParameterDirection.Input;
                    upCommand.Parameters["FOOTER_IN"].SourceVersion = DataRowVersion.Current;

                    var _with18 = objWK.MyDataAdapter;
                    _with18.UpdateCommand = upCommand;
                    _with18.UpdateCommand.Transaction = TRANS;
                    _with18.UpdateCommand.ExecuteNonQuery();
                    Traspk = SaveQtdtl(QTPK, TRANS, dsrate);
                    res = SaveFrts(Traspk, TRANS, dsfreight);
                    dssurvey = (DataSet)HttpContext.Current.Session["SURVEY"];
                    if ((dssurvey != null))
                    {
                        if (dssurvey.Tables[0].Rows.Count > 0)
                        {
                            res = SaveSurveydtl(QTPK, TRANS, dssurvey);
                        }
                    }
                    TRANS.Commit();
                    objWK.MyConnection.Close();
                    return "Saved";
                }
                //'Manjunath  PTS ID:Sep-02  28/09/2011
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRANS.Rollback();
                if (string.IsNullOrEmpty(getDefault(QTPK, "").ToString()))
                {
                    RollbackProtocolKey("REMOVAL QUOTATION", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), RefNr, System.DateTime.Now);
                }
                throw ex;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }

        /// <summary>
        /// Fetches the survey grid.
        /// </summary>
        /// <param name="Qtpk">The QTPK.</param>
        /// <returns></returns>
        public DataSet FetchSurveyGrid(long Qtpk)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();
            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with19 = objWK.MyCommand;
                _with19.CommandType = CommandType.StoredProcedure;
                _with19.CommandText = objWK.MyUserName + ".FETCH_REM_QTN_DETAILS_PKG.SURVEY_INFO_FETCH";
                objWK.MyCommand.Parameters.Clear();
                var _with20 = objWK.MyCommand.Parameters;
                _with20.Add("QTPK_IN", Qtpk).Direction = ParameterDirection.Input;
                _with20.Add("FET_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

                return dsData;
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

        /// <summary>
        /// Fetches the status.
        /// </summary>
        /// <param name="qtpk">The QTPK.</param>
        /// <returns></returns>
        public DataSet FetchStatus(string qtpk)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append(" SELECT QT.QUOT_STATUS,TRN.QUOT_TRN_HDR_REF_TYPE ,QT.QUOT_REF_NR,ENQ.REM_M_ENQ_REF_NR,SUR.SURVEY_NUMBER,QT.QUOT_ENQ_FK ENQPK,QT.QUOT_SURVEY_FK SURPK FROM REM_M_QUOT_MST_TBL QT,REM_T_QUOT_TRN_HRD_TBL TRN ,REM_M_SURVEY_MST_TBL SUR,REM_M_ENQUIRY_MST_TBL ENQ ");
                strQuery.Append(" WHERE QT.QUOT_PK=" + qtpk + "AND QT.QUOT_PK=TRN.QUOT_FK AND QT.QUOT_SURVEY_FK=SUR.SURVEY_MST_PK(+) AND QT.QUOT_ENQ_FK=ENQ.REM_M_ENQ_MST_TBL_PK(+) ");
                return objWF.GetDataSet(strQuery.ToString());
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

        //adding by thiyagarajan on 23/1/09:TrackNTrace Task:VEK Req.
        /// <summary>
        /// Saves the in track n trace.
        /// </summary>
        /// <param name="refno">The refno.</param>
        /// <param name="refpk">The refpk.</param>
        /// <param name="status">The status.</param>
        /// <param name="Doctype">The doctype.</param>
        /// <param name="TRAN">The tran.</param>
        public void SaveInTrackNTrace(string refno, Int32 refpk, string status, Int32 Doctype, OracleTransaction TRAN)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            Int32 Return_value = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            try
            {
                objWF.OpenConnection();
                objWF.MyConnection = TRAN.Connection;
                insCommand.Connection = objWF.MyConnection;
                insCommand.Transaction = TRAN;
                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.CommandText = objWF.MyUserName + ".REM_TRACK_N_TRACE_PKG.REM_TRACK_N_TRACE_INS";
                var _with21 = insCommand.Parameters;
                _with21.Clear();
                _with21.Add("REF_NO_IN", refno).Direction = ParameterDirection.Input;
                _with21.Add("REF_FK_IN", refpk).Direction = ParameterDirection.Input;
                _with21.Add("LOC_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with21.Add("STATUS_IN", status).Direction = ParameterDirection.Input;
                _with21.Add("CREATED_BY_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with21.Add("DOCTYPE_IN", Doctype).Direction = ParameterDirection.Input;
                insCommand.ExecuteNonQuery();
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

        //end

        /// <summary>
        /// Saves the QTDTL.
        /// </summary>
        /// <param name="QTPK">The QTPK.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="dsrate">The dsrate.</param>
        /// <returns></returns>
        private string SaveQtdtl(string QTPK, OracleTransaction TRAN, DataSet dsrate)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            try
            {
                objWK.OpenConnection();
                objWK.MyConnection = TRAN.Connection;
                var _with22 = insCommand;
                _with22.Connection = objWK.MyConnection;
                _with22.CommandType = CommandType.StoredProcedure;
                _with22.CommandText = objWK.MyUserName + ".FETCH_REM_QTN_DETAILS_PKG.REM_QUT_HDRTRN_INS";
                _with22.Parameters.Clear();
                insCommand.Parameters.Add("QTFK_IN", QTPK).Direction = ParameterDirection.Input;
                insCommand.Parameters["QTFK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("QUOT_TRN_HDR_REF_TYPE_IN", OracleDbType.Int32, 1, "QUOT_TRN_HDR_REF_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["QUOT_TRN_HDR_REF_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("QUOT_TRN_HDR_REF_FK_IN", OracleDbType.Int32, 10, "QUOT_TRN_HDR_REF_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["QUOT_TRN_HDR_REF_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("QUOT_TRN_HDR_REF_NR_IN", OracleDbType.Varchar2, 20, "QUOT_TRN_HDR_REF_NR").Direction = ParameterDirection.Input;
                insCommand.Parameters["QUOT_TRN_HDR_REF_NR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("QUOT_TRN_HDR_AI_RATE_IN", OracleDbType.Int32, 10, "QUOT_TRN_HDR_AI_RATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["QUOT_TRN_HDR_AI_RATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_DT_IN", DateTime.Now.Date).Direction = ParameterDirection.Input;
                insCommand.Parameters["CREATED_DT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "QUOT_TRN_HDR_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with23 = updCommand;
                _with23.Connection = objWK.MyConnection;
                _with23.CommandType = CommandType.StoredProcedure;
                _with23.CommandText = objWK.MyUserName + ".FETCH_REM_QTN_DETAILS_PKG.REM_QUT_HDRTRN_UPD";
                _with23.Parameters.Clear();
                updCommand.Parameters.Add("TRN_PK_IN", OracleDbType.Int32, 10, "QUOT_TRN_HDR_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["TRN_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QUOT_TRN_HDR_REF_TYPE_IN", OracleDbType.Int32, 1, "QUOT_TRN_HDR_REF_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUOT_TRN_HDR_REF_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QUOT_TRN_HDR_REF_FK_IN", OracleDbType.Int32, 10, "QUOT_TRN_HDR_REF_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUOT_TRN_HDR_REF_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QUOT_TRN_HDR_REF_NR_IN", OracleDbType.Varchar2, 20, "QUOT_TRN_HDR_REF_NR").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUOT_TRN_HDR_REF_NR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QUOT_TRN_HDR_AI_RATE_IN", OracleDbType.Int32, 10, "QUOT_TRN_HDR_AI_RATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUOT_TRN_HDR_AI_RATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                //updCommand.Parameters["MODIFIED_BY_FK_IN"].SourceVersion = DataRowVersion.Current

                updCommand.Parameters.Add("MODIFIED_DT_IN", DateTime.Now.Date).Direction = ParameterDirection.Input;
                //updCommand.Parameters["MODIFIED_DT_IN"].SourceVersion = DataRowVersion.Current

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "QUOT_TRN_HDR_PK").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with24 = objWK.MyDataAdapter;
                _with24.InsertCommand = insCommand;
                _with24.InsertCommand.Transaction = TRAN;
                _with24.UpdateCommand = updCommand;
                _with24.UpdateCommand.Transaction = TRAN;
                Int32 RecAfct = default(Int32);
                RecAfct = _with24.Update(dsrate);
                if (RecAfct > 0)
                {
                    return Convert.ToString(dsrate.Tables[0].Rows[0]["QUOT_TRN_HDR_PK"]);
                }
                else
                {
                    return Convert.ToString(dsrate.Tables[0].Rows[0]["QUOT_TRN_HDR_PK"]);
                }
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

        /// <summary>
        /// Saves the surveydtl.
        /// </summary>
        /// <param name="QTPK">The QTPK.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="dssurvey">The dssurvey.</param>
        /// <returns></returns>
        public string SaveSurveydtl(string QTPK, OracleTransaction TRAN, DataSet dssurvey)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();
            Int32 RecAfct = default(Int32);
            try
            {
                objWK.OpenConnection();
                objWK.MyConnection = TRAN.Connection;
                var _with25 = insCommand;
                _with25.Connection = objWK.MyConnection;
                _with25.CommandType = CommandType.StoredProcedure;
                _with25.CommandText = objWK.MyUserName + ".FETCH_REM_QTN_DETAILS_PKG.REM_QUT_SURVEY_INS";
                _with25.Parameters.Clear();
                insCommand.Parameters.Add("QTFK_IN", QTPK).Direction = ParameterDirection.Input;
                insCommand.Parameters["QTFK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("QUOT_SURVEY_DTL_PK_IN", OracleDbType.Int32, 10, "survey_dtl_pk").Direction = ParameterDirection.Input;
                insCommand.Parameters["QUOT_SURVEY_DTL_PK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ITEM_IN", OracleDbType.Varchar2, 50, "ITEM").Direction = ParameterDirection.Input;
                insCommand.Parameters["ITEM_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("QTY_IN", OracleDbType.Int32, 7, "QUANTITY").Direction = ParameterDirection.Input;
                insCommand.Parameters["QTY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("C_ORIGIN", OracleDbType.Varchar2, 100, "C_ORIGIN").Direction = ParameterDirection.Input;
                insCommand.Parameters["C_ORIGIN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PACK_QTY_IN", OracleDbType.Int32, 7, "pack_qty").Direction = ParameterDirection.Input;
                insCommand.Parameters["PACK_QTY_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("LENGHT_IN", OracleDbType.Int32, 11, "LENGTH").Direction = ParameterDirection.Input;
                insCommand.Parameters["LENGHT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WIDTH_IN", OracleDbType.Int32, 11, "WIDTH").Direction = ParameterDirection.Input;
                insCommand.Parameters["WIDTH_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("HEIGHT_IN", OracleDbType.Int32, 11, "HEIGHT").Direction = ParameterDirection.Input;
                insCommand.Parameters["HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("VOLUME_IN", OracleDbType.Int32, 22, "VOLUME").Direction = ParameterDirection.Input;
                insCommand.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("WEIGHT_IN", OracleDbType.Int32, 11, "WEIGHT").Direction = ParameterDirection.Input;
                insCommand.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("DISMANTING_IN", OracleDbType.Int32, 1, "DISMANTING").Direction = ParameterDirection.Input;
                insCommand.Parameters["DISMANTING_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("NO_OF_UNITS_IN", OracleDbType.Int32, 5, "NO_OF_UNITS").Direction = ParameterDirection.Input;
                insCommand.Parameters["NO_OF_UNITS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("MODE_IN", OracleDbType.Int32, 1, "MODE_OF_TRANSPORT").Direction = ParameterDirection.Input;
                insCommand.Parameters["MODE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("AREA_DESC_IN", OracleDbType.Varchar2, 50, "AREA_DESCRIPTION").Direction = ParameterDirection.Input;
                insCommand.Parameters["AREA_DESC_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("ELEVATOR_IN", OracleDbType.Int32, 1, "ELEVATOR").Direction = ParameterDirection.Input;
                insCommand.Parameters["ELEVATOR_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PACK_TYPE_FK_IN", OracleDbType.Int32, 10, "PACK_TYPE_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 250, "REMARKS").Direction = ParameterDirection.Input;
                insCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_DT_IN", DateTime.Now.Date).Direction = ParameterDirection.Input;
                insCommand.Parameters["CREATED_DT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                var _with26 = updCommand;
                _with26.Connection = objWK.MyConnection;
                _with26.CommandType = CommandType.StoredProcedure;
                _with26.CommandText = objWK.MyUserName + ".FETCH_REM_QTN_DETAILS_PKG.REM_QUT_SURVEY_UPD";
                _with26.Parameters.Clear();
                updCommand.Parameters.Add("QUOT_SURVEY_DTL_PK_IN", OracleDbType.Int32, 10, "survey_dtl_pk").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUOT_SURVEY_DTL_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ITEM_IN", OracleDbType.Varchar2, 50, "ITEM").Direction = ParameterDirection.Input;
                updCommand.Parameters["ITEM_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QTY_IN", OracleDbType.Int32, 7, "QUANTITY").Direction = ParameterDirection.Input;
                updCommand.Parameters["QTY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("C_ORIGIN", OracleDbType.Varchar2, 100, "C_ORIGIN").Direction = ParameterDirection.Input;
                updCommand.Parameters["C_ORIGIN"].SourceVersion = DataRowVersion.Current;

                //updCommand.Parameters.Add("EXP_DEST_IN", OracleClient.OracleDbType.Varchar2, 100, "EXP_DESTINATION").Direction = ParameterDirection.Input
                //updCommand.Parameters["EXP_DEST_IN"].SourceVersion = DataRowVersion.Current

                updCommand.Parameters.Add("LENGHT_IN", OracleDbType.Int32, 11, "LENGTH").Direction = ParameterDirection.Input;
                updCommand.Parameters["LENGHT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WIDTH_IN", OracleDbType.Int32, 11, "WIDTH").Direction = ParameterDirection.Input;
                updCommand.Parameters["WIDTH_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("HEIGHT_IN", OracleDbType.Int32, 11, "HEIGHT").Direction = ParameterDirection.Input;
                updCommand.Parameters["HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VOLUME_IN", OracleDbType.Int32, 22, "VOLUME").Direction = ParameterDirection.Input;
                updCommand.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("WEIGHT_IN", OracleDbType.Int32, 11, "WEIGHT").Direction = ParameterDirection.Input;
                updCommand.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("DISMANTING_IN", OracleDbType.Int32, 1, "DISMANTING").Direction = ParameterDirection.Input;
                updCommand.Parameters["DISMANTING_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("NO_OF_UNITS_IN", OracleDbType.Int32, 5, "NO_OF_UNITS").Direction = ParameterDirection.Input;
                updCommand.Parameters["NO_OF_UNITS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MODE_IN", OracleDbType.Int32, 1, "MODE_OF_TRANSPORT").Direction = ParameterDirection.Input;
                updCommand.Parameters["MODE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AREA_DESC_IN", OracleDbType.Varchar2, 50, "AREA_DESCRIPTION").Direction = ParameterDirection.Input;
                updCommand.Parameters["AREA_DESC_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("ELEVATOR_IN", OracleDbType.Int32, 1, "ELEVATOR").Direction = ParameterDirection.Input;
                updCommand.Parameters["ELEVATOR_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PACK_TYPE_FK_IN", OracleDbType.Int32, 10, "PACK_TYPE_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PACK_QTY_IN", OracleDbType.Int32, 7, "pack_qty").Direction = ParameterDirection.Input;
                updCommand.Parameters["PACK_QTY_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("REMARKS_IN", OracleDbType.Varchar2, 250, "REMARKS").Direction = ParameterDirection.Input;
                updCommand.Parameters["REMARKS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters["LAST_MODIFIED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 10, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                //updCommand.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output
                //updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current
                //With delCommand
                //    .Connection = objWK.MyConnection
                //    .CommandType = CommandType.StoredProcedure
                //    .CommandText = objWK.MyUserName & ".FETCH_REM_QTN_DETAILS_PKG.REM_QUT_SURVEY_DEL"
                //    .Parameters.Add("QUOT_SURVEY_DTL_PK_IN", OracleClient.OracleDbType.Int32, 10, "survey_dtl_pk").Direction = ParameterDirection.Input
                //    '.Parameters.Add("RETURN_VALUE", OracleClient.OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output
                //End With

                var _with27 = objWK.MyDataAdapter;
                _with27.InsertCommand = insCommand;
                _with27.InsertCommand.Transaction = TRAN;
                _with27.UpdateCommand = updCommand;
                _with27.UpdateCommand.Transaction = TRAN;
                //.DeleteCommand = delCommand
                //.DeleteCommand.Transaction = TRAN
                RecAfct = _with27.Update(dssurvey);
                if (RecAfct > 0)
                {
                    return "Saved";
                }
                else
                {
                    return "";
                }
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

        /// <summary>
        /// Saves the FRTS.
        /// </summary>
        /// <param name="QTTRNPK">The QTTRNPK.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="dsfright">The dsfright.</param>
        /// <returns></returns>
        public string SaveFrts(string QTTRNPK, OracleTransaction TRAN, DataSet dsfright)
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            try
            {
                objWK.OpenConnection();
                objWK.MyConnection = TRAN.Connection;
                var _with28 = insCommand;
                _with28.Connection = objWK.MyConnection;
                _with28.CommandType = CommandType.StoredProcedure;
                _with28.CommandText = objWK.MyUserName + ".FETCH_REM_QTN_DETAILS_PKG.REM_QUT_FRT_INS";
                _with28.Parameters.Clear();
                insCommand.Parameters.Add("QUOT_TRN_HDR_FK_IN", QTTRNPK).Direction = ParameterDirection.Input;
                insCommand.Parameters["QUOT_TRN_HDR_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("QUOTED_RATE_IN", OracleDbType.Int32, 10, "QUOTED_RATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["QUOTED_RATE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("PYMT_TYPE_IN", OracleDbType.Int32, 1, "PYMT_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("QUOT_TRN_FRT_PAYER_FK_IN", OracleDbType.Int32, 10, "QUOT_TRN_FRT_PAYER_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["QUOT_TRN_FRT_PAYER_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("FRT_OTH_CHRG_FLAG_IN", OracleDbType.Int32, 1, "FRT_OTH_CHRG_FLAG").Direction = ParameterDirection.Input;
                insCommand.Parameters["FRT_OTH_CHRG_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                insCommand.Parameters["CREATED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CREATED_DT_IN", DateTime.Now.Date).Direction = ParameterDirection.Input;
                insCommand.Parameters["CREATED_DT_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "QUOT_TRN_FRT_DTL_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with29 = updCommand;
                _with29.Connection = objWK.MyConnection;
                _with29.CommandType = CommandType.StoredProcedure;
                _with29.CommandText = objWK.MyUserName + ".FETCH_REM_QTN_DETAILS_PKG.REM_QUT_FRT_UPD";
                _with29.Parameters.Clear();
                updCommand.Parameters.Add("QUOT_TRN_FRT_DTL_PK_IN", OracleDbType.Int32, 10, "QUOT_TRN_FRT_DTL_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUOT_TRN_FRT_DTL_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "FREIGHT_ELEMENT_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QUOTED_RATE_IN", OracleDbType.Int32, 10, "QUOTED_RATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUOTED_RATE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("PYMT_TYPE_IN", OracleDbType.Int32, 1, "PYMT_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("QUOT_TRN_FRT_PAYER_FK_IN", OracleDbType.Int32, 10, "QUOT_TRN_FRT_PAYER_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["QUOT_TRN_FRT_PAYER_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("FRT_OTH_CHRG_FLAG_IN", OracleDbType.Int32, 1, "FRT_OTH_CHRG_FLAG").Direction = ParameterDirection.Input;
                updCommand.Parameters["FRT_OTH_CHRG_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                updCommand.Parameters["MODIFIED_BY_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("MODIFIED_DT_IN", DateTime.Now.Date).Direction = ParameterDirection.Input;
                updCommand.Parameters["MODIFIED_DT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 10, "QUOT_TRN_FRT_DTL_PK").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with30 = objWK.MyDataAdapter;
                _with30.InsertCommand = insCommand;
                _with30.InsertCommand.Transaction = TRAN;
                _with30.UpdateCommand = updCommand;
                _with30.UpdateCommand.Transaction = TRAN;
                Int32 RecAfct = default(Int32);
                RecAfct = _with30.Update(dsfright);
                if (RecAfct > 0)
                {
                    return "Saved";
                }
                else
                {
                    return "";
                }
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

        /// <summary>
        /// Fetches the listing.
        /// </summary>
        /// <param name="refno">The refno.</param>
        /// <param name="surref">The surref.</param>
        /// <param name="enqref">The enqref.</param>
        /// <param name="status">The status.</param>
        /// <param name="service">The service.</param>
        /// <param name="partyfk">The partyfk.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="Qtdate">The qtdate.</param>
        /// <param name="dldt">The DLDT.</param>
        /// <param name="mvdt">The MVDT.</param>
        /// <param name="validFrom">The valid from.</param>
        /// <param name="validTo">The valid to.</param>
        /// <param name="plrfk">The PLRFK.</param>
        /// <param name="pfdfk">The PFDFK.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="ChkONLD">The CHK onld.</param>
        /// <returns></returns>
        public DataSet FetchListing(string refno, string surref, string enqref, Int32 status, Int32 service, Int32 partyfk, string SearchType, string Qtdate, string dldt, string mvdt,
        string validFrom, string validTo, string plrfk, string pfdfk, string SortColumn, string SortType, Int32 CurrentPage, Int32 TotalPage, Int32 ChkONLD = 0)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strsql = new StringBuilder();
            string str = " SELECT * FROM (SELECT ROWNUM SLNO, QRY.* FROM ( ";
            string strtop = " SELECT count(*) FROM ( ";
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strsort = "QT.QUOT_DATE ";
            string SrOP = (SearchType == "C" ? "%" : "");
            try
            {
                if (SortColumn == "QTNO")
                {
                    strsort = " QT.QUOT_REF_NR ";
                }
                else if (SortColumn == "PLR")
                {
                    strsort = " PLR.PLACE_CODE ";
                }
                else if (SortColumn == "PFD")
                {
                    strsort = " PFD.PLACE_CODE ";
                }
                else if (SortColumn == "Customer")
                {
                    strsort = " CUST.CUSTOMER_NAME ";
                }
                else if (SortColumn == "Status")
                {
                    strsort = " QT.QUOT_STATUS ";
                }

                strsql.Append(" SELECT QT.QUOT_REF_NR QTNO,QT.QUOT_PK QTPK,TRN.QUOT_TRN_HDR_REF_TYPE QTYPE,TO_CHAR(QT.QUOT_DATE,dateformat) QTDT,PLR.PLACE_CODE PLR,PFD.PLACE_CODE PFD, ");
                strsql.Append(" CUST.CUSTOMER_NAME Customer,DECODE(QT.QUOT_STATUS,1,'Active',2,'Confirm',3,'Cancelled','Approved') STATUS ");
                strsql.Append(" FROM REM_M_QUOT_MST_TBL QT,rem_t_quot_trn_hrd_tbl TRN,REM_M_ENQUIRY_MST_TBL ENQ , REM_M_SURVEY_MST_TBL SUR,PLACE_MST_TBL PLR, PLACE_MST_TBL PFD,CUSTOMER_MST_TBL CUST WHERE 1=1 ");
                strsql.Append(" AND QT.QUOT_PK=TRN.QUOT_FK AND QT.QUOT_ENQ_FK=ENQ.REM_M_ENQ_MST_TBL_PK(+) AND  QT.QUOT_SURVEY_FK=SUR.SURVEY_MST_PK(+) ");
                strsql.Append(" AND QT.QUOT_PLR_FK=PLR.PLACE_PK AND QT.QUOT_PFD_FK=PFD.PLACE_PK AND QT.QUOT_PARTY_FK=CUST.CUSTOMER_MST_PK");
                if (ChkONLD == 0)
                {
                    strsql.Append(" AND 1=2 ");
                }
                if (refno.Length > 0)
                {
                    strsql.Append(" AND QT.QUOT_REF_NR LIKE '" + SrOP + refno.ToUpper().Trim() + "%' ");
                }

                if (enqref.Length > 0)
                {
                    strsql.Append(" AND ENQ.REM_M_ENQ_REF_NR LIKE '" + SrOP + enqref.ToUpper().Trim() + "%' ");
                }

                if (surref.Length > 0)
                {
                    strsql.Append(" AND SUR.survey_number LIKE '" + SrOP + surref.ToUpper().Trim() + "%' ");
                }
                if (status > 0)
                {
                    strsql.Append(" AND QT.QUOT_STATUS=" + status);
                }

                if (service >= 0)
                {
                    strsql.Append(" AND QT.QUOT_MOVE_TYPE = " + service);
                }

                if (Qtdate.Length > 0)
                {
                    strsql.Append(" AND QT.QUOT_DATE = to_date('" + Qtdate + "',dateformat) ");
                }

                if (plrfk.Length > 0)
                {
                    strsql.Append(" AND QT.QUOT_PLR_FK = " + plrfk);
                }

                if (pfdfk.Length > 0)
                {
                    strsql.Append(" AND QT.QUOT_PFD_FK = " + pfdfk);
                }
                if (partyfk > 0)
                {
                    strsql.Append(" AND QT.QUOT_PARTY_FK = " + partyfk);
                }

                if (mvdt.Length > 0)
                {
                    strsql.Append(" AND QT.QUOT_MOVE_DT = to_date('" + mvdt + "',dateformat) ");
                }

                if (dldt.Length > 0)
                {
                    strsql.Append(" AND QT.QUOT_DEL_DT = to_date('" + dldt + "',dateformat) ");
                }

                if (Convert.ToString(getDefault(validFrom, "")).Length > 0 & Convert.ToString(getDefault(validTo, "")).Length > 0)
                {
                    strsql.Append(" and ((to_date('" + validFrom + "' ,'" + dateFormat + "') between to_date(QT.QUOT_VALID_FROM_DT ,'" + dateFormat + "') and to_date(QT.QUOT_VALID_TO_DT ,'" + dateFormat + "') or " + "to_date('" + validFrom + " ','" + dateFormat + "')= to_date(QT.QUOT_VALID_FROM_DT ,'" + dateFormat + "')  or to_date('" + validFrom + "' ,'" + dateFormat + "')= to_date(QT.QUOT_VALID_TO_DT ,'" + dateFormat + "')) or ( " + " to_date('" + validTo + "' ,'" + dateFormat + "') between to_date(QT.QUOT_VALID_FROM_DT ,'" + dateFormat + "') and to_date(QT.QUOT_VALID_TO_DT ,'" + dateFormat + "')" + " or to_date('" + validTo + " ','" + dateFormat + "')= to_date(QT.QUOT_VALID_FROM_DT ,'" + dateFormat + "')  or to_date('" + validTo + "' ,'" + dateFormat + "')= to_date(QT.QUOT_VALID_TO_DT ,'" + dateFormat + "'))) ");
                }
                else if (Convert.ToString(getDefault(validFrom, "")).Length > 0)
                {
                    strsql.Append(" and (to_date('" + validFrom + "' ,'" + dateFormat + "') between to_date(QT.QUOT_VALID_FROM_DT ,'" + dateFormat + "') and to_date(QT.QUOT_VALID_TO_DT ,'" + dateFormat + "') or " + "to_date('" + validFrom + " ','" + dateFormat + "')= to_date(QT.QUOT_VALID_FROM_DT ,'" + dateFormat + "')  or to_date('" + validFrom + "' ,'" + dateFormat + "')= to_date(QT.QUOT_VALID_TO_DT,'" + dateFormat + "')) ");
                }
                else if (Convert.ToString(getDefault(validTo, "")).Length > 0)
                {
                    strsql.Append(" and (to_date('" + validTo + "' ,'" + dateFormat + "') between to_date(QT.QUOT_VALID_FROM_DT ,'" + dateFormat + "') and to_date(QT.QUOT_VALID_TO_DT ,'" + dateFormat + "')" + " or to_date('" + validTo + " ','" + dateFormat + "')= to_date(QT.QUOT_VALID_FROM_DT ,'" + dateFormat + "')  or to_date('" + validTo + "' ,'" + dateFormat + "')= to_date(QT.QUOT_VALID_TO_DT ,'" + dateFormat + "')) ");
                }

                strsql.Append(" Order By " + strsort + SortType);
                strsql.Append("  )QRY ");
                TotalRecords = Convert.ToInt32(objWK.ExecuteScaler(strtop + strsql.ToString()));
                // Getting No of satisfying records.
                TotalPage = TotalRecords / RecordsPerPage;
                if (TotalRecords % RecordsPerPage != 0)
                    TotalPage += 1;
                if (CurrentPage > TotalPage)
                    CurrentPage = 1;
                if (TotalRecords == 0)
                    CurrentPage = 0;
                last = CurrentPage * RecordsPerPage;
                start = (CurrentPage - 1) * RecordsPerPage + 1;
                strsql.Append(" ) MAINQUERY  where SLNO between " + start + " and " + last);
                return objWK.GetDataSet(str + strsql.ToString());
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
    }
}