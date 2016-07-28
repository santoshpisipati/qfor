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
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_DaysCollection : CommonFeatures
    {
        #region " Fetch"

        /// <summary>
        /// Fetches the data.
        /// </summary>
        /// <param name="BankName">Name of the bank.</param>
        /// <param name="AccountNr">The account nr.</param>
        /// <param name="Mode">The mode.</param>
        /// <param name="Status">The status.</param>
        /// <param name="Fromdt">The fromdt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="PartyPK">The party pk.</param>
        /// <param name="LocFK">The loc fk.</param>
        /// <param name="DepositeRefNr">The deposite reference nr.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="Excel">The excel.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurFK">The current fk.</param>
        /// <returns></returns>
        public DataSet FetchData(string BankName = "", string AccountNr = "", string Mode = "", string Status = "", string Fromdt = "", string ToDt = "", string PartyPK = "", string LocFK = "", string DepositeRefNr = "", string SearchType = "",
        int Excel = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, Int32 CurFK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("BANK_NAME_IN", (string.IsNullOrEmpty(BankName) ? "" : BankName)).Direction = ParameterDirection.Input;
                _with1.Add("ACCOUNT_NUMNER_IN", (string.IsNullOrEmpty(AccountNr) ? "" : AccountNr)).Direction = ParameterDirection.Input;
                _with1.Add("MODE_IN", (string.IsNullOrEmpty(Mode) ? "" : Mode)).Direction = ParameterDirection.Input;
                _with1.Add("STATUS_IN", (string.IsNullOrEmpty(Status) ? "" : Status)).Direction = ParameterDirection.Input;
                _with1.Add("FROM_DATE_IN", (string.IsNullOrEmpty(Fromdt) ? "" : Fromdt)).Direction = ParameterDirection.Input;
                _with1.Add("TODATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with1.Add("PARTY_PK_IN", (string.IsNullOrEmpty(PartyPK) ? "" : PartyPK)).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_FK_IN", (string.IsNullOrEmpty(LocFK) ? "" : LocFK)).Direction = ParameterDirection.Input;
                _with1.Add("DEPOSIT_REF_NR", (string.IsNullOrEmpty(DepositeRefNr) ? "" : DepositeRefNr)).Direction = ParameterDirection.Input;
                _with1.Add("SERACH_TYPE_IN", SearchType).Direction = ParameterDirection.Input;
                _with1.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with1.Add("EXCEL_IN", Excel).Direction = ParameterDirection.Input;
                _with1.Add("CURRENCY_IN", CurFK).Direction = ParameterDirection.Input;
                _with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_DAYS_COLLECTION_PKG", "FETCH_DAYS_COLLECTION");
                //TotalPage = objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value;
                //CurrentPage = objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value;
                //if (TotalPage == 0)
                //{
                //    CurrentPage = 0;
                //}
                //else
                //{
                //    CurrentPage = objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value;
                //}
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        /// <summary>
        /// Fetches the summary details.
        /// </summary>
        /// <param name="BankName">Name of the bank.</param>
        /// <param name="AccountNr">The account nr.</param>
        /// <param name="Mode">The mode.</param>
        /// <param name="Status">The status.</param>
        /// <param name="Fromdt">The fromdt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="PartyPK">The party pk.</param>
        /// <param name="LocFK">The loc fk.</param>
        /// <param name="DepositeRefNr">The deposite reference nr.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="Excel">The excel.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="CurFK">The current fk.</param>
        /// <returns></returns>
        public DataSet FetchSummaryDetails(string BankName = "", string AccountNr = "", string Mode = "", string Status = "", string Fromdt = "", string ToDt = "", string PartyPK = "", string LocFK = "", string DepositeRefNr = "", string SearchType = "",
        int Excel = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0, Int32 CurFK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with2 = objWF.MyCommand.Parameters;
                _with2.Add("BANK_NAME_IN", (string.IsNullOrEmpty(BankName) ? "" : BankName)).Direction = ParameterDirection.Input;
                _with2.Add("ACCOUNT_NUMNER_IN", (string.IsNullOrEmpty(AccountNr) ? "" : AccountNr)).Direction = ParameterDirection.Input;
                _with2.Add("MODE_IN", (string.IsNullOrEmpty(Mode) ? "" : Mode)).Direction = ParameterDirection.Input;
                _with2.Add("STATUS_IN", (string.IsNullOrEmpty(Status) ? "" : Status)).Direction = ParameterDirection.Input;
                _with2.Add("FROM_DATE_IN", (string.IsNullOrEmpty(Fromdt) ? "" : Fromdt)).Direction = ParameterDirection.Input;
                _with2.Add("TODATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with2.Add("PARTY_PK_IN", (string.IsNullOrEmpty(PartyPK) ? "" : PartyPK)).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_FK_IN", (string.IsNullOrEmpty(LocFK) ? "" : LocFK)).Direction = ParameterDirection.Input;
                _with2.Add("DEPOSIT_REF_NR", (string.IsNullOrEmpty(DepositeRefNr) ? "" : DepositeRefNr)).Direction = ParameterDirection.Input;
                _with2.Add("SERACH_TYPE_IN", SearchType).Direction = ParameterDirection.Input;
                _with2.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                //_with2.Add("LOC_PK_IN", (CurFK == 0 ? "" : CurFK)).Direction = ParameterDirection.Input;
                _with2.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_DAYS_COLLECTION_PKG", "FETCH_SUMMARY_DETAILS");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion " Fetch"

        #region "Fetch Deposit Details"

        /// <summary>
        /// Fetches the deposit details.
        /// </summary>
        /// <param name="ColPKS">The col PKS.</param>
        /// <param name="ColModePKS">The col mode PKS.</param>
        /// <returns></returns>
        public DataSet FetchDepositDetails(string ColPKS, string ColModePKS)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                var _with3 = objWF.MyCommand.Parameters;
                _with3.Add("COLLECTION_PK_IN", (string.IsNullOrEmpty(ColPKS) ? "" : ColPKS)).Direction = ParameterDirection.Input;
                _with3.Add("COLLECTION_MODE_PK_IN", (string.IsNullOrEmpty(ColModePKS) ? "" : ColModePKS)).Direction = ParameterDirection.Input;
                _with3.Add("BASE_CURRENCY_FK_IN", BaseCurrFk).Direction = ParameterDirection.Input;
                _with3.Add("BASE_CURRENCY_IN", HttpContext.Current.Session["CURRENCY_ID"]).Direction = ParameterDirection.Input;
                _with3.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_DAYS_COLLECTION_PKG", "FETCH_DEPOSIT_DETAILS");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "Fetch Deposit Details"

        #region "Save"

        /// <summary>
        /// Saves the deposit details.
        /// </summary>
        /// <param name="MainDS">The main ds.</param>
        /// <param name="DepositRefNr">The deposit reference nr.</param>
        /// <param name="DepositDt">The deposit dt.</param>
        /// <param name="Remarks">The remarks.</param>
        /// <returns></returns>
        public ArrayList SaveDepositDetails(DataSet MainDS, string DepositRefNr = "", string DepositDt = "", string Remarks = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataRow dr = null;
            OracleCommand OracleCom = new OracleCommand();
            arrMessage.Clear();
            try
            {
                objWF.OpenConnection();
                OracleCom.Connection = objWF.MyConnection;
                OracleCom.CommandType = CommandType.StoredProcedure;

                if (MainDS.Tables[0].Rows.Count > 0)
                {
                    foreach (DataRow dr_loopVariable in MainDS.Tables[0].Rows)
                    {
                        dr = dr_loopVariable;
                        OracleCom.CommandText = objWF.MyUserName + ".DEPOSIT_MST_TBL_PKG.DEPOSIT_MST_TBL_INS";
                        var _with4 = OracleCom.Parameters;
                        _with4.Clear();
                        _with4.Add("COLLECTIONS_MODE_TRN_FK_IN", dr["COLLECTIONS_MODE_TRN_PK"]).Direction = ParameterDirection.Input;
                        _with4.Add("DEPOSIT_REF_NR_IN", (string.IsNullOrEmpty(DepositRefNr) ? "" : DepositRefNr)).Direction = ParameterDirection.Input;
                        _with4.Add("DEPOSIT_REF_DT_IN", (string.IsNullOrEmpty(DepositDt) ? "" : DepositDt)).Direction = ParameterDirection.Input;
                        _with4.Add("DEPOSIT_REMARKS_IN", (string.IsNullOrEmpty(Remarks) ? "" : Remarks)).Direction = ParameterDirection.Input;
                        _with4.Add("DEPOSIT_AMOUNT_IN", dr["DEP_AMT"]).Direction = ParameterDirection.Input;
                        _with4.Add("VERSION_NO_IN", (string.IsNullOrEmpty(dr["VERSION_NO"].ToString()) ? 0 : dr["VERSION_NO"])).Direction = ParameterDirection.Input;
                        _with4.Add("EX_RATE_IN", dr["ROE"]).Direction = ParameterDirection.Input;
                        _with4.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                        _with4.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                        _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        OracleCom.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        OracleCom.ExecuteNonQuery();
                    }
                }
                arrMessage.Add("All Data Saved Successfully");
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
            finally
            {
                objWF.CloseConnection();
            }
        }

        #endregion "Save"

        #region "Get Deposit Reference Details"

        /// <summary>
        /// Fetches the deposit reference details.
        /// </summary>
        /// <param name="ColModePK">The col mode pk.</param>
        /// <returns></returns>
        public DataSet FetchDepositRefDetails(string ColModePK = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                var _with5 = objWF.MyCommand.Parameters;
                _with5.Add("COLLECTION_MODE_PK_IN", (string.IsNullOrEmpty(ColModePK) ? "" : ColModePK)).Direction = ParameterDirection.Input;
                _with5.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_DAYS_COLLECTION_PKG", "FETCH_DEPOSIT_REF_DETIALS");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "Get Deposit Reference Details"

        #region "Get Deposit Remarks Details"

        /// <summary>
        /// Fetches the deposit remarks.
        /// </summary>
        /// <param name="ColModePK">The col mode pk.</param>
        /// <returns></returns>
        public DataSet FetchDepositRemarks(string ColModePK = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
                var _with6 = objWF.MyCommand.Parameters;
                _with6.Add("COLLECTION_MODE_PK_IN", (string.IsNullOrEmpty(ColModePK) ? "" : ColModePK)).Direction = ParameterDirection.Input;
                _with6.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_DAYS_COLLECTION_PKG", "FETCH_DEPOSIT_REMARKS");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "Get Deposit Remarks Details"

        #region "Fill Combo"

        /// <summary>
        /// Fills the name of the bank.
        /// </summary>
        /// <param name="LocPK">The loc pk.</param>
        /// <returns></returns>
        public DataSet FillBankName(string LocPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                ObjWk.OpenConnection();
                ObjWk.MyCommand.Connection = ObjWk.MyConnection;
                var _with7 = ObjWk.MyCommand;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = ObjWk.MyUserName + ".CONSOL_INV_PKG.FETCH_BANK_COLLECTION";
                ObjWk.MyCommand.Parameters.Clear();
                var _with8 = ObjWk.MyCommand.Parameters;
                _with8.Add("LOCATION_PK_IN", LocPK).Direction = ParameterDirection.Input;
                _with8.Add("BANK_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                ObjWk.MyDataAdapter.SelectCommand = ObjWk.MyCommand;
                ObjWk.MyDataAdapter.Fill(dsData);
                return dsData;
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
        /// Fills the name of the acc.
        /// </summary>
        /// <param name="BankPK">The bank pk.</param>
        /// <returns></returns>
        public DataSet FillAccName(string BankPK)
        {
            WorkFlow ObjWk = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                ObjWk.OpenConnection();
                ObjWk.MyCommand.Connection = ObjWk.MyConnection;
                var _with9 = ObjWk.MyCommand;
                _with9.CommandType = CommandType.StoredProcedure;
                _with9.CommandText = ObjWk.MyUserName + ".CONSOL_INV_PKG.FETCH_ACCOUNTNO";
                ObjWk.MyCommand.Parameters.Clear();
                var _with10 = ObjWk.MyCommand.Parameters;
                _with10.Add("BANK_PK_IN", BankPK).Direction = ParameterDirection.Input;
                _with10.Add("BANK_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                ObjWk.MyDataAdapter.SelectCommand = ObjWk.MyCommand;
                ObjWk.MyDataAdapter.Fill(dsData);
                return dsData;
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

        #endregion "Fill Combo"
    }
}