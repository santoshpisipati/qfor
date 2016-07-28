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
    public class clsAirlineSchedule : CommonFeatures
    {
        #region "Fetch All Schedules"

        /// <summary>
        /// Fetches all schedules.
        /// </summary>
        /// <param name="Carrier_FK">The carrier_ fk.</param>
        /// <param name="strFlightNo">The string flight no.</param>
        /// <param name="excludeExp">The exclude exp.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <returns></returns>
        public DataSet FetchAllSchedules(string Carrier_FK = "", string strFlightNo = "", Int16 excludeExp = 0, string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long usrLocFK = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = new DataSet();
            try
            {
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("CARRIER_FK_IN", (string.IsNullOrEmpty(Carrier_FK) ? "" : Carrier_FK)).Direction = ParameterDirection.Input;
                _with1.Add("FLIGHT_NO_IN", (string.IsNullOrEmpty(strFlightNo) ? "" : strFlightNo.ToUpper())).Direction = ParameterDirection.Input;
                _with1.Add("EXCLUDE_EXPIRE_IN", excludeExp).Direction = ParameterDirection.Input;
                _with1.Add("SORT_COL_IN", (string.IsNullOrEmpty(SortColumn) ? "" : SortColumn)).Direction = ParameterDirection.Input;
                _with1.Add("USER_LOC_FK_IN", (usrLocFK == 0 ? 0 : usrLocFK)).Direction = ParameterDirection.Input;
                _with1.Add("PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("AIRLINE_SCHEDULE_PKG", "FETCH_AIRLINE_SCHEDULE_LIST");
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                return dsAll;
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

        #endregion "Fetch All Schedules"

        #region "FetchAirlineSchedDatatoExport"

        /// <summary>
        /// Fetches the airline sched datato export.
        /// </summary>
        /// <param name="Carrier_FK">The carrier_ fk.</param>
        /// <param name="strFlightNo">The string flight no.</param>
        /// <param name="excludeExp">The exclude exp.</param>
        /// <returns></returns>
        public DataSet FetchAirlineSchedDatatoExport(string Carrier_FK = "", string strFlightNo = "", Int16 excludeExp = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet MainDS = new DataSet();
            try
            {
                objWF.MyDataAdapter = new OracleDataAdapter();
                objWF.MyDataAdapter.SelectCommand = new OracleCommand();
                objWF.MyDataAdapter.SelectCommand.Connection = objWF.MyConnection;
                objWF.MyDataAdapter.SelectCommand.CommandText = objWF.MyUserName + ".AIRLINE_SCHEDULE_PKG.FETCH_AIRLINE_SCHED_TO_EXPORT";
                objWF.MyDataAdapter.SelectCommand.CommandType = CommandType.StoredProcedure;
                objWF.MyDataAdapter.SelectCommand.Parameters.Add("CARRIER_FK_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(Carrier_FK) ? "" : Carrier_FK);
                objWF.MyDataAdapter.SelectCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(strFlightNo) ? "" : strFlightNo.ToUpper());
                objWF.MyDataAdapter.SelectCommand.Parameters.Add("EXCLUDE_EXPIRE_IN", OracleDbType.Varchar2).Value = excludeExp;
                objWF.MyDataAdapter.SelectCommand.Parameters.Add("FLIGHT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWF.MyDataAdapter.SelectCommand.Parameters.Add("SCHED_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWF.MyDataAdapter.Fill(MainDS);
                DataRelation relAirlineSched = new DataRelation("AIRLINESCHED", new DataColumn[] { MainDS.Tables[0].Columns["AIRLINE_SCHEDULE_MST_PK"] }, new DataColumn[] { MainDS.Tables[1].Columns["AIRLINE_SCHEDULE_MST_FK"] });
                relAirlineSched.Nested = true;
                MainDS.Relations.Add(relAirlineSched);
                return MainDS;
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

        #endregion "FetchAirlineSchedDatatoExport"

        #region "Fetch Airline Schedules"

        /// <summary>
        /// Fetches the airline schedule.
        /// </summary>
        /// <param name="airlineSchedMstPk">The airline sched MST pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <returns></returns>
        public DataTable FetchAirlineSchedule(long airlineSchedMstPk = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = new DataSet();
            DataTable dtAll = null;
            try
            {
                var _with2 = objWF.MyCommand.Parameters;
                _with2.Add("AIRLINE_SCHED_MST_PK", (airlineSchedMstPk == 0 ? 0 : airlineSchedMstPk)).Direction = ParameterDirection.Input;
                _with2.Add("PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("AIRLINE_SCHEDULE_PKG", "FETCH_AIRLINE_SCHEDULE_TRN");
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                dtAll = dsAll.Tables[0];
                dsAll.Tables.RemoveAt(0);
                return dtAll;
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

        #endregion "Fetch Airline Schedules"

        #region "Fetch Airline Schedule Details"

        /// <summary>
        /// Fetches the airline sched MST detail.
        /// </summary>
        /// <param name="airlineSchedMstPk">The airline sched MST pk.</param>
        /// <param name="SCHEDULE">The schedule.</param>
        /// <param name="AOO_FK">The ao o_ fk.</param>
        /// <param name="AOD_FK">The ao d_ fk.</param>
        /// <param name="CARRIER_FK">The carrie r_ fk.</param>
        /// <param name="FLIGHT_NO">The fligh t_ no.</param>
        /// <returns></returns>
        public DataSet FetchAirlineSchedMstDetail(long airlineSchedMstPk = 0, string SCHEDULE = "", long AOO_FK = 0, long AOD_FK = 0, long CARRIER_FK = 0, string FLIGHT_NO = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = new DataSet();
            try
            {
                var _with3 = objWF.MyCommand.Parameters;
                _with3.Add("AIRLINE_SCHED_MST_PK_IN", (airlineSchedMstPk == 0 ? 0 : airlineSchedMstPk)).Direction = ParameterDirection.Input;
                _with3.Add("AOO_FK_IN", (AOO_FK == 0 ? 0 : AOO_FK)).Direction = ParameterDirection.Input;
                _with3.Add("AOD_FK_IN", (AOD_FK == 0 ? 0 : AOD_FK)).Direction = ParameterDirection.Input;
                _with3.Add("CARRIER_FK_IN", (CARRIER_FK == 0 ? 0 : CARRIER_FK)).Direction = ParameterDirection.Input;
                _with3.Add("FLIGHT_NO_IN", (string.IsNullOrEmpty(FLIGHT_NO) ? "" : FLIGHT_NO)).Direction = ParameterDirection.Input;
                _with3.Add("SCHEDULE_IN", (string.IsNullOrEmpty(SCHEDULE) ? "" : SCHEDULE)).Direction = ParameterDirection.Input;
                _with3.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("AIRLINE_SCHEDULE_PKG", "FETCH_AIRLINE_SCHED_MST_DET");
                return dsAll;
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

        #endregion "Fetch Airline Schedule Details"

        #region "DeleteAirlineSchedule"

        /// <summary>
        /// Deletes the airline schedule.
        /// </summary>
        /// <param name="AirlineSchedMstPK">The airline sched MST pk.</param>
        /// <param name="Version_No">The version_ no.</param>
        /// <param name="ConfigPk">The configuration pk.</param>
        /// <returns></returns>
        public ArrayList DeleteAirlineSchedule(long AirlineSchedMstPK, short Version_No, long ConfigPk)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            int RESULT = 0;
            string return_Value = null;
            try
            {
                OracleCommand DelCommand = new OracleCommand();
                Int16 VER = default(Int16);
                var _with4 = DelCommand;
                _with4.Connection = objWK.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWK.MyUserName + ".AIRLINE_SCHEDULE_PKG.AIRLINE_SCHEDULE_TBL_DEL";
                _with4.Parameters.Add("AIRLINE_SCHEDULE_MST_PK_IN", AirlineSchedMstPK).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("DELETED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("VERSION_NO_IN", Version_No).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("CONFIG_MST_FK_IN", ConfigPk).Direction = ParameterDirection.Input;
                _with4.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                RESULT = _with4.ExecuteNonQuery();
                return_Value = Convert.ToString(_with4.Parameters["RETURN_VALUE"].Value);
                _with4.Parameters.Clear();
                arrMessage.Add(return_Value);
                return arrMessage;
            }
            catch (OracleException OraExp)
            {
                arrMessage.Add(OraExp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "DeleteAirlineSchedule"

        #region "DeleteAirlineSchedTrn"

        /// <summary>
        /// Deletes the airline sched TRN.
        /// </summary>
        /// <param name="TRAN">The tran.</param>
        /// <param name="AirlineSchedTrnPKs">The airline sched TRN p ks.</param>
        /// <param name="Version_No">The version_ no.</param>
        /// <param name="ConfigPk">The configuration pk.</param>
        /// <param name="AirlineSchedMstPk">The airline sched MST pk.</param>
        /// <returns></returns>
        public ArrayList DeleteAirlineSchedTrn(OracleTransaction TRAN, string AirlineSchedTrnPKs, short Version_No, long ConfigPk, long AirlineSchedMstPk)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.MyConnection = TRAN.Connection;
            int RESULT = 0;
            string return_Value = null;
            try
            {
                OracleCommand DelCommand = new OracleCommand();
                Int16 VER = default(Int16);
                var _with5 = DelCommand;
                _with5.Transaction = TRAN;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".AIRLINE_SCHEDULE_PKG.AIRLINE_SCHEDULE_TRN_DEL";
                _with5.Parameters.Add("AIRLINE_SCHEDULE_TRN_PK_IN", AirlineSchedTrnPKs).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("DELETED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("VERSION_NO_IN", Version_No).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("CONFIG_MST_FK_IN", ConfigPk).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("AIRLINE_SCHEDULE_MST_FK_IN", AirlineSchedMstPk).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                RESULT = _with5.ExecuteNonQuery();
                return_Value = Convert.ToString(_with5.Parameters["RETURN_VALUE"].Value);
                _with5.Parameters.Clear();
                return arrMessage;
            }
            catch (OracleException OraExp)
            {
                arrMessage.Add(OraExp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
        }

        #endregion "DeleteAirlineSchedTrn"

        #region "Validate Airline Schedule"

        /// <summary>
        /// Validates the airline schedule.
        /// </summary>
        /// <param name="CarrierFk">The carrier fk.</param>
        /// <param name="AOO_FK">The ao o_ fk.</param>
        /// <param name="AOD_FK">The ao d_ fk.</param>
        /// <param name="FLIGHT_NO">The fligh t_ no.</param>
        /// <param name="VALIDFROMDT">The validfromdt.</param>
        /// <param name="VALIDTODT">The validtodt.</param>
        /// <returns></returns>
        public string ValidateAirlineSchedule(long CarrierFk, long AOO_FK, long AOD_FK, string FLIGHT_NO, string VALIDFROMDT, string VALIDTODT)
        {
            string strMsg = "";
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = new DataSet();
            DataTable dtAll = null;
            int RESULT = 0;
            OracleCommand cmd = new OracleCommand();
            objWF.OpenConnection();
            try
            {
                var _with9 = cmd;
                _with9.Connection = objWF.MyConnection;
                _with9.CommandType = CommandType.StoredProcedure;
                _with9.CommandText = objWF.MyUserName + ".AIRLINE_SCHEDULE_PKG.VALIDATE_AIRLINE_SCHEDULE";
                _with9.Parameters.Add("CARRIER_FK_IN", CarrierFk).Direction = ParameterDirection.Input;
                _with9.Parameters.Add("FLIGHT_NO_IN", FLIGHT_NO).Direction = ParameterDirection.Input;
                _with9.Parameters.Add("AOD_FK_IN", AOD_FK).Direction = ParameterDirection.Input;
                _with9.Parameters.Add("AOO_FK_IN", AOO_FK).Direction = ParameterDirection.Input;
                _with9.Parameters.Add("VALID_FROM_IN", VALIDFROMDT).Direction = ParameterDirection.Input;
                _with9.Parameters.Add("VALID_TO_IN", VALIDTODT).Direction = ParameterDirection.Input;
                _with9.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                RESULT = _with9.ExecuteNonQuery();
                strMsg = Convert.ToString(_with9.Parameters["RETURN_VALUE"].Value);
                _with9.Parameters.Clear();
                return strMsg;
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
            finally
            {
                objWF.CloseConnection();
            }
        }

        #endregion "Validate Airline Schedule"

        #region "Fetch Operator ID"

        /// <summary>
        /// Fetches the operator identifier.
        /// </summary>
        /// <param name="OperID">The oper identifier.</param>
        /// <returns></returns>
        public DataSet fetchOperatorID(string OperID)
        {
            WorkFlow objWF = new WorkFlow();
            string strQuery = null;
            string strSQL = null;
            strSQL = "SELECT AMT.ACTIVE_FLAG,";
            strSQL = strSQL + "  AMT.AIRLINE_ID,";
            strSQL = strSQL + "  AMT.AIRLINE_NAME,AMT.AIRLINE_MST_PK";
            strSQL = strSQL + "        FROM AIRLINE_MST_TBL     AMT";
            strSQL = strSQL + "  Where AMT.AIRLINE_ID = '" + OperID + "' ";
            try
            {
                return objWF.GetDataSet(strSQL);
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

        #endregion "Fetch Operator ID"

        #region "GettingPOLANDPODPk"

        /// <summary>
        /// Gettings the port pk.
        /// </summary>
        /// <param name="PORID">The porid.</param>
        /// <returns></returns>
        public long GettingPortPk(string PORID)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT PMT.PORT_MST_PK FROM PORT_MST_TBL PMT WHERE PMT.BUSINESS_TYPE = 1 AND PMT.PORT_ID  = '" + PORID.ToUpper() + "'");
            try
            {
                return Convert.ToInt64(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "GettingPOLANDPODPk"

        #region "GettingPOLANDPODPk"

        /// <summary>
        /// Gettings the name of the port.
        /// </summary>
        /// <param name="PORID">The porid.</param>
        /// <returns></returns>
        public string GettingPortName(string PORID)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT PMT.PORT_NAME FROM PORT_MST_TBL PMT WHERE PMT.BUSINESS_TYPE = 1 AND PMT.PORT_ID  = '" + PORID.ToUpper() + "'");
            try
            {
                return objWF.ExecuteScaler(sb.ToString());
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

        #endregion "GettingPOLANDPODPk"
    }
}