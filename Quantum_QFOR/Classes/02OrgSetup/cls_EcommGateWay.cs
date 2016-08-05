using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    class cls_EcommGateWay :CommonFeatures
    {
        #region " Fetch Ecomm Details "
        public string FetchEcommDetails(Int32 CurrentPage, Int32 TotalPage, short LoadFlg, string Company = "", string City = "", long LocationFK = 0, long CountryFK = 0, int Status = 0, string FromDate = "", string ToDate = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with1 = objWF.MyDataAdapter;
                _with1.SelectCommand = new OracleCommand();
                _with1.SelectCommand.Connection = objWF.MyConnection;
                _with1.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_ECOMM_GATE_PKG.FETCH_ECOMM";

                _with1.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with1.SelectCommand.Parameters.Add("COMPANY_NAME_IN", OracleDbType.Varchar2).Value = getDefault(Company, DBNull.Value);
                _with1.SelectCommand.Parameters.Add("CITY_IN", OracleDbType.Varchar2).Value = getDefault(City, DBNull.Value);
                _with1.SelectCommand.Parameters.Add("STATUS_IN", OracleDbType.Int32).Value = getDefault(Status, 0);
                _with1.SelectCommand.Parameters.Add("FROM_DATE_IN", OracleDbType.Varchar2).Value = getDefault(FromDate, DBNull.Value);
                _with1.SelectCommand.Parameters.Add("TO_DATE_IN", OracleDbType.Varchar2).Value = getDefault(ToDate, DBNull.Value);
                _with1.SelectCommand.Parameters.Add("COUNTRY_FK_IN", OracleDbType.Int32).Value = getDefault(CountryFK, DBNull.Value);
                _with1.SelectCommand.Parameters.Add("LOCATION_MST_PK_IN", OracleDbType.Int32).Value = getDefault(LocationFK, DBNull.Value);
                _with1.SelectCommand.Parameters.Add("LOAD_FLAG_IN", LoadFlg).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", OracleDbType.Int32).Value = RecordsPerPage;
                _with1.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with1.SelectCommand.Parameters.Add("PL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Fill(ds);
                TotalPage = Convert.ToInt32(_with1.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(_with1.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);
                return JsonConvert.SerializeObject(ds, Formatting.Indented);
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            finally
            {
                objWF.CloseConnection();
            }
            return "";
        }

        public DataSet FetchUserDetails(string CUST_REK_FK, string USER_ID)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with2 = objWF.MyDataAdapter;
                _with2.SelectCommand = new OracleCommand();
                _with2.SelectCommand.Connection = objWF.MyConnection;
                _with2.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_ECOMM_GATE_PKG.FETCH_USER_DETAILS";

                _with2.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with2.SelectCommand.Parameters.Add("CUST_REG_FK_IN", OracleDbType.Varchar2).Value = getDefault(CUST_REK_FK, DBNull.Value);
                _with2.SelectCommand.Parameters.Add("USER_ID_IN", OracleDbType.Varchar2).Value = getDefault(USER_ID, DBNull.Value);
                _with2.SelectCommand.Parameters.Add("PL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Fill(ds);
                return ds;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }
        #endregion

        #region "UPDATE STATUS"
        public ArrayList UpdateStatus(string CustRegFK, string UserId, int STATUS, string Remarks = "")
        {
            WorkFlow objwk = new WorkFlow();
            objwk.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objwk.MyConnection.BeginTransaction();
            arrMessage.Clear();
            try
            {
                var _with3 = objwk.MyCommand;
                _with3.Transaction = TRAN;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objwk.MyUserName + ".FETCH_ECOMM_GATE_PKG.UPDATESTATUS";
                _with3.Parameters.Clear();
                _with3.Parameters.Add("CUST_REG_FK_IN", CustRegFK).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("STATUS_IN", STATUS).Direction = ParameterDirection.Input;
                _with3.Parameters.Add("USER_ID_IN", UserId).Direction = ParameterDirection.Input;
                if (!string.IsNullOrEmpty(Remarks))
                {
                    _with3.Parameters.Add("REMARKS_IN", Remarks.ToString().Trim()).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with3.Parameters.Add("REMARKS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                }
                _with3.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                _with3.ExecuteNonQuery();
                arrMessage.Add("All data Saved Successfully");
                TRAN.Commit();
                return arrMessage;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objwk.CloseConnection();
            }
        }
        #endregion
    }
}
