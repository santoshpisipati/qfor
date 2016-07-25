using Oracle.DataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    public class clsRegionDetails : CommonFeatures
    {
        public string FetchLocationByNameForCountry(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strReq = null;
            int CountryPk = 0;
            dynamic strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            CountryPk = Convert.ToInt32(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_LOCATION_PKG.GETLOC_BY_LOCNAME_FOR_COUNTRY";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with1.Add("COUNTRY_PK_IN", CountryPk).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.Varchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (System.Data.OracleClient.OracleException OraExp)
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
    }
}