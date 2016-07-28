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
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsCustCreditInwFromSage : CommonFeatures
    {
        #region " Validate Credit Customer "

        /// <summary>
        /// Validates the credit customer.
        /// </summary>
        /// <param name="CustId">The customer identifier.</param>
        /// <returns></returns>
        public Int32 ValidateCreditCust(string CustId)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                string strSQL = "";

                strSQL = string.Empty;
                strSQL += " SELECT COUNT(CREDIT_CUSTOMER) CNT FROM CUSTOMER_MST_TBL WHERE CUSTOMER_ID = '" + CustId + "' ";
                strSQL += " AND CREDIT_CUSTOMER = 1 ";

                return Convert.ToInt32(objWK.ExecuteScaler(strSQL));
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

        #endregion " Validate Credit Customer "

        #region " clsCustCreditInwFromSage "
        public DataSet Save(ref DataSet dsSave, ref DataSet dsErrorLog, ref int inc)
        {

            WorkFlow objWK = new WorkFlow();
            DataRow dr = null;
            objWK.OpenConnection();
            int Cnt = 0;
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            try
            {
                var _with1 = objWK.MyCommand;
                _with1.CommandText = objWK.MyUserName + ".CUST_CREDIT_UPD_FROM_SAGE_PKG.Update_Cust_Credit";
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.Transaction = TRAN;
                var _with2 = _with1.Parameters;
                for (Cnt = 0; Cnt <= dsSave.Tables[0].Rows.Count - 1; Cnt++)
                {
                    _with2.Clear();
                    _with2.Add("CUSTOMER_ID_IN", Convert.ToString(dsSave.Tables[0].Rows[Cnt]["CustId"])).Direction = ParameterDirection.Input;
                    _with2.Add("CUST_BALANCE_DUE_IN", Convert.ToDouble(dsSave.Tables[0].Rows[Cnt]["Balance"])).Direction = ParameterDirection.Input;
                    _with2.Add("CUST_CREDIT_LIMIT_IN", Convert.ToDouble(dsSave.Tables[0].Rows[Cnt]["CreditLimit"])).Direction = ParameterDirection.Input;
                    _with2.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    objWK.MyCommand.ExecuteNonQuery();
                    if (string.Compare(objWK.MyCommand.Parameters["RETURN_VALUE"].Value.ToString(), "Customer")>0)
                    {
                        dr = dsErrorLog.Tables[0].NewRow();
                        dr["SNo"] = inc;
                        //1
                        dr["CustId"] = Convert.ToString(dsSave.Tables[0].Rows[Cnt]["CustId"]);
                        dr["Balance"] = Convert.ToDouble(dsSave.Tables[0].Rows[Cnt]["Balance"]);
                        dr["CreditLimit"] = Convert.ToDouble(dsSave.Tables[0].Rows[Cnt]["CreditLimit"]);
                        dsErrorLog.Tables[0].Rows.Add(dr);
                        inc += 1;
                    }
                }
                TRAN.Commit();
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new DataSet();
        }
        #endregion
       
    }


}