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
    public class cls_MBL_List :CommonFeatures
    {
        public string FetchMBLForJobCard(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            string strLoc = null;
            string agent = "";
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            strJobCardPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                agent = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_MBL_REF_NO_PKG.GET_MBL_REF_JOBCARD";
                var _with1 = SCM.Parameters;
                _with1.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with1.Add("JOB_CARD_PK_IN", getDefault(strJobCardPK, 0)).Direction = ParameterDirection.Input;
                _with1.Add("AGENT_TYPE_IN", (string.IsNullOrEmpty(agent) ? "" : agent)).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = default(OracleClob);
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
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
            finally
            {
                SCM.Connection.Close();
            }
        }

        public string FetchMBLForJobCardImpInvoice(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            string strLoc = null;
            string PROCESS_TYPE_IN = null;
            string agent = "";
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            strJobCardPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                agent = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                PROCESS_TYPE_IN = Convert.ToString(arr.GetValue(5));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_MBL_REF_NO_PKG.GET_MBL_REF_JOBCARD_INVOICE";
                var _with2 = SCM.Parameters;
                _with2.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with2.Add("JOB_CARD_PK_IN", getDefault(strJobCardPK, 0)).Direction = ParameterDirection.Input;
                _with2.Add("AGENT_TYPE_IN", (string.IsNullOrEmpty(agent) ? "" : agent)).Direction = ParameterDirection.Input;
                _with2.Add("PROCESS_TYPE_IN", (string.IsNullOrEmpty(PROCESS_TYPE_IN) ? "" : PROCESS_TYPE_IN)).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = default(OracleClob);
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
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
            finally
            {
                SCM.Connection.Close();
            }
        }

        public string FetchMBLForJobCardExp(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            string strLoc = null;
            string agent = "";
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strJobCardPK = Convert.ToString(arr.GetValue(3));
            //strJobCardPK = arr(3)
            if (arr.Length > 4)
                agent = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_MBL_REF_NO_PKG.GET_MBL_REF_JOBCARD_EXP";
                var _with3 = SCM.Parameters;
                _with3.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with3.Add("JOB_CARD_PK_IN", getDefault(strJobCardPK, 0)).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = default(OracleClob);
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
                //strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
                //Return strReturn
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
            finally
            {
                SCM.Connection.Close();
            }
        }
        public string FetchMBLForJobCardImp(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string strJobCardPK = "";
            string strReq = null;
            string strLoc = null;
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBusiType = Convert.ToString(arr.GetValue(2));
            strJobCardPK = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_MBL_REF_NO_PKG.GET_MBL_REF_JOBCARD_IMP";
                var _with4 = SCM.Parameters;
                _with4.Add("SEARCH_IN", (string.IsNullOrEmpty(strSERACH_IN) ? "" : strSERACH_IN)).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with4.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with4.Add("JOB_CARD_PK_IN", getDefault(strJobCardPK, 0)).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = default(OracleClob);
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
                // strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
                //Return strReturn
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
            finally
            {
                SCM.Connection.Close();
            }
        }
    }
}