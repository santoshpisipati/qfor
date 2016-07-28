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
    public class cls_InvoiceListSea : CommonFeatures
    {
        #region "Enhance Search"
        public string FetchInvoice(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strLoc = null;
            strLoc = loc;
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBizType = Convert.ToString(arr.GetValue(2));
            //If arr.Length > 1 Then strReq = arr(0)
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_INV_REF_NO_PKG.GET_INV_REF_COMMON";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
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
        public string FetchCustomerForJobCard(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strJobPK = "";
            string strReq = null;
            string strLoc = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strJobPK = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLoc = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCUSTOMER_JOBCARD";

                var _with2 = selectCommand.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLoc) ? strLoc : "")).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with2.Add("JOB_CARD_PK_IN", getDefault(strJobPK, 0)).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
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
        public string FetchVoyageForJobCard(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = null;
            string strBizType = null;
            string strJobPK = null;
            string strReq = null;
            string strLoc = null;
            var strNull = DBNull.Value;
            string SEARCH_FLAG_IN = "";
            strLoc = loc;
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strVES = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strVOY = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strJobPK = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 6)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(6));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.GET_VES_VOY_JOBCARD";

                var _with3 = selectCommand.Parameters;
                _with3.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with3.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", getDefault(strReq, DBNull.Value)).Direction = ParameterDirection.Input;
                //.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input
                _with3.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLoc) ? strLoc : "")).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", getDefault(strBizType, 3)).Direction = ParameterDirection.Input;
                _with3.Add("JOB_CARD_PK_IN", getDefault(strJobPK, 0)).Direction = ParameterDirection.Input;
                _with3.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? "" : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                //strReturn = CStr(selectCommand.Parameters("RETURN_VALUE").Value)
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
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
        public string FetchFleightForInvDP(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = null;
            string strBizType = null;
            string strJobPK = null;
            string strReq = null;
            string strLoc = null;
            string AGENT_TYPE_IN = null;
            string PROCESS_TYPE_IN = null;
            var strNull = DBNull.Value;
            string SEARCH_FLAG_IN = "";
            strLoc = loc;
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strVES = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strVOY = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strJobPK = Convert.ToString(arr.GetValue(4));
            //  If arr.Length > 6 Then SEARCH_FLAG_IN = arr(6)
            if (arr.Length > 6)
                PROCESS_TYPE_IN = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 5)
                AGENT_TYPE_IN = Convert.ToString(arr.GetValue(5));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.GET_FLEIGHT_INVOICE";

                var _with4 = selectCommand.Parameters;
                _with4.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with4.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", getDefault(strReq, DBNull.Value)).Direction = ParameterDirection.Input;
                //.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input
                _with4.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLoc) ? strLoc : "")).Direction = ParameterDirection.Input;
                _with4.Add("BUSINESS_TYPE_IN", getDefault(strBizType, 3)).Direction = ParameterDirection.Input;
                _with4.Add("JOB_CARD_PK_IN", getDefault(strJobPK, 0)).Direction = ParameterDirection.Input;
                _with4.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? "" : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with4.Add("PROCESS_TYPE_IN", (string.IsNullOrEmpty(PROCESS_TYPE_IN) ? "" : PROCESS_TYPE_IN)).Direction = ParameterDirection.Input;
                _with4.Add("AGENT_TYPE_IN", (string.IsNullOrEmpty(AGENT_TYPE_IN) ? "" : AGENT_TYPE_IN)).Direction = ParameterDirection.Input;
                //.Add("RETURN_VALUE", OracleDbType.Varchar2, 32767, "RETURN_VALUE").Direction = ParameterDirection.Output
                _with4.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)selectCommand.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
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
        public string FetchVOYAGE_FLIGHT_NOForJobCardNew(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strVES = null;
            string strVOY = null;
            string strBizType = null;
            string strJobPK = null;
            string strReq = null;
            string strLoc = null;
            var strNull = DBNull.Value;
            string SEARCH_FLAG_IN = "";
            strLoc = loc;
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strVES = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strVOY = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBizType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strJobPK = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 6)
                SEARCH_FLAG_IN = Convert.ToString(arr.GetValue(6));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.GET_VES_VOY_JOBCARD_NEW";

                var _with5 = selectCommand.Parameters;
                _with5.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with5.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with5.Add("LOOKUP_VALUE_IN", getDefault(strReq, DBNull.Value)).Direction = ParameterDirection.Input;
                //.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input
                _with5.Add("LOCATION_IN", (!string.IsNullOrEmpty(strLoc) ? strLoc : "")).Direction = ParameterDirection.Input;
                _with5.Add("BUSINESS_TYPE_IN", getDefault(strBizType, 3)).Direction = ParameterDirection.Input;
                _with5.Add("JOB_CARD_PK_IN", getDefault(strJobPK, 0)).Direction = ParameterDirection.Input;
                _with5.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? "" : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", OracleDbType.Varchar2, 32767, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
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
        public string FetchJobCardForInvoice(string strCond, double loc = 0)
        {
            //loc is used for log in location 
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string custpk = null;
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                custpk = Convert.ToString(arr.GetValue(3));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_JOB_REF_NO_PKG.GET_ACTIVE_JOB_REF_FOR_INV";

                var _with6 = selectCommand.Parameters;
                _with6.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                //LOCATION_IN
                _with6.Add("LOCATION_IN", loc).Direction = ParameterDirection.Input;
                //
                _with6.Add("CUSTOMER_PK_IN", getDefault(custpk, DBNull.Value)).Direction = ParameterDirection.Input;
                //
                _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
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

        #region "Fetch All"
        public DataSet FetchAll(string strInvPK = "", string strJobPK = "", string strHBLPK = "", string strMBLPK = "", string strCustPK = "", string strVessel = "", string strVoyage = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0,
        string SortType = " DESC ", long usrLocFK = 0, Int32 flag = 0)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strCondition = "FROM ";
            strCondition += "INV_CUST_SEA_EXP_TBL INV,";
            strCondition += "JOB_CARD_TRN JOB,";
            strCondition += "HBL_EXP_TBL HBL,";
            strCondition += "MBL_EXP_TBL MBL,";
            strCondition += "CUSTOMER_MST_TBL CMT,";
            strCondition += "CURRENCY_TYPE_MST_TBL CUMT,";
            strCondition += "USER_MST_TBL UMT ";
            strCondition += "WHERE";
            strCondition += "INV.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK";
            strCondition += "AND JOB.HBL_HAWB_FK=HBL.HBL_EXP_TBL_PK (+)";
            strCondition += "AND JOB.MBL_MAWB_FK=MBL.MBL_EXP_TBL_PK (+)";
            strCondition += "AND JOB.SHIPPER_CUST_MST_FK=CMT.CUSTOMER_MST_PK (+)";
            strCondition += "AND INV.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";
            strCondition += "AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ";
            strCondition += "AND INV.CREATED_BY_FK = UMT.USER_MST_PK ";
            //If BlankGrid = 0 Then
            //    strCondition &= vbCrLf & " AND 1=2 "
            //End If
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (!string.IsNullOrEmpty(strInvPK.Trim()))
            {
                strCondition += "AND UPPER(INV.INVOICE_REF_NO) LIKE '%" + strInvPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strJobPK.Trim()))
            {
                strCondition += "AND UPPER(JOB.JOBCARD_REF_NO) LIKE '%" + strJobPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strHBLPK.Trim()))
            {
                strCondition += "AND UPPER(HBL.HBL_REF_NO) LIKE '%" + strHBLPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strMBLPK.Trim()))
            {
                strCondition += "AND UPPER(MBL.MBL_REF_NO) LIKE '%" + strMBLPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strCustPK.Trim()))
            {
                strCondition += "AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + strCustPK.Trim() + "%'";
            }

            if (!string.IsNullOrEmpty(strVessel.Trim()))
            {
                strCondition += "AND JOB.VESSEL_NAME LIKE '%" + strVessel.Trim() + "%'";
            }

            if (!string.IsNullOrEmpty(strVoyage.Trim()))
            {
                strCondition += "AND JOB.VOYAGE_FLIGHT_NO LIKE '%" + strVoyage.Trim() + "%'";
            }


            string strCount = null;
            strCount = "SELECT COUNT(*)  ";
            strCount += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;


            strSQL = " SELECT * FROM (";
            strSQL += "SELECT ROWNUM SR_NO, q.* FROM ";
            strSQL += "( SELECT ";
            strSQL += "INV.INV_CUST_SEA_EXP_PK,";
            strSQL += "INV.JOB_CARD_SEA_EXP_FK,";
            strSQL += "JOB.HBL_HAWB_FK,";
            strSQL += "JOB.MBL_MAWB_FK,";
            strSQL += "JOB.SHIPPER_CUST_MST_FK,";
            strSQL += "INV.CURRENCY_MST_FK,";
            strSQL += "INV.INVOICE_REF_NO,";
            strSQL += "INV.INVOICE_DATE AS INVDATE,";
            strSQL += "JOB.JOBCARD_REF_NO,";
            strSQL += "HBL.HBL_REF_NO,";
            strSQL += "MBL.MBL_REF_NO,";
            strSQL += "CMT.CUSTOMER_NAME,";
            strSQL += "(CASE WHEN (NVL(JOB.VESSEL_NAME,'') || '/' || NVL(JOB.VOYAGE_FLIGHT_NO,'')='/') THEN '' ELSE NVL(JOB.VESSEL_NAME,'') || '/' || NVL(JOB.VOYAGE_FLIGHT_NO,'') END) AS VESVOYAGE,";
            strSQL += "CUMT.CURRENCY_ID,";
            strSQL += "INV.NET_PAYABLE";
            strSQL += strCondition;
            if (SortColumn == "INVDATE")
            {
                SortColumn = "INV.INVOICE_DATE";
            }
            else if (SortColumn == "VESVOYAGE")
            {
                SortColumn = "JOB.VESSEL_NAME,JOB.VOYAGE_FLIGHT_NO";
            }
            strSQL += " ORDER BY " + SortColumn + SortType + "  ,INVOICE_REF_NO DESC  ) q  ) ";
            strSQL += " WHERE SR_NO  BETWEEN " + start + " AND " + last;

            DataSet DS = null;
            DS = objWF.GetDataSet(strSQL);
            try
            {
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
        public DataSet FetchUninvoicedJC(string strJobPK = "", string strHBLPK = "", string strMBLPK = "", string strCustPK = "", string strVessel = "", string strVoyage = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ",
        long usrLocFK = 0, Int32 flag = 0)
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            strCondition = "FROM ";
            strCondition += "JOB_CARD_TRN JOB,";
            strCondition += "HBL_EXP_TBL HBL,";
            strCondition += "MBL_EXP_TBL MBL,";
            strCondition += "CUSTOMER_MST_TBL CMT,";
            strCondition += "USER_MST_TBL UMT ";
            strCondition += "WHERE JOB.JOB_CARD_STATUS=1";

            strCondition += "AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + "";
            strCondition += " AND JOB.CREATED_BY_FK = UMT.USER_MST_PK ";

            strCondition += "AND JOB.HBL_HAWB_FK=HBL.HBL_EXP_TBL_PK (+)";
            strCondition += "AND JOB.MBL_MAWB_FK=MBL.MBL_EXP_TBL_PK (+)";
            strCondition += "AND JOB.SHIPPER_CUST_MST_FK=CMT.CUSTOMER_MST_PK (+)";

            if (!string.IsNullOrEmpty(strJobPK.Trim()))
            {
                strCondition += "AND UPPER(JOB.JOBCARD_REF_NO) LIKE '%" + strJobPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strHBLPK.Trim()))
            {
                strCondition += "AND UPPER(HBL.HBL_REF_NO) LIKE '%" + strHBLPK.Trim() + "%'";
            }

            if (!string.IsNullOrEmpty(strMBLPK.Trim()))
            {
                strCondition += "AND UPPER(MBL.MBL_REF_NO) LIKE '%" + strMBLPK.Trim() + "%'";
            }

            if (!string.IsNullOrEmpty(strCustPK.Trim()))
            {
                strCondition += "AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + strCustPK.Trim() + "%'";
            }

            if (!string.IsNullOrEmpty(strVessel.Trim()))
            {
                strCondition += "AND JOB.VESSEL_NAME LIKE '%" + strVessel.Trim() + "%'";
            }

            if (!string.IsNullOrEmpty(strVoyage.Trim()))
            {
                strCondition += "AND JOB.VOYAGE_FLIGHT_NO LIKE '%" + strVoyage.Trim() + "%'";
            }

            strSQL = " SELECT SR_NO,INV_CUST_SEA_EXP_PK,JOB_CARD_SEA_EXP_FK,HBL_HAWB_FK,";
            strSQL += "MBL_MAWB_FK,SHIPPER_CUST_MST_FK,CURRENCY_MST_FK,";
            strSQL += "INVOICE_REF_NO,INVDATE,JOBCARD_REF_NO,HBL_REF_NO,";
            strSQL += "MBL_REF_NO,CUSTOMER_NAME, VESVOYAGE, CURRENCY_ID, NET_PAYABLE";
            strSQL += "FROM (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "(SELECT * FROM  ( SELECT ";
            strSQL += "0 INV_CUST_SEA_EXP_PK,";
            strSQL += "JOB.JOB_CARD_TRN_PK AS JOB_CARD_SEA_EXP_FK,";
            strSQL += "JOB.HBL_HAWB_FK,";
            strSQL += "JOB.MBL_MAWB_FK,";
            strSQL += "JOB.SHIPPER_CUST_MST_FK,";
            strSQL += "0 CURRENCY_MST_FK,";
            strSQL += "'' INVOICE_REF_NO,";
            strSQL += "NULL AS INVDATE,";
            strSQL += "JOB.JOBCARD_REF_NO,";
            strSQL += "HBL.HBL_REF_NO,";
            strSQL += "MBL.MBL_REF_NO,";
            strSQL += "CMT.CUSTOMER_NAME,";
            strSQL += "(CASE WHEN JOB.VESSEL_NAME IS NOT NULL AND JOB.VOYAGE_FLIGHT_NO IS NOT NULL AND JOB.VESSEL_NAME <> '' AND JOB.VOYAGE_FLIGHT_NO <> '' THEN JOB.VESSEL_NAME || '/' || JOB.VOYAGE_FLIGHT_NO ELSE JOB.VESSEL_NAME END) AS VESVOYAGE,";
            strSQL += "'' CURRENCY_ID,";
            strSQL += "NULL NET_PAYABLE,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_PIA PIA WHERE PIA.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK AND PIA.INV_CUST_TRN_FK IS NULL AND PIA.INV_AGENT_TRN_FK IS NULL) AS PC,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_FD FD WHERE FD.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK AND FD.INVOICE_TBL_FK IS NULL AND FD.INV_AGENT_TRN_FK IS NULL) AS FC,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_OTH_CHRG OTH WHERE OTH.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK AND OTH.INV_CUST_TRN_FK IS NULL AND OTH.INV_AGENT_TRN_FK IS NULL) AS OC,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_PIA PIA WHERE PIA.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK) AS PC1,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_FD FD WHERE FD.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK) AS FC1,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_OTH_CHRG OTH WHERE OTH.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK) AS OC1";
            strSQL += strCondition;
            if (SortColumn == "INVDATE")
            {
                // SortColumn = "INV.INVOICE_DATE"
            }
            else if (SortColumn == "VESVOYAGE")
            {
                SortColumn = "JOB.VESSEL_NAME,JOB.VOYAGE_FLIGHT_NO";
            }
            strSQL += " ORDER BY " + SortColumn + SortType + ")) q  ";
            strSQL += " WHERE  (PC>0 OR FC>0 OR OC>0 OR (PC1=0 AND FC1=0 AND OC1=0)))";

            string strCount = null;
            strCount = "SELECT COUNT(*) FROM ( ";
            strCount += strSQL;
            strCount += ")";
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount));
            TotalPage = TotalRecords / RecordsPerPage;
            if (TotalRecords % RecordsPerPage != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;
            last = CurrentPage * RecordsPerPage;
            start = (CurrentPage - 1) * RecordsPerPage + 1;

            strSQL += " WHERE SR_NO  BETWEEN " + start + " AND " + last + " ";
            DataSet DS = null;
            DS = objWF.GetDataSet(strSQL);
            try
            {
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
        #endregion

        #region "Invoice To Customer -- Exports Sea --  Reports Section"
        public DataSet FetchInvToCustExpSeaMain(Int32 InvPK)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Strsql = "SELECT INVCUSTEXP.CONSOL_INVOICE_PK INVPK," ;
            Strsql += "INVCUSTEXP.INVOICE_REF_NO       INVREFNO," ;

            Strsql += "NVL(INVCUSTEXP.DISCOUNT_AMT,0)         DICSOUNT," ;
            Strsql += "INVTAGTEXP.TAX_PCNT             VATPCT," ;
            Strsql += "INVTAGTEXP.TAX_AMT              VATAMT," ;
            Strsql += "JSE.JOB_CARD_TRN_PK        JOBPK," ;
            Strsql += "JSE.JOBCARD_REF_NO             JOBREFNO," ;
            Strsql += "'' CLEARANCEPOINT," ;
            Strsql += "JSE.ETD_DATE ETD," ;
            Strsql += "JSE.ETA_DATE ETA," ;
            Strsql += "BAT.CARGO_TYPE CARGOTYPE, " ;
            Strsql += "SHIPMST.CUSTOMER_NAME    SHIPPER," ;
            Strsql += "BAT.CUSTOMER_REF_NO  SHIPPERREFNO," ;
            Strsql += "SHIPDTLS.ADM_ADDRESS_1   SHIPPERADD1," ;
            Strsql += "SHIPDTLS.ADM_ADDRESS_2   SHIPPERADD2," ;
            Strsql += "SHIPDTLS.ADM_ADDRESS_3   SHIPPERADD3," ;
            Strsql += "SHIPDTLS.ADM_CITY        SHIPPERCITY," ;
            Strsql += "SHIPDTLS.ADM_ZIP_CODE    SHIPPERZIP," ;
            Strsql += "SHIPDTLS.ADM_PHONE_NO_1  SHIPPERPHONE," ;
            Strsql += "SHIPDTLS.ADM_FAX_NO      SHIPPERFAX," ;
            Strsql += "SHIPDTLS.ADM_EMAIL_ID    SHIPPEREMAIL," ;
            Strsql += "SHIPCOUNTRY.COUNTRY_NAME SHIPPERCOUNTRY," ;
            Strsql += "SHIPMST.VAT_NO VATNO," ;
            Strsql += "SHIPMST.CREDIT_DAYS PAYMENTDAYS," ;

            Strsql += "CONSMST.CUSTOMER_NAME    CONSIGNEE," ;
            Strsql += "CONSDTLS.ADM_ADDRESS_1   CONSIGNEEADD1," ;
            Strsql += "CONSDTLS.ADM_ADDRESS_2   CONSIGNEEADD2," ;
            Strsql += "CONSDTLS.ADM_ADDRESS_3   CONSIGNEEADD3," ;
            Strsql += "CONSDTLS.ADM_CITY        CONSIGNEECITY," ;
            Strsql += "CONSDTLS.ADM_ZIP_CODE    CONSIGNEEZIP," ;
            Strsql += "CONSDTLS.ADM_PHONE_NO_1  CONSIGNEEPHONE," ;
            Strsql += "CONSDTLS.ADM_FAX_NO      CONSIGNEEFAX," ;
            Strsql += "CONSDTLS.ADM_EMAIL_ID    CONSIGNEEMAIL," ;
            Strsql += "CONSCOUNTRY.COUNTRY_NAME CONSIGNEEOUNTRY," ;

            Strsql += "FEMST.FREIGHT_ELEMENT_NAME FREIGHTNAME," ;
            Strsql += "NVL(INVTAGTEXP.AMT_IN_INV_CURR,0)     FREIGHTAMT," ;
            Strsql += "INVTAGTEXP.TAX_PCNT        FRETAXPCNT," ;
            Strsql += "INVTAGTEXP.TAX_AMT         FRETAXANT," ;

            Strsql += "CURRMST.CURRENCY_ID CURRID," ;
            Strsql += "CURRMST.CURRENCY_NAME CURRNAME," ;
            Strsql += "(CASE WHEN JSE.VOYAGE_FLIGHT_NO IS NOT NULL THEN" ;
            Strsql += "JSE.VESSEL_NAME || '-' || JSE.VOYAGE_FLIGHT_NO" ;
            Strsql += "ELSE" ;
            Strsql += "JSE.VESSEL_NAME END) VES_FLIGHT," ;
            Strsql += "JSE.PYMT_TYPE PYMT," ;
            Strsql += "JSE.GOODS_DESCRIPTION GOODS," ;
            Strsql += "JSE.MARKS_NUMBERS MARKS," ;
            Strsql += "NVL(JSE.INSURANCE_AMT, 0) INSURANCE," ;
            Strsql += "STMST.INCO_CODE TERMS," ;
            Strsql += "COLMST.PLACE_NAME COLPLACE," ;
            Strsql += "DELMST.PLACE_NAME DELPLACE," ;
            Strsql += "POLMST.PORT_NAME POL," ;
            Strsql += "PODMST.PORT_NAME POD," ;
            Strsql += " HBL.HBL_REF_NO HBLREFNO ," ;
            Strsql += " MBL.MBL_REF_NO MBLREFNO ," ;
            Strsql += " CGMST.COMMODITY_GROUP_DESC COMMODITY," ;
            Strsql += "SUM(JSEC.VOLUME_IN_CBM) VOLUME," ;
            Strsql += "SUM(JSEC.GROSS_WEIGHT) GROSS," ;
            Strsql += "SUM(JSEC.NET_WEIGHT) NETWT," ;
            Strsql += "SUM(JSEC.CHARGEABLE_WEIGHT) CHARWT," ;
            Strsql += "INVCUSTEXP.INVOICE_DATE " ;


            Strsql += " FROM CONSOL_INVOICE_TBL INVCUSTEXP," ;
            Strsql += " CURRENCY_TYPE_MST_TBL CURRMST," ;

            Strsql += " CONSOL_INVOICE_TRN_TBL INVTAGTEXP," ;
            Strsql += "FREIGHT_ELEMENT_MST_TBL   FEMST," ;

            Strsql += "JOB_CARD_TRN    JSE," ;
            Strsql += "JOB_TRN_CONT    JSEC," ;

            Strsql += "SHIPPING_TERMS_MST_TBL  STMST," ;
            Strsql += "BOOKING_MST_TBL         BAT," ;
            Strsql += "PLACE_MST_TBL           COLMST," ;
            Strsql += "PLACE_MST_TBL           DELMST," ;
            Strsql += "PORT_MST_TBL            POLMST," ;
            Strsql += "PORT_MST_TBL            PODMST," ;
            Strsql += "HBL_EXP_TBL            HBL," ;
            Strsql += "MBL_EXP_TBL            MBL," ;
            Strsql += "COMMODITY_GROUP_MST_TBL CGMST," ;

            Strsql += "CUSTOMER_MST_TBL      SHIPMST," ;
            Strsql += "CUSTOMER_CONTACT_DTLS SHIPDTLS," ;
            Strsql += "COUNTRY_MST_TBL       SHIPCOUNTRY," ;

            Strsql += "CUSTOMER_MST_TBL      CONSMST," ;
            Strsql += "CUSTOMER_CONTACT_DTLS CONSDTLS," ;
            Strsql += "COUNTRY_MST_TBL CONSCOUNTRY" ;

            Strsql += "WHERE(INVTAGTEXP.JOB_CARD_FK = JSE.JOB_CARD_TRN_PK)" ;
            Strsql += "AND CURRMST.CURRENCY_MST_PK(+) = INVCUSTEXP.CURRENCY_MST_FK" ;

            Strsql += "AND INVTAGTEXP.CONSOL_INVOICE_FK(+) = INVCUSTEXP.CONSOL_INVOICE_PK" ;
            Strsql += "AND FEMST.FREIGHT_ELEMENT_MST_PK(+) = INVTAGTEXP.FRT_OTH_ELEMENT_FK" ;

            Strsql += "AND JSE.JOB_CARD_TRN_PK = JSEC.JOB_CARD_TRN_FK(+)" ;
            Strsql += "AND STMST.SHIPPING_TERMS_MST_PK(+) = JSE.SHIPPING_TERMS_MST_FK" ;
            Strsql += " AND BAT.BOOKING_MST_PK(+) = JSE.BOOKING_MST_FK" ;
            Strsql += "AND COLMST.PLACE_PK(+) = BAT.COL_PLACE_MST_FK" ;
            Strsql += "AND DELMST.PLACE_PK(+) = BAT.DEL_PLACE_MST_FK" ;
            Strsql += "AND POLMST.PORT_MST_PK = BAT.PORT_MST_POL_FK" ;
            Strsql += "AND PODMST.PORT_MST_PK = BAT.PORT_MST_POD_FK" ;
            Strsql += "AND HBL.HBL_EXP_TBL_PK(+) = JSE.HBL_HAWB_FK" ;
            Strsql += "AND MBL.MBL_EXP_TBL_PK(+) = JSE.MBL_MAWB_FK" ;
            Strsql += "AND CGMST.COMMODITY_GROUP_PK(+) = JSE.COMMODITY_GROUP_FK" ;

            Strsql += "AND CONSMST.CUSTOMER_MST_PK(+) = JSE.CONSIGNEE_CUST_MST_FK" ;
            Strsql += "AND CONSDTLS.CUSTOMER_MST_FK(+) = CONSMST.CUSTOMER_MST_PK" ;
            Strsql += "AND CONSDTLS.ADM_COUNTRY_MST_FK = CONSCOUNTRY.COUNTRY_MST_PK(+)" ;

            Strsql += "AND SHIPMST.CUSTOMER_MST_PK(+) = JSE.SHIPPER_CUST_MST_FK" ;
            Strsql += "AND SHIPDTLS.CUSTOMER_MST_FK(+) = SHIPMST.CUSTOMER_MST_PK" ;
            Strsql += "AND SHIPDTLS.ADM_COUNTRY_MST_FK = SHIPCOUNTRY.COUNTRY_MST_PK(+)" ;
            Strsql += "AND INVCUSTEXP.CONSOL_INVOICE_PK=" + InvPK ;
            Strsql += "GROUP BY INVCUSTEXP.CONSOL_INVOICE_PK," ;
            Strsql += "INVCUSTEXP.INVOICE_REF_NO," ;

            Strsql += "INVCUSTEXP.DISCOUNT_AMT," ;
            Strsql += "INVTAGTEXP.TAX_PCNT," ;
            Strsql += "INVTAGTEXP.TAX_AMT," ;
            Strsql += "JSE.JOB_CARD_TRN_PK," ;
            Strsql += "JSE.JOBCARD_REF_NO," ;
            Strsql += " JSE.ETD_DATE," ;
            Strsql += " JSE.ETA_DATE," ;
            Strsql += "BAT.CARGO_TYPE," ;
            Strsql += " SHIPMST.CUSTOMER_NAME," ;
            Strsql += "BAT.CUSTOMER_REF_NO," ;
            Strsql += "SHIPDTLS.ADM_ADDRESS_1," ;
            Strsql += "SHIPDTLS.ADM_ADDRESS_2," ;
            Strsql += "SHIPDTLS.ADM_ADDRESS_3," ;
            Strsql += "SHIPDTLS.ADM_CITY," ;
            Strsql += "SHIPDTLS.ADM_ZIP_CODE," ;
            Strsql += "SHIPDTLS.ADM_PHONE_NO_1," ;
            Strsql += "SHIPDTLS.ADM_FAX_NO," ;
            Strsql += "SHIPDTLS.ADM_EMAIL_ID," ;
            Strsql += "SHIPCOUNTRY.COUNTRY_NAME," ;
            Strsql += "SHIPMST.VAT_NO ," ;
            Strsql += "SHIPMST.CREDIT_DAYS," ;

            Strsql += "CONSMST.CUSTOMER_NAME    ," ;
            Strsql += "CONSDTLS.ADM_ADDRESS_1  ," ;
            Strsql += "CONSDTLS.ADM_ADDRESS_2 ," ;
            Strsql += "CONSDTLS.ADM_ADDRESS_3 ," ;
            Strsql += "CONSDTLS.ADM_CITY   ," ;
            Strsql += "CONSDTLS.ADM_ZIP_CODE ," ;
            Strsql += "CONSDTLS.ADM_PHONE_NO_1 ," ;
            Strsql += "CONSDTLS.ADM_FAX_NO  ," ;
            Strsql += "CONSDTLS.ADM_EMAIL_ID  ," ;
            Strsql += "CONSCOUNTRY.COUNTRY_NAME ," ;

            Strsql += "FEMST.FREIGHT_ELEMENT_NAME," ;
            Strsql += "INVTAGTEXP.AMT_IN_INV_CURR," ;
            Strsql += "INVTAGTEXP.TAX_PCNT," ;
            Strsql += "INVTAGTEXP.TAX_AMT," ;
            Strsql += "CURRMST.CURRENCY_ID," ;
            Strsql += "CURRMST.CURRENCY_NAME," ;
            Strsql += "INVCUSTEXP.INVOICE_DATE," ;
            Strsql += "(CASE WHEN JSE.VOYAGE_FLIGHT_NO IS NOT NULL THEN" ;
            Strsql += "JSE.VESSEL_NAME || '-' || JSE.VOYAGE_FLIGHT_NO" ;
            Strsql += "ELSE" ;
            Strsql += "JSE.VESSEL_NAME END)," ;
            Strsql += "JSE.PYMT_TYPE," ;
            Strsql += "JSE.GOODS_DESCRIPTION," ;
            Strsql += "JSE.MARKS_NUMBERS," ;
            Strsql += "JSE.INSURANCE_AMT," ;
            Strsql += "STMST.INCO_CODE," ;
            Strsql += "COLMST.PLACE_NAME," ;
            Strsql += "DELMST.PLACE_NAME," ;
            Strsql += "POLMST.PORT_NAME," ;
            Strsql += "PODMST.PORT_NAME," ;
            Strsql += "HBL.HBL_REF_NO," ;
            Strsql += "MBL.MBL_REF_NO," ;
            Strsql += "CGMST.COMMODITY_GROUP_DESC" ;

            try
            {
                return ObjWk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchContainerDetails(Int64 nInvPK)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            strSQL = "SELECT ICSE.CONSOL_INVOICE_TRN_PK ,JTEC.CONTAINER_NUMBER CONTAINER";
            strSQL += "FROM CONSOL_INVOICE_TRN_TBL ICSE,";
            strSQL += "JOB_CARD_TRN  JSE,";
            strSQL += "JOB_TRN_CONT JTEC";
            strSQL += "WHERE(ICSE.Job_Card_Fk = JSE.JOB_CARD_TRN_PK)";
            strSQL += "AND JTEC.JOB_CARD_TRN_FK = JSE.JOB_CARD_TRN_PK";
            strSQL += "AND ICSE.CONSOL_INVOICE_TRN_PK = " + nInvPK;
            try
            {
                return (objWK.GetDataSet(strSQL));
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchInvToCusExpSeaDetails(Int32 JobPk)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();

            Strsql = "  SELECT J.JOB_CARD_TRN_PK,  J.JOBCARD_REF_NO,  '' HBL_REF_NO, " ;
            Strsql += "  J.MARKS_NUMBERS,   J.GOODS_DESCRIPTION,  SUM(HH.VOLUME_IN_CBM) AS CUBEM3, " ;
            Strsql += " SUM(HH.GROSS_WEIGHT) AS GROSS,  CASE WHEN (SELECT B.CARGO_TYPE FROM BOOKING_MST_TBL B WHERE B.BOOKING_MST_PK=(SELECT J.BOOKING_MST_FK FROM  JOB_CARD_TRN J WHERE J.JOB_CARD_TRN_PK=" + JobPk + "))=1 THEN  SUM(HH.NET_WEIGHT) ELSE SUM(HH.CHARGEABLE_WEIGHT) END AS NET " ;
            Strsql += " FROM JOB_CARD_TRN J, JOB_TRN_CONT HH " ;
            Strsql += "  WHERE J.JOB_CARD_TRN_PK=" + JobPk ;
            Strsql += "  AND HH.JOB_CARD_TRN_FK = J.JOB_CARD_TRN_PK " ;
            Strsql += "  GROUP BY J.JOB_CARD_TRN_PK, J.JOBCARD_REF_NO, J.MARKS_NUMBERS, J.GOODS_DESCRIPTION " ;

            try
            {
                return ObjWk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchInvToCusExpSeaDesc(Int32 JobPk, string InvRefNo)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();

            Strsql = "  SELECT JS.JOB_CARD_TRN_PK,JS.JOBCARD_REF_NO,ICT.COST_FRT_ELEMENT,ICT.COST_FRT_ELEMENT_FK," ;
            Strsql += "  DECODE(ICT.COST_FRT_ELEMENT,1," ;
            Strsql += "  (SELECT CE.COST_ELEMENT_NAME FROM COST_ELEMENT_MST_TBL CE " ;
            Strsql += "  WHERE CE.COST_ELEMENT_MST_PK=ICT.COST_FRT_ELEMENT_FK) ,2," ;
            Strsql += "  (SELECT FE.FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL FE WHERE" ;
            Strsql += "   FE.FREIGHT_ELEMENT_MST_PK=ICT.COST_FRT_ELEMENT_FK),3," ;
            Strsql += " (SELECT FE.FREIGHT_ELEMENT_NAME FROM FREIGHT_ELEMENT_MST_TBL FE WHERE" ;
            Strsql += "   FE.FREIGHT_ELEMENT_MST_PK=ICT.COST_FRT_ELEMENT_FK)) AS DESCRIPTION," ;
            Strsql += "  ICT.AMT_IN_INV_CURR AS CHARGES,ICT.TAX_AMT AS TAX," ;
            //PRIYA
            Strsql += "   '' AS CODE, '' AS TAXCODE,ICT.TOT_AMT AS TOTCHARGE,NVL(ICS.VAT_AMT,0) AS VAT_AMT,ICS.INVOICE_REF_NO,CT.CURRENCY_ID,CT.CURRENCY_NAME" ;
            Strsql += "   ,ICS.VAT_PCNT,ICS.VAT_AMT,NVL(ICS.DISCOUNT_AMT,0) AS DISCOUNT_AMT,(ICT.TOT_AMT+ICS.VAT_AMT-ICS.DISCOUNT_AMT) AS TOTAMOUNTDUE" ;
            Strsql += "  FROM JOB_CARD_TRN JS,INV_CUST_SEA_EXP_TBL ICS,INV_CUST_TRN_SEA_EXP_TBL ICT,CURRENCY_TYPE_MST_TBL CT" ;
            Strsql += "  WHERE JS.JOB_CARD_TRN_PK=" + JobPk ;
            Strsql += "  AND ICS.JOB_CARD_SEA_EXP_FK=JS.JOB_CARD_TRN_PK" ;
            Strsql += "  AND ICS.INV_CUST_SEA_EXP_PK=ICT.INV_CUST_SEA_EXP_FK" ;
            Strsql += "  AND ICS.JOB_CARD_SEA_EXP_FK=JS.JOB_CARD_TRN_PK" ;
            Strsql += "  AND ICS.INVOICE_REF_NO='" + InvRefNo + "'" ;
            Strsql += "  AND CT.CURRENCY_MST_PK=ICS.CURRENCY_MST_FK" ;


            try
            {
                return ObjWk.GetDataSet(Strsql);
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion
    }
}