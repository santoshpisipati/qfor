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
    public class cls_InvoiceListSeaImp : CommonFeatures
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

            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBizType = Convert.ToString(arr.GetValue(2));
            // If arr.Length > 3 Then
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_IMP_INV_REF_NO_PKG.GET_INV_REF_COMMON";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_IN", loc).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
            string strJobPK = null;
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strBizType = Convert.ToString(arr.GetValue(2));
            strJobPK = Convert.ToString(arr.GetValue(3));

            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_CUSTOMER_PKG.GETCUSTOMER_JOBCARD_IMP";

                var _with2 = selectCommand.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with2.Add("JOB_CARD_PK_IN", strJobPK).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
        public string FetchVoyageForJobCard(string strCond, string loc = "0")
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
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.GET_VES_VOY_JOBCARD_IMP";

                var _with3 = selectCommand.Parameters;
                _with3.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with3.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", getDefault(strReq, "L")).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", getDefault(strBizType, 3)).Direction = ParameterDirection.Input;
                _with3.Add("JOB_CARD_PK_IN", getDefault(strJobPK, 0)).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
        public string FetchVoyageForJobCardNew(string strCond, string loc = "0")
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
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_VES_VOY_PKG.GET_VES_VOY_JOBCARD_IMP_NEW";

                var _with4 = selectCommand.Parameters;
                _with4.Add("VES_IN", (!string.IsNullOrEmpty(strVES) ? strVES : "")).Direction = ParameterDirection.Input;
                _with4.Add("VOY_IN", (!string.IsNullOrEmpty(strVOY) ? strVOY : "")).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", getDefault(strReq, "L")).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with4.Add("BUSINESS_TYPE_IN", getDefault(strBizType, 3)).Direction = ParameterDirection.Input;
                _with4.Add("JOB_CARD_PK_IN", getDefault(strJobPK, 0)).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
        public string FetchJobCardForInvoice(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = "";
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strLoc = null;
            string strCustPk = null;

            strLoc = loc;
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strCustPk = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_IMPORT_JOB_REF_NO_PKG.GET_ACTIVE_JOB_REF_FOR_INV";

                var _with5 = selectCommand.Parameters;
                _with5.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with5.Add("LOCATION_IN", getDefault(strLoc, 0)).Direction = ParameterDirection.Input;
                _with5.Add("BUSINESS_TYPE_IN", getDefault(strBizType, 3)).Direction = ParameterDirection.Input;
                _with5.Add("CUSTOMER_PK_IN", getDefault(strCustPk, 0)).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", OracleDbType.NVarchar2, 2000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
        string SortType = " ASC ", long usrLocFK = 0, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strCondition = "FROM ";
            strCondition += "INV_CUST_SEA_IMP_TBL INV,";
            strCondition += "JOB_CARD_SEA_IMP_TBL JOB,";
            strCondition += "CUSTOMER_MST_TBL CMT,";
            strCondition += "CURRENCY_TYPE_MST_TBL CUMT,";
            strCondition += "USER_MST_TBL UMT ";
            strCondition += "WHERE";
            strCondition += "INV.JOB_CARD_SEA_IMP_FK = JOB.JOB_CARD_SEA_IMP_PK";
            strCondition += "AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ";
            strCondition += "AND INV.CREATED_BY_FK = UMT.USER_MST_PK ";
            strCondition += "AND JOB.CONSIGNEE_CUST_MST_FK=CMT.CUSTOMER_MST_PK (+)";
            strCondition += "AND INV.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";

            if (!string.IsNullOrEmpty(strInvPK.Trim()))
            {
                strCondition += "AND UPPER(INV.INVOICE_REF_NO) LIKE '" + "%" + strInvPK.ToUpper().Replace("'", "''") + "%'";
            }

            if (!string.IsNullOrEmpty(strJobPK.Trim()))
            {
                strCondition += "AND UPPER(JOB.JOBCARD_REF_NO) LIKE'%" + strJobPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strHBLPK.Trim()))
            {
                strCondition += "AND UPPER(JOB.HBL_REF_NO) LIKE '%" + strHBLPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strMBLPK.Trim()))
            {
                strCondition += "AND UPPER(JOB.MBL_REF_NO) LIKE '%" + strMBLPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strCustPK.Trim()))
            {
                strCondition += "AND CMT.CUSTOMER_ID='" + strCustPK.Trim() + "'";
            }

            if (!string.IsNullOrEmpty(strVessel.Trim()))
            {
                strCondition += "AND JOB.VESSEL_NAME LIKE '%" + strVessel.Trim() + "%'";
            }

            if (!string.IsNullOrEmpty(strVoyage.Trim()))
            {
                strCondition += "AND JOB.VOYAGE LIKE '%" + strVoyage.Trim() + "%'";
            }
            //If BlankGrid = 0 Then
            //    strCondition &= vbCrLf & " AND 1=2 "
            //End If
            if (flag == 0)
            {
                strCondition += " AND 1=2";
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
            strSQL += "INV.INV_CUST_SEA_IMP_PK,";
            strSQL += "INV.JOB_CARD_SEA_IMP_FK,";
            strSQL += "JOB.CONSIGNEE_CUST_MST_FK,";
            strSQL += "INV.CURRENCY_MST_FK,";
            strSQL += "INV.INVOICE_REF_NO,";
            strSQL += "INV.INVOICE_DATE INVDATE,";
            strSQL += "JOB.JOBCARD_REF_NO,";
            strSQL += "JOB.HBL_REF_NO,";
            strSQL += "JOB.MBL_REF_NO,";
            strSQL += "CMT.CUSTOMER_NAME,";
            strSQL += "(CASE WHEN (NVL(JOB.VESSEL_NAME,'') || '/' || NVL(JOB.VOYAGE,'')='/') THEN '' ELSE NVL(JOB.VESSEL_NAME,'') || '/' || NVL(JOB.VOYAGE,'') END ) AS VESVOYAGE,";
            strSQL += "CUMT.CURRENCY_ID,";
            strSQL += "INV.NET_PAYABLE";
            strSQL += strCondition;
            if (SortColumn == "INVDATE")
            {
                SortColumn = "INV.INVOICE_DATE";
            }
            else if (SortColumn == "VESVOYAGE")
            {
                SortColumn = "JOB.VESSEL_NAME,JOB.VOYAGE";
            }
            strSQL += " ORDER BY " + SortColumn + SortType + "  ,INVOICE_REF_NO DESC ) q  ) ";
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
        long usrLocFK = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strCondition = "FROM ";
            strCondition += "JOB_CARD_SEA_IMP_TBL JOB,";
            strCondition += "CUSTOMER_MST_TBL CMT,";
            strCondition += "USER_MST_TBL UMT ";
            strCondition += "WHERE";
            strCondition += " UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ";
            strCondition += "AND JOB.CREATED_BY_FK = UMT.USER_MST_PK ";
            strCondition += "AND JOB.CONSIGNEE_CUST_MST_FK=CMT.CUSTOMER_MST_PK (+)";

            if (!string.IsNullOrEmpty(strJobPK.Trim()))
            {
                strCondition += "AND UPPER(JOB.JOBCARD_REF_NO) LIKE'%" + strJobPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strHBLPK.Trim()))
            {
                strCondition += "AND UPPER(JOB.HBL_REF_NO) LIKE '%" + strHBLPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strMBLPK.Trim()))
            {
                strCondition += "AND UPPER(JOB.MBL_REF_NO) LIKE '%" + strMBLPK.Trim().ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(strCustPK.Trim()))
            {
                strCondition += "AND CMT.CUSTOMER_ID='" + strCustPK.Trim() + "'";
            }

            if (!string.IsNullOrEmpty(strVessel.Trim()))
            {
                strCondition += "AND JOB.VESSEL_NAME LIKE '%" + strVessel.Trim() + "%'";
            }

            if (!string.IsNullOrEmpty(strVoyage.Trim()))
            {
                strCondition += "AND JOB.VOYAGE LIKE '%" + strVoyage.Trim() + "%'";
            }

            strSQL = " SELECT SR_NO,INV_CUST_SEA_IMP_PK,JOB_CARD_SEA_IMP_FK,";
            strSQL += "CONSIGNEE_CUST_MST_FK,CURRENCY_MST_FK,";
            strSQL += "INVOICE_REF_NO,INVDATE,JOBCARD_REF_NO,HBL_REF_NO,";
            strSQL += "MBL_REF_NO,CUSTOMER_NAME, VESVOYAGE, CURRENCY_ID, NET_PAYABLE";
            strSQL += "FROM (";
            strSQL += "SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += "( SELECT * FROM (SELECT ";
            strSQL += "0 INV_CUST_SEA_IMP_PK,";
            strSQL += "JOB.JOB_CARD_SEA_IMP_PK JOB_CARD_SEA_IMP_FK,";
            strSQL += "JOB.CONSIGNEE_CUST_MST_FK,";
            strSQL += "0 CURRENCY_MST_FK,";
            strSQL += "''INVOICE_REF_NO,";
            strSQL += "NULL AS INVDATE,";
            strSQL += "JOB.JOBCARD_REF_NO,";
            strSQL += "JOB.HBL_REF_NO,";
            strSQL += "JOB.MBL_REF_NO,";
            strSQL += "CMT.CUSTOMER_NAME,";
            strSQL += "(CASE WHEN JOB.VESSEL_NAME IS NOT NULL AND JOB.VOYAGE IS NOT NULL AND JOB.VESSEL_NAME <> '' AND JOB.VOYAGE <> '' THEN JOB.VESSEL_NAME || '/' || JOB.VOYAGE ELSE JOB.VESSEL_NAME END) AS VESVOYAGE,";
            strSQL += "'' CURRENCY_ID,";
            strSQL += "NULL NET_PAYABLE,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_SEA_IMP_PIA PIA WHERE PIA.JOB_CARD_SEA_IMP_FK=JOB.JOB_CARD_SEA_IMP_PK AND PIA.INVOICE_SEA_TBL_FK IS NULL AND PIA.INV_AGENT_TRN_SEA_IMP_FK IS NULL ) AS PC,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_SEA_IMP_FD FD WHERE FD.JOB_CARD_SEA_IMP_FK=JOB.JOB_CARD_SEA_IMP_PK AND FD.INV_CUST_TRN_SEA_IMP_FK IS NULL AND FD.INV_AGENT_TRN_SEA_IMP_FK IS NULL ) AS FC,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_SEA_IMP_OTH_CHRG OTH WHERE OTH.JOB_CARD_SEA_IMP_FK=JOB.JOB_CARD_SEA_IMP_PK AND OTH.INV_CUST_TRN_SEA_IMP_FK IS NULL AND OTH.INV_AGENT_TRN_SEA_IMP_FK IS NULL) AS OC,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_SEA_IMP_PIA PIA WHERE PIA.JOB_CARD_SEA_IMP_FK=JOB.JOB_CARD_SEA_IMP_PK) AS PC1,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_SEA_IMP_FD FD WHERE FD.JOB_CARD_SEA_IMP_FK=JOB.JOB_CARD_SEA_IMP_PK) AS FC1,";
            strSQL += "(SELECT COUNT(*) FROM JOB_TRN_SEA_IMP_OTH_CHRG OTH WHERE OTH.JOB_CARD_SEA_IMP_FK=JOB.JOB_CARD_SEA_IMP_PK) AS OC1";
            strSQL += strCondition;
            if (SortColumn == "INVDATE")
            {
                //SortColumn = "INV.INVOICE_DATE"
            }
            else if (SortColumn == "VESVOYAGE")
            {
                SortColumn = "JOB.VESSEL_NAME,JOB.VOYAGE";
            }
            strSQL += " ORDER BY " + SortColumn + SortType + " )  ";
            strSQL += " WHERE  (PC>0 OR FC>0 OR OC>0 OR (PC1=0 AND FC1=0 AND OC1=0)))Q)";


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

            strSQL += " WHERE SR_NO BETWEEN " + start + " AND " + last + " ";

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

    }
}