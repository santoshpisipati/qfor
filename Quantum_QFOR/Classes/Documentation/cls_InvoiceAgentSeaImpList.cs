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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class cls_InvoiceAgentSeaImpList : CommonFeatures
    {
        #region "ENHANCE SEARCH"

        public string FetchJobCardImportForInvoice(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = null;
            string strBizType = null;
            string strReq = null;
            string strAgentType = "";
            string strWithAgent = "";
            string strLoc = null;
            strLoc = loc;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBizType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strAgentType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strWithAgent = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_IMPORT_JOB_REF_NO_PKG.GET_ACTIVE_JOB_REF_FOR_IMP_INV";

                var _with1 = selectCommand.Parameters;
                _with1.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                _with1.Add("AGENTTYPE_IN", getDefault(strAgentType, 0)).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with1.Add("WITH_AGENT_IN", (!string.IsNullOrEmpty(strWithAgent) ? strWithAgent : "")).Direction = ParameterDirection.Input;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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

        public string FetchActiveJobCard(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string stragent = null;
            string strLoc = null;
            string strWithAgent = "";
            string strReq = null;
            strLoc = loc;
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBusiType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                stragent = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strWithAgent = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_IMPORT_JOB_REF_NO_PKG.GET_ACTIVE_JOB_REF_COMMON";
                var _with2 = SCM.Parameters;
                _with2.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with2.Add("AGENT_TYPE_IN", (!string.IsNullOrEmpty(stragent) ? stragent : "")).Direction = ParameterDirection.Input;
                _with2.Add("WITH_AGENT_IN", (!string.IsNullOrEmpty(strWithAgent) ? strWithAgent : "")).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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

        public string FetchActiveJobAgentImp(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strBusiType = "";
            string stragent = null;
            string strLoc = null;
            string strReq = null;
            strLoc = loc;
            arr = strCond.Split('~');
            if (arr.Length > 0)
                strReq = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 1)
                strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBusiType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                stragent = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLoc = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_IMPORT_JOB_REF_NO_PKG.GET_ACTIVE_JOB_REF_AGIMP";
                var _with3 = SCM.Parameters;
                _with3.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_IN", strLoc).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with3.Add("AGENT_TYPE_IN", (!string.IsNullOrEmpty(stragent) ? stragent : "")).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.NVarchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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

        #endregion "ENHANCE SEARCH"

        #region "Fetch All"

        public DataSet FetchAll(string strInvPK = "", string strJobPK = "", string strHBLPK = "", string strMBLPK = "", string strCustPK = "", string strVessel = "", string strVoyage = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0,
        int InvType = 0, string SortType = " ASC ", short AgentType = 0, long usrLocFK = 0, Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            strCondition = "FROM ";
            strCondition += "INV_AGENT_TBL INV,";
            strCondition += "JOB_CARD_SEA_IMP_TBL JOB,";
            strCondition += "AGENT_MST_TBL CMT,";
            strCondition += "CURRENCY_TYPE_MST_TBL CUMT,";
            strCondition += "USER_MST_TBL UMT ";
            strCondition += "WHERE";

            strCondition += " INV.JOB_CARD_SEA_IMP_FK = JOB.JOB_CARD_SEA_IMP_PK";
            strCondition += "AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ";
            strCondition += "AND INV.CREATED_BY_FK = UMT.USER_MST_PK ";

            if (InvType > 0)
            {
                if (InvType == 1)
                {
                    strCondition += " and nvl(INV.CHK_INVOICE,0) = 1 ";
                }
                else if (InvType == 2)
                {
                    strCondition += " and nvl(INV.CHK_INVOICE,0) = 0 ";
                }
                else
                {
                    strCondition += " and nvl(INV.CHK_INVOICE,0) = 2 ";
                }
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (AgentType == 1)
            {
                strCondition += "AND JOB.CB_AGENT_MST_FK=CMT.AGENT_MST_PK (+)";
            }
            else
            {
                strCondition += "AND JOB.POL_AGENT_MST_FK=CMT.AGENT_MST_PK (+)";
            }
            strCondition += "AND INV.CURRENCY_MST_FK=CUMT.CURRENCY_MST_PK";

            if (AgentType == 1)
            {
                strCondition += " AND INV.CB_OR_LOAD_AGENT=1 ";
            }
            else if (AgentType == 2)
            {
                strCondition += " AND INV.CB_OR_LOAD_AGENT=2 ";
            }

            if (!string.IsNullOrEmpty(strInvPK.Trim()))
            {
                strCondition += "AND UPPER(INV.INVOICE_REF_NO)  LIKE '" + "%" + strInvPK.ToUpper().Replace("'", "''") + "%'";
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
                strCondition += "AND CMT.AGENT_ID='" + strCustPK.Trim() + "'";
            }

            if (!string.IsNullOrEmpty(strVessel.Trim()))
            {
                strCondition += "AND JOB.VESSEL_NAME LIKE '%" + strVessel.Trim() + "%'";
            }

            if (!string.IsNullOrEmpty(strVoyage.Trim()))
            {
                strCondition += "AND JOB.VOYAGE LIKE '%" + strVoyage.Trim() + "%'";
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
            strSQL += "INV.INV_AGENT_PK,";
            strSQL += "INV.JOB_CARD_FK,";
            strSQL += "JOB.HBL_REF_NO HBL,";
            strSQL += "JOB.MBL_REF_NO MBL,";
            strSQL += "JOB.CB_AGENT_MST_FK,";
            strSQL += "INV.CURRENCY_MST_FK,";
            strSQL += "INV.INVOICE_REF_NO,";
            strSQL += "INV.INVOICE_DATE INVDATE,";
            strSQL += "JOB.JOBCARD_REF_NO,";
            strSQL += "JOB.HBL_REF_NO,";
            strSQL += "JOB.MBL_REF_NO,";
            strSQL += "CMT.AGENT_NAME,";
            strSQL += "DECODE((JOB.VESSEL_NAME  || '' || JOB.VOYAGE),'','',JOB.VESSEL_NAME  || '' || JOB.VOYAGE) VESVOYAGE,";
            strSQL += "CUMT.CURRENCY_ID,";
            strSQL += "INV.NET_INV_AMT,";
            strSQL += " DECODE(INV.CHK_INVOICE,0,'Pending',1,'Approved',2,'Cancelled') CHK_INVOICE, ";
            strSQL += " NVL((SELECT DISTINCT CT.INVOICE_REF_NR FROM COLLECTIONS_TRN_TBL CT WHERE CT.INVOICE_REF_NR =INV.Invoice_Ref_No ),'0') COLL_STATUS, ";
            strSQL += "'' SEL ";
            strSQL += strCondition;
            if (SortColumn == "INVDATE")
            {
                SortColumn = "INV.INVOICE_DATE";
            }
            else if (SortColumn == "VESVOYAGE")
            {
                SortColumn = "JOB.VESSEL_NAME,JOB.VOYAGE";
            }
            strSQL += " ORDER BY " + SortColumn + SortType + " ,INVOICE_REF_NO DESC  ) q  ) ";
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

        #endregion "Fetch All"

        #region "Fetch Invoice Agent Sea Import List"

        public int GetIASICount(string iasiRefNr, string iasiAgentType, int iasiPk, long locPK)
        {
            try
            {
                System.Text.StringBuilder strIASIQuery = new System.Text.StringBuilder(5000);
                strIASIQuery.Append(" select iasi.inv_agent_pk, iasi.invoice_ref_no");
                strIASIQuery.Append(" from INV_AGENT_TBL iasi, user_mst_tbl umt");
                strIASIQuery.Append(" where iasi.invoice_ref_no like '%" + iasiRefNr + "%'");
                strIASIQuery.Append(" and iasi.created_by_fk = umt.user_mst_pk");
                strIASIQuery.Append(" and umt.default_location_fk=" + locPK);
                if (iasiAgentType == "CB")
                {
                    strIASIQuery.Append(" and iasi.cb_or_load_agent = '1'");
                }
                else if (iasiAgentType == "LA")
                {
                    strIASIQuery.Append(" and iasi.cb_or_load_agent = '2'");
                }

                WorkFlow objWF = new WorkFlow();
                DataSet objIASIDS = new DataSet();
                objIASIDS = objWF.GetDataSet(strIASIQuery.ToString());
                if (objIASIDS.Tables[0].Rows.Count == 1)
                {
                    iasiPk = Convert.ToInt32(objIASIDS.Tables[0].Rows[0][0]);
                }
                return objIASIDS.Tables[0].Rows.Count;
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

        #endregion "Fetch Invoice Agent Sea Import List"

        #region "UpdateInvStatus"

        public string UpdateInvStatus(string InvoicePkS, string remarks, short CUST_OR_AGENT = 0)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand updCmd = new OracleCommand();
            DataSet dsPia = null;
            string str = null;
            string strIns = null;
            string Ret = null;
            Int16 intDel = default(Int16);
            Int16 intIns = default(Int16);
            try
            {
                objWF.OpenConnection();
                var _with4 = updCmd;
                _with4.Connection = objWF.MyConnection;
                _with4.CommandType = CommandType.StoredProcedure;
                _with4.CommandText = objWF.MyUserName + ".INVOICE_CANCELLATION_PKG.CANCEL_INVOICE";
                var _with5 = _with4.Parameters;
                updCmd.Parameters.Add("INVOICE_PK_IN", InvoicePkS).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("REMARKS_IN", remarks).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("INV_TYPE_FLAG_IN", "CB_AGENT_SEA_IMP").Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                intIns = Convert.ToInt16(_with4.ExecuteNonQuery());
                return Convert.ToString(updCmd.Parameters["RETURN_VALUE"].Value);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "UpdateInvStatus"
    }
}