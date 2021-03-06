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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsCreditNoteToAgentAirImpList : CommonFeatures
    {
        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="creditNo">The credit no.</param>
        /// <param name="InvNo">The inv no.</param>
        /// <param name="Agent">The agent.</param>
        /// <param name="AgentName">Name of the agent.</param>
        /// <param name="AgentType">Type of the agent.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="status">The status.</param>
        /// <returns></returns>
        public DataSet FetchAll(string creditNo = "", string InvNo = "", string Agent = "", string AgentName = "", short AgentType = 0, string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", long usrLocFK = 0,
        Int32 flag = 0, Int32 status = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            //**********Condition part*********
            strCondition = " FROM ";
            strCondition += " CR_AGENT_TBL CR, ";
            strCondition += " CURRENCY_TYPE_MST_TBL CUR,";
            strCondition += " AGENT_MST_TBL AG, ";
            strCondition += " INV_AGENT_AIR_IMP_TBL INV,";
            strCondition += "USER_MST_TBL UMT ";
            strCondition += " WHERE CR.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK ";

            strCondition += "AND UMT.DEFAULT_LOCATION_FK = " + usrLocFK + " ";
            strCondition += "AND CR.CREATED_BY_FK = UMT.USER_MST_PK ";

            strCondition += " AND AG.AGENT_MST_PK(+) = CR.AGENT_MST_FK ";
            strCondition += " AND CR.INV_AGENT_FK = INV.INV_AGENT_AIR_IMP_PK(+)";

            if (InvNo.Length > 0)
            {
                strCondition += " AND UPPER(INV.INVOICE_REF_NO) LIKE '%" + InvNo.ToUpper().Replace("'", "''") + "%'";
            }

            if (AgentType == 1)
            {
                strCondition += " AND CR.CB_DP_LOAD_AGENT=1 ";
                strCondition += " AND AG.AGENT_MST_PK(+) = CR.AGENT_MST_FK ";
            }
            else if (AgentType == 2)
            {
                strCondition += " AND CR.CB_DP_LOAD_AGENT=2 ";
                strCondition += " AND AG.AGENT_MST_PK(+) = CR.AGENT_MST_FK ";
            }
            if (status > 0)
            {
                strCondition += " AND CR.CRN_STATUS=" + status;
            }
            if (Agent.Length > 0)
            {
                strCondition += " AND UPPER(AG.AGENT_ID) LIKE '%" + Agent.ToUpper().Replace("'", "''") + "%'";
            }
            if (AgentName.Length > 0)
            {
                strCondition += " AND UPPER(AG.AGENT_NAME) LIKE '%" + AgentName.ToUpper().Replace("'", "''") + "%'";
            }

            if (creditNo.Length > 0)
            {
                strCondition += " AND UPPER(CR.CREDIT_NOTE_REF_NO) LIKE '%" + creditNo.ToUpper().Replace("'", "''") + "%'";
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2 ";
            }
            //*****'End Condition****
            //strSQL &= vbCrLf & "
            strSQL = " SELECT COUNT(*) ";
            strSQL += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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
            strSQL = " SELECT * from (";
            strSQL += " SELECT ROWNUM SR_NO,q.* FROM ";
            strSQL += " (SELECT CR.CR_AGENT_PK,";
            strSQL += " CR.CREDIT_NOTE_REF_NO, ";
            strSQL += " CR.CREDIT_NOTE_DATE, ";
            strSQL += " INV.INVOICE_REF_NO, ";
            strSQL += " AG.AGENT_NAME, ";
            strSQL += " CR.CURRENCY_MST_FK, ";
            strSQL += " CUR.CURRENCY_ID, ";
            strSQL += " CR.CREDIT_NOTE_AMT ,DECODE(CR.CRN_STATUS,null,'Approved',1,'Approved',2,'Cancelled') CRN_STATUS ,'' SEL";
            strSQL += strCondition;
            if (SortColumn == "CR_DATE")
            {
                SortColumn = "CR.CREDIT_NOTE_DATE";
            }
            strSQL += " ORDER BY " + SortColumn + SortType + " ,CREDIT_NOTE_REF_NO desc) q  ) ";
            strSQL += " WHERE SR_NO  Between " + start + " and " + last;

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

        #region "UpdateCRNoteStatus"

        /// <summary>
        /// Updates the cr note status.
        /// </summary>
        /// <param name="CrNotePk">The cr note pk.</param>
        /// <param name="remarks">The remarks.</param>
        /// <returns></returns>
        public string UpdateCRNoteStatus(string CrNotePk, string remarks)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand updCmd = new OracleCommand();
            Int16 intIns = default(Int16);
            try
            {
                objWF.OpenConnection();
                var _with1 = updCmd;
                _with1.Connection = objWF.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWF.MyUserName + ".PAYMENT_CRN_CANCELLATION_PKG.CANCEL_PAYMENT_CRN";
                var _with2 = _with1.Parameters;
                updCmd.Parameters.Add("PK_IN", CrNotePk).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("REMARKS_IN", remarks).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("TYPE_FLAG_IN", "CR_AGENT_AIR_IMP").Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                //intIns = _with1.ExecuteNonQuery();
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

        #endregion "UpdateCRNoteStatus"
    }
}