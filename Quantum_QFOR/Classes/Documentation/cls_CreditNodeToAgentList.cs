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

using Oracle.DataAccess.Client;
using System;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsCreditNoteToAgentList : CommonFeatures
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
        /// <param name="lngUsrLocFk">The LNG usr loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="status">The status.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchAll(string creditNo = "", string InvNo = "", string Agent = "", string AgentName = "", short AgentType = 0, string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " DESC ", long lngUsrLocFk = 0,
        Int32 flag = 0, Int32 status = 0, Int32 BizType = 0, Int32 ProcessType = 0)
        {
            //***********************for new query procedure*******************************
            try
            {
                WorkFlow objWF = new WorkFlow();
                DataTable dt = null;
                DataSet ds = new DataSet();

                if (creditNo.Length > 0)
                {
                    creditNo = "%" + creditNo.ToUpper() + "%";
                }

                if (InvNo.Length > 0)
                {
                    InvNo = "%" + InvNo.ToUpper() + "%";
                }
                if (flag == 0)
                {
                    BlankGrid = 2;
                }
                else
                {
                    BlankGrid = 1;
                }
                objWF.MyCommand.Parameters.Clear();
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("CREDIT_NOTE_REF_IN", (creditNo.Length > 0 ? creditNo : "")).Direction = ParameterDirection.Input;
                _with1.Add("INV_NO_IN", (InvNo.Length > 0 ? InvNo : "")).Direction = ParameterDirection.Input;
                _with1.Add("AGENT_ID_IN", (Agent.Length > 0 ? Agent : "")).Direction = ParameterDirection.Input;
                _with1.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with1.Add("AGENT_TYPE_IN", AgentType).Direction = ParameterDirection.Input;
                _with1.Add("COLUMN", SortColumn).Direction = ParameterDirection.Input;
                _with1.Add("SORT", SortType).Direction = ParameterDirection.Input;
                _with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("USERLOCPK_IN", lngUsrLocFk).Direction = ParameterDirection.Input;
                _with1.Add("BLANK", BlankGrid).Direction = ParameterDirection.Input;
                //_with1.Add("status_in", (status == 0 ? "" : status)).Direction = ParameterDirection.Input;
                _with1.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with1.Add("PROCESSTYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with1.Add("BL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dt = objWF.GetDataTable("CR_AGENT_LIST_ALLTYPE_PKG", "FETCH_DATA_ALLTYPE");
                //TotalPage = objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value;
                //CurrentPage = objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value;
                ds.Tables.Add(dt);
                return ds;
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
                var _with2 = updCmd;
                _with2.Connection = objWF.MyConnection;
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.CommandText = objWF.MyUserName + ".PAYMENT_CRN_CANCELLATION_PKG.CANCEL_PAYMENT_CRN";
                var _with3 = _with2.Parameters;
                updCmd.Parameters.Add("PK_IN", CrNotePk).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("REMARKS_IN", remarks).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("TYPE_FLAG_IN", "CR_AGENT").Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                //intIns = _with2.ExecuteNonQuery();
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