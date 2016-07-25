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

using Oracle.DataAccess.Client;
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
    public class cls_InvoiceToAgentApproval : CommonFeatures
    {

        #region "Fetch All"
        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="strInvRefNo">The string inv reference no.</param>
        /// <param name="strJobRefNo">The string job reference no.</param>
        /// <param name="strHBLRefNo">The string HBL reference no.</param>
        /// <param name="strMBLRefNo">The string MBL reference no.</param>
        /// <param name="strCustID">The string customer identifier.</param>
        /// <param name="strVessel">The string vessel.</param>
        /// <param name="strVoyFlightNo">The string voy flight no.</param>
        /// <param name="SortColumn">The sort column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="InvType">Type of the inv.</param>
        /// <param name="SortType">Type of the sort.</param>
        /// <param name="AgentType">Type of the agent.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="InvoiceCatFlg">The invoice cat FLG.</param>
        /// <returns></returns>
        public DataSet FetchAll(string strInvRefNo = "", string strJobRefNo = "", string strHBLRefNo = "", string strMBLRefNo = "", string strCustID = "", string strVessel = "", string strVoyFlightNo = "", string SortColumn = "", Int32 CurrentPage = 0, Int32 TotalPage = 0,
        int InvType = 0, string SortType = " ASC ", short AgentType = 0, long usrLocFK = 0, Int32 flag = 0, string InvoiceCatFlg = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet dsAll = new DataSet();
            try
            {
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("INV_REF_NO_IN", (string.IsNullOrEmpty(strInvRefNo) ? "" : strInvRefNo.ToUpper())).Direction = ParameterDirection.Input;
                _with1.Add("JC_REF_NO_IN", (string.IsNullOrEmpty(strJobRefNo) ? "" : strJobRefNo.ToUpper())).Direction = ParameterDirection.Input;
                _with1.Add("HBL_REF_NO_IN", (string.IsNullOrEmpty(strHBLRefNo) ? "" : strHBLRefNo.ToUpper())).Direction = ParameterDirection.Input;
                _with1.Add("MBL_REF_NO_IN", (string.IsNullOrEmpty(strMBLRefNo) ? "" : strMBLRefNo.ToUpper())).Direction = ParameterDirection.Input;
                _with1.Add("CUST_ID_IN", (string.IsNullOrEmpty(strCustID) ? "" : strCustID.ToUpper())).Direction = ParameterDirection.Input;
                _with1.Add("VOY_FLIGHT_NO_IN", (string.IsNullOrEmpty(strVoyFlightNo) ? "" : strVoyFlightNo.ToUpper())).Direction = ParameterDirection.Input;
                _with1.Add("SORT_COL_IN", (string.IsNullOrEmpty(SortColumn) ? "" : SortColumn)).Direction = ParameterDirection.Input;
                _with1.Add("INV_TYPE_IN", (InvType == 0 ? 0 : InvType)).Direction = ParameterDirection.Input;
                _with1.Add("USER_LOC_FK_IN", (usrLocFK == 0 ? 0 : usrLocFK)).Direction = ParameterDirection.Input;
                _with1.Add("FLAG_IN", flag).Direction = ParameterDirection.Input;
                _with1.Add("SORT_TYPE_IN", (string.IsNullOrEmpty(SortType) ? "" : SortType)).Direction = ParameterDirection.Input;
                _with1.Add("INV_CAT_FLG_IN", (string.IsNullOrEmpty(InvoiceCatFlg) ? "" : (InvoiceCatFlg == "0" ? "" : InvoiceCatFlg))).Direction = ParameterDirection.Input;
                _with1.Add("PAGE_SIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;
                _with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("FETCH_INV_TO_AGENT_APPR_PKG", "FETCH_INV_TO_AGENT_APPR_LIST");
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
        #endregion

        #region "UpdateInvStatus"
        /// <summary>
        /// Updates the inv status.
        /// </summary>
        /// <param name="InvoicePkS">The invoice pk s.</param>
        /// <param name="remarks">The remarks.</param>
        /// <param name="Agent_Type">Type of the agent_.</param>
        /// <returns></returns>
        public string UpdateInvStatus(string InvoicePkS, string remarks, string Agent_Type = "")
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
                var _with2 = updCmd;
                _with2.Connection = objWF.MyConnection;
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.CommandText = objWF.MyUserName + ".FETCH_INV_TO_AGENT_APPR_PKG.APPROVE_INVOICE";
                var _with3 = _with2.Parameters;
                updCmd.Parameters.Add("INVOICE_PK_IN", InvoicePkS).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("REMARKS_IN", remarks).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("LAST_MODIFIED_BY_FK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("INV_TYPE_FLAG_IN", Agent_Type).Direction = ParameterDirection.Input;
                updCmd.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 5000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                intIns = Convert.ToInt16(_with2.ExecuteNonQuery());
                PushToTallyAtRunTime(InvoicePkS);
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
        /// <summary>
        /// Pushes to tally at run time.
        /// </summary>
        /// <param name="InvoiceToAgentPks">The invoice to agent PKS.</param>
        public void PushToTallyAtRunTime(string InvoiceToAgentPks)
        {
            //Push to financial system if realtime is selected
            if (!string.IsNullOrEmpty(InvoiceToAgentPks))
            {
                Quantum_QFOR.cls_Scheduler objSch = new Quantum_QFOR.cls_Scheduler();
                ArrayList schDtls = null;
                bool errGen = false;
                if (objSch.GetSchedulerPushType() == true)
                {
                    //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                    //try {
                    //	schDtls = objSch.FetchSchDtls();
                    //	//'Used to Fetch the Sch Dtls
                    //	objPush.UpdateTaxElement(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                    //	objPush.UpdatebankAcc(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                    //	objPush.UpdateCurrency(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen);
                    //	objPush.UpdateTransaction(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, InvoiceToAgentPks);
                    //	if (System.Configuration.ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
                    //		objPush.EventViewer(1, 1, HttpContext.Current.Session["USER_PK"]);
                    //	}
                    //} catch (Exception ex) {
                    //	if (System.Configuration.ConfigurationSettings.AppSettings["EVENTVIEWER"]) {
                    //		objPush.EventViewer(1, 2, HttpContext.Current.Session["USER_PK"]);
                    //	}
                }
            }
        }
    }
    #endregion


}