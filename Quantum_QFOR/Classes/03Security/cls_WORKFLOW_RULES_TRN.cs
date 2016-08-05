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

using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
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
    public class clsWORKFLOW_RULES_TRN : CommonFeatures
    {
        /// <summary>
        /// </summary>
        public long pk = 0;

        #region "FetchListing"

        /// <summary>
        /// Fetches the listing.
        /// </summary>
        /// <param name="strWFRulesId">The string wf rules identifier.</param>
        /// <param name="strDocId">The string document identifier.</param>
        /// <param name="strEmpName">Name of the string emp.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="strColumnName">Name of the string column.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="IsActive">The is active.</param>
        /// <param name="blnSortAscending">if set to <c>true</c> [BLN sort ascending].</param>
        /// <param name="intBusType">Type of the int bus.</param>
        /// <param name="intUser">The int user.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="biz_type">The biz_type.</param>
        /// <returns></returns>
        public string FetchListing(string strWFRulesId = "", string strDocId = "", string strEmpName = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 IsActive = 0, bool blnSortAscending = false, int intBusType = 0,
        int intUser = 0, Int32 flag = 0, string biz_type = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("WFRULES_ID_IN", (string.IsNullOrEmpty(strWFRulesId) ? "" : strWFRulesId)).Direction = ParameterDirection.Input;
                _with1.Add("DOC_ID_IN", (string.IsNullOrEmpty(strDocId) ? "" : strDocId)).Direction = ParameterDirection.Input;
                _with1.Add("EMP_NAME_IN", (string.IsNullOrEmpty(strEmpName) ? "" : strEmpName)).Direction = ParameterDirection.Input;
                _with1.Add("SEARCH_TYPE_IN", (string.IsNullOrEmpty(SearchType) ? "" : SearchType)).Direction = ParameterDirection.Input;
                _with1.Add("IS_ACTIVE_IN", IsActive).Direction = ParameterDirection.Input;
                _with1.Add("SORT_IN", (blnSortAscending == true ? 1 : 0)).Direction = ParameterDirection.Input;
                _with1.Add("SORT_COL_IN", (string.IsNullOrEmpty(strColumnName) ? "" : strColumnName)).Direction = ParameterDirection.Input;
                _with1.Add("FLAG_IN", flag).Direction = ParameterDirection.Input;
                _with1.Add("BIZ_IN", biz_type).Direction = ParameterDirection.Input;
                _with1.Add("M_MASTERPAGESIZE_IN", MasterPageSize).Direction = ParameterDirection.Input;
                _with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("CURRENTPAGE_IN", 1).Direction = ParameterDirection.InputOutput;
                _with1.Add("WF_BAND0", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("WF_BAND1", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_DOC_WORKFLOW", "FETCH_WORKFLOW_LIST");
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = 1;
                }
                DataRelation WFRel = null;
                WFRel = new DataRelation("WFRelation", DS.Tables[0].Columns["WORKFLOW_RULES_PK"], DS.Tables[1].Columns["WORKFLOW_RULES_PK"], true);
                WFRel.Nested = true;
                DS.Relations.Add(WFRel);
                return JsonConvert.SerializeObject(DS, Formatting.Indented);
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "FetchListing"

        #region "FetchLEntry"

        /// <summary>
        /// Fetches the entry.
        /// </summary>
        /// <param name="lngWFPk">The LNG wf pk.</param>
        /// <returns></returns>
        public DataSet FetchEntry(long lngWFPk)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with2 = objWF.MyCommand.Parameters;
                _with2.Add("WORKFLOW_RULES_FK_IN", lngWFPk).Direction = ParameterDirection.Input;
                _with2.Add("WF_HEADER", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_DOC_WORKFLOW", "FETCH_WORKFLOW_DETAILS");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "FetchLEntry"

        #region "Fetch Function"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="P_Workflow_Rules_Pk">The p_ workflow_ rules_ pk.</param>
        /// <param name="P_Document_Mst_Fk">The p_ document_ MST_ fk.</param>
        /// <param name="P_From_Loc_Mst_Fk">The p_ from_ loc_ MST_ fk.</param>
        /// <param name="P_To_Loc_Mst_Fk">The p_ to_ loc_ MST_ fk.</param>
        /// <param name="P_Department_Mst_Fk">The p_ department_ MST_ fk.</param>
        /// <param name="P_Designation_Mst_Fk">The p_ designation_ MST_ fk.</param>
        /// <param name="P_User_Message_Fk">The p_ user_ message_ fk.</param>
        /// <param name="P_Copy_To1_Fk">The p_ copy_ to1_ fk.</param>
        /// <param name="P_Copy_To2_Fk">The p_ copy_ to2_ fk.</param>
        /// <param name="P_Copy_To3_Fk">The p_ copy_ to3_ fk.</param>
        /// <param name="P_Active">The p_ active.</param>
        /// <param name="P_Valid_From">The p_ valid_ from.</param>
        /// <param name="P_Validto">The p_ validto.</param>
        /// <param name="P_Dueinhours">The p_ dueinhours.</param>
        /// <param name="P_Stepinhours">The p_ stepinhours.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="SortExpression">The sort expression.</param>
        /// <returns></returns>
        public DataSet FetchAll(Int64 P_Workflow_Rules_Pk, Int64 P_Document_Mst_Fk, Int64 P_From_Loc_Mst_Fk, Int64 P_To_Loc_Mst_Fk, Int64 P_Department_Mst_Fk, Int64 P_Designation_Mst_Fk, Int64 P_User_Message_Fk, Int64 P_Copy_To1_Fk, Int64 P_Copy_To2_Fk, Int64 P_Copy_To3_Fk,
        Int64 P_Active, string P_Valid_From, string P_Validto, Int64 P_Dueinhours, string P_Stepinhours, string SearchType = "", string SortExpression = "")
        {
            string strSQL = null;
            strSQL = "SELECT ROWNUM SR_NO, ";
            strSQL = strSQL + " Workflow_Rules_Pk,";
            strSQL = strSQL + " Document_Mst_Fk,";
            strSQL = strSQL + " To_Loc_Mst_Fk,";
            strSQL = strSQL + " Department_Mst_Fk,";
            strSQL = strSQL + " Designation_Mst_Fk,";
            strSQL = strSQL + " User_Message_Fk,";
            strSQL = strSQL + " Copy_To1_Fk,";
            strSQL = strSQL + " Copy_To2_Fk,";
            strSQL = strSQL + " Copy_To3_Fk,";
            strSQL = strSQL + " Active,";
            strSQL = strSQL + " Valid_From,";
            strSQL = strSQL + " Validto,";
            strSQL = strSQL + " Dueinhours,";
            strSQL = strSQL + " Stepinhours,";
            strSQL = strSQL + " Version_No ";
            strSQL = strSQL + " FROM WORKFLOW_RULES_TRN ";
            strSQL = strSQL + " WHERE ( 1 = 1) ";
            if (P_Workflow_Rules_Pk > 0)
            {
                strSQL = strSQL + " And Workflow_Rules_Pk = " + P_Workflow_Rules_Pk + " ";
            }
            if (P_Document_Mst_Fk > 0)
            {
                strSQL = strSQL + " And Document_Mst_Fk = " + P_Document_Mst_Fk + " ";
            }
            if (P_From_Loc_Mst_Fk > 0)
            {
                strSQL = strSQL + " And From_Loc_Mst_Fk = " + P_From_Loc_Mst_Fk + " ";
            }
            if (P_To_Loc_Mst_Fk > 0)
            {
                strSQL = strSQL + " And To_Loc_Mst_Fk = " + P_To_Loc_Mst_Fk + " ";
            }
            if (P_Department_Mst_Fk > 0)
            {
                strSQL = strSQL + " And Department_Mst_Fk = " + P_Department_Mst_Fk + " ";
            }
            if (P_Designation_Mst_Fk > 0)
            {
                strSQL = strSQL + " And Designation_Mst_Fk = " + P_Designation_Mst_Fk + " ";
            }
            if (P_User_Message_Fk > 0)
            {
                strSQL = strSQL + " And User_Message_Fk = " + P_User_Message_Fk + " ";
            }
            if (P_Copy_To1_Fk > 0)
            {
                strSQL = strSQL + " And Copy_To1_Fk = " + P_Copy_To1_Fk + " ";
            }
            if (P_Copy_To2_Fk > 0)
            {
                strSQL = strSQL + " And Copy_To2_Fk = " + P_Copy_To2_Fk + " ";
            }
            if (P_Copy_To3_Fk > 0)
            {
                strSQL = strSQL + " And Copy_To3_Fk = " + P_Copy_To3_Fk + " ";
            }
            if (P_Active > 0)
            {
                strSQL = strSQL + " And Active = " + P_Active + " ";
            }
            if (P_Valid_From.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Valid_From like '%" + P_Valid_From + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Valid_From like '" + P_Valid_From + "%' ";
                }
            }
            else
            {
            }
            if (P_Validto.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Validto like '%" + P_Validto + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Validto like '" + P_Validto + "%' ";
                }
            }
            else
            {
            }
            if (P_Dueinhours > 0)
            {
                strSQL = strSQL + " And Dueinhours = " + P_Dueinhours + " ";
            }
            if (P_Stepinhours.ToString().Trim().Length > 0)
            {
                if (SearchType == "C")
                {
                    strSQL = strSQL + " And Stepinhours like '%" + P_Stepinhours + "%' ";
                }
                else
                {
                    strSQL = strSQL + " And Stepinhours like '" + P_Stepinhours + "%' ";
                }
            }
            else
            {
            }
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL);
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

        #endregion "Fetch Function"

        #region "Save Function"

        /// <summary>
        /// Saves the work flow.
        /// </summary>
        /// <param name="lngWFPk">The LNG wf pk.</param>
        /// <param name="strWorkFlowID">The string work flow identifier.</param>
        /// <param name="lngDocFk">The LNG document fk.</param>
        /// <param name="strDocName">Name of the string document.</param>
        /// <param name="lngToLocFk">The LNG to loc fk.</param>
        /// <param name="strToLoc">The string to loc.</param>
        /// <param name="lngDeptFk">The LNG dept fk.</param>
        /// <param name="strDeptName">Name of the string dept.</param>
        /// <param name="lngDesgFk">The LNG desg fk.</param>
        /// <param name="strDesgName">Name of the string desg.</param>
        /// <param name="lngCopy1Fk">The LNG copy1 fk.</param>
        /// <param name="strCopy1">The string copy1.</param>
        /// <param name="lngCopy2">The LNG copy2.</param>
        /// <param name="strCopy2">The string copy2.</param>
        /// <param name="lngCopy3">The LNG copy3.</param>
        /// <param name="strCopy3">The string copy3.</param>
        /// <param name="lngEmpPk">The LNG emp pk.</param>
        /// <param name="strEmp">The string emp.</param>
        /// <param name="intActive">The int active.</param>
        /// <param name="blnUpdate">if set to <c>true</c> [BLN update].</param>
        /// <param name="intVersion">The int version.</param>
        /// <param name="strLoc">The string loc.</param>
        /// <param name="DSLog">The ds log.</param>
        /// <param name="ValidFrom">The valid from.</param>
        /// <param name="ValidTo">The valid to.</param>
        /// <returns></returns>
        public ArrayList SaveWorkFlow(long lngWFPk, string strWorkFlowID, long lngDocFk, string strDocName, long lngToLocFk, string strToLoc, long lngDeptFk, string strDeptName, long lngDesgFk, string strDesgName,
        long lngCopy1Fk, string strCopy1, long lngCopy2, string strCopy2, long lngCopy3, string strCopy3, long lngEmpPk, string strEmp, int intActive, bool blnUpdate,
        int intVersion, string strLoc, DataSet DSLog, string ValidFrom = "", string ValidTo = "")
        {
            WorkFlow objWK = new WorkFlow();
            bool chkflag = false;
            try
            {
                if (blnUpdate == true)
                {
                    chkflag = true;
                }
                else
                {
                    chkflag = false;
                }
                objWK.OpenConnection();
                OracleTransaction TRAN = null;
                TRAN = objWK.MyConnection.BeginTransaction();
                var _with3 = objWK.MyCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.Transaction = TRAN;
                if (blnUpdate == true)
                {
                    _with3.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_TRN_PKG.WORKFLOW_RULES_TRN_UPD";
                }
                else
                {
                    _with3.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_TRN_PKG.WORKFLOW_RULES_TRN_INS";
                }

                var _with4 = _with3.Parameters;
                if (blnUpdate == true)
                {
                    _with4.Add("WORKFLOW_RULES_PK_IN", lngWFPk).Direction = ParameterDirection.Input;
                    _with4.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with4.Add("VERSION_NO_IN", intVersion).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with4.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                }
                _with4.Add("WORKFLOW_ID_IN", strWorkFlowID).Direction = ParameterDirection.Input;
                _with4.Add("DOCUMENT_MST_FK_IN", lngDocFk).Direction = ParameterDirection.Input;
                _with4.Add("TO_LOC_MST_FK_IN", lngToLocFk).Direction = ParameterDirection.Input;
                _with4.Add("DEPARTMENT_MST_FK_IN", lngDeptFk).Direction = ParameterDirection.Input;
                _with4.Add("DESIGNATION_MST_FK_IN", lngDesgFk).Direction = ParameterDirection.Input;
                _with4.Add("COPY_TO1_FK_IN", lngCopy1Fk).Direction = ParameterDirection.Input;
                _with4.Add("COPY_TO2_FK_IN", lngCopy2).Direction = ParameterDirection.Input;
                _with4.Add("COPY_TO3_FK_IN", lngCopy3).Direction = ParameterDirection.Input;
                _with4.Add("VALID_FROM_IN", (string.IsNullOrEmpty(ValidFrom) ? "" : ValidFrom)).Direction = ParameterDirection.Input;
                _with4.Add("VALID_TO_IN", (string.IsNullOrEmpty(ValidTo) ? "" : ValidTo)).Direction = ParameterDirection.Input;
                _with4.Add("USER_MST_FK_IN", lngEmpPk).Direction = ParameterDirection.Input;
                _with4.Add("ACTIVE_IN", intActive).Direction = ParameterDirection.Input;
                _with4.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                if ((_with3.ExecuteNonQuery() > 0))
                {
                    lngWFPk = Convert.ToInt64(_with3.Parameters["RETURN_VALUE"].Value);
                    pk = lngWFPk;
                    if (SaveChild(objWK, lngWFPk, strLoc, TRAN, blnUpdate, DSLog) > 0)
                    {
                        arrMessage.Add("All Data Saved Successfully");
                        return arrMessage;
                    }
                }
                else
                {
                    if (chkflag == false)
                    {
                        RollbackProtocolKey("WORKFLOW", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), strWorkFlowID, DateTime.Now);
                        chkflag = true;
                    }
                    TRAN.Rollback();
                    arrMessage.Add("Invalid save operation");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                if (chkflag == false)
                {
                    RollbackProtocolKey("WORKFLOW", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), strWorkFlowID, DateTime.Now);
                    chkflag = true;
                }
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                if (chkflag == false)
                {
                    RollbackProtocolKey("WORKFLOW", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), strWorkFlowID, DateTime.Now);
                    chkflag = true;
                }
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
            return new ArrayList();
        }

        /// <summary>
        /// Saves the child.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="lngWFPk">The LNG wf pk.</param>
        /// <param name="strLoc">The string loc.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="blnUpdate">if set to <c>true</c> [BLN update].</param>
        /// <param name="DSLog">The ds log.</param>
        /// <returns></returns>
        private long SaveChild(WorkFlow objWK, long lngWFPk, string strLoc, OracleTransaction TRAN, bool blnUpdate, DataSet DSLog)
        {
            OracleCommand insCommand = new OracleCommand();
            try
            {
                objWK.OpenConnection();
                objWK.MyConnection = TRAN.Connection;
                insCommand.Connection = objWK.MyConnection;
                insCommand.Transaction = TRAN;

                insCommand.CommandType = CommandType.StoredProcedure;
                insCommand.CommandText = objWK.MyUserName + ".WORKFLOW_RULES_TRN_PKG.SAVECHILD";
                var _with5 = insCommand.Parameters;
                _with5.Clear();
                _with5.Add("WORKFLOW_PK_IN", lngWFPk).Direction = ParameterDirection.Input;
                _with5.Add("LOCPK_IN", strLoc).Direction = ParameterDirection.Input;
                var _with6 = objWK.MyDataAdapter;
                _with6.InsertCommand = insCommand;
                _with6.InsertCommand.Transaction = TRAN;
                if (objWK.MyDataAdapter.InsertCommand.ExecuteNonQuery() > 0)
                {
                    //'Saving Log Details
                    if (SaveLogDetails(objWK, lngWFPk, TRAN, blnUpdate, DSLog) > 0)
                    {
                        return 1;
                    }
                    else
                    {
                        return 0;
                    }
                }
                else
                {
                    return 0;
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        /// <summary>
        /// Saves the log details.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="lngWFPk">The LNG wf pk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="blnUpdate">if set to <c>true</c> [BLN update].</param>
        /// <param name="DSLog">The ds log.</param>
        /// <returns></returns>
        private long SaveLogDetails(WorkFlow objWK, long lngWFPk, OracleTransaction TRAN, bool blnUpdate, DataSet DSLog)
        {
            OracleCommand insCommand = new OracleCommand();
            Int32 RecAfct = default(Int32);
            try
            {
                objWK.OpenConnection();
                objWK.MyConnection = TRAN.Connection;
                insCommand.Connection = objWK.MyConnection;
                insCommand.Transaction = TRAN;

                //'First Save
                if (blnUpdate == false)
                {
                    insCommand.CommandType = CommandType.StoredProcedure;
                    insCommand.CommandText = objWK.MyUserName + ".FETCH_DOC_WORKFLOW.WORKFLOW_LOG_SAVE";
                    var _with7 = insCommand.Parameters;
                    _with7.Clear();
                    _with7.Add("WF_PK_IN", lngWFPk).Direction = ParameterDirection.Input;
                    _with7.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with7.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    var _with8 = objWK.MyDataAdapter;
                    _with8.InsertCommand = insCommand;
                    _with8.InsertCommand.Transaction = TRAN;
                    if (objWK.MyDataAdapter.InsertCommand.ExecuteNonQuery() > 0)
                    {
                        TRAN.Commit();
                        return 1;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return 0;
                    }
                }
                else
                {
                    //'
                    if (DSLog.Tables[0].Rows.Count > 0)
                    {
                        var _with9 = insCommand;
                        _with9.CommandType = CommandType.StoredProcedure;
                        _with9.CommandText = objWK.MyUserName + ".FETCH_DOC_WORKFLOW.WORKFLOW_RULES_LOG_INS";
                        var _with10 = _with9.Parameters;
                        insCommand.Parameters.Add("WORKFLOW_RULES_FK_IN", OracleDbType.Int32, 20, "WORKFLOW_RULES_FK").Direction = ParameterDirection.Input;
                        insCommand.Parameters["WORKFLOW_RULES_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("FROM_LOCATION_MST_FK_IN", OracleDbType.Int32, 20, "FROM_LOCATION_MST_FK").Direction = ParameterDirection.Input;
                        insCommand.Parameters["FROM_LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("TO_LOCATION_MST_FK_IN", OracleDbType.Int32, 20, "TO_LOCATION_MST_FK").Direction = ParameterDirection.Input;
                        insCommand.Parameters["TO_LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("USER_MST_FK_IN", OracleDbType.Int32, 20, "USER_MST_FK").Direction = ParameterDirection.Input;
                        insCommand.Parameters["USER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("DEPARTMENT_MST_FK_IN", OracleDbType.Int32, 20, "DEPARTMENT_MST_FK").Direction = ParameterDirection.Input;
                        insCommand.Parameters["DEPARTMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("DESIGNATION_MST_FK_IN", OracleDbType.Int32, 20, "DESIGNATION_MST_FK").Direction = ParameterDirection.Input;
                        insCommand.Parameters["DESIGNATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("COPY_TO1_FK_IN", OracleDbType.Int32, 20, "COPY_TO1_FK").Direction = ParameterDirection.Input;
                        insCommand.Parameters["COPY_TO1_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("COPY_TO2_FK_IN", OracleDbType.Int32, 20, "COPY_TO2_FK").Direction = ParameterDirection.Input;
                        insCommand.Parameters["COPY_TO2_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("COPY_TO3_FK_IN", OracleDbType.Int32, 20, "COPY_TO3_FK").Direction = ParameterDirection.Input;
                        insCommand.Parameters["COPY_TO3_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("VALID_FROM_IN", OracleDbType.Date, 1, "VALID_FROM").Direction = ParameterDirection.Input;
                        insCommand.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("VALID_TO_IN", OracleDbType.Date, 1, "VALID_TO").Direction = ParameterDirection.Input;
                        insCommand.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("ADD_DEL_IN", OracleDbType.Int32, 1, "ADD_DEL").Direction = ParameterDirection.Input;
                        insCommand.Parameters["ADD_DEL_IN"].SourceVersion = DataRowVersion.Current;

                        insCommand.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                        insCommand.Parameters.Add("CREATED_BY_DT_IN", DateTime.Today.Date).Direction = ParameterDirection.Input;
                        insCommand.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                        insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "WORKFLOW_LOG_PK").Direction = ParameterDirection.Output;
                        insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        var _with11 = objWK.MyDataAdapter;
                        _with11.InsertCommand = insCommand;
                        _with11.InsertCommand.Transaction = TRAN;
                        RecAfct = _with11.Update(DSLog.Tables[0]);
                        if (RecAfct > 0)
                        {
                            TRAN.Commit();
                            return 1;
                        }
                        else
                        {
                            TRAN.Rollback();
                            return 0;
                        }
                    }
                    else
                    {
                        TRAN.Commit();
                        return 1;
                    }
                    //'
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                throw oraexp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Save Function"

        #region "FetchListing"

        /// <summary>
        /// Fetch_s the work flow log.
        /// </summary>
        /// <param name="lngWFPk">The LNG wf pk.</param>
        /// <returns></returns>
        public DataSet Fetch_WorkFlowLog(long lngWFPk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            try
            {
                var _with12 = objWF.MyCommand.Parameters;
                _with12.Add("WF_PK_IN", lngWFPk).Direction = ParameterDirection.Input;
                _with12.Add("WF_LOG", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_DOC_WORKFLOW", "FETCH_WORKFLOW_LOG");
                return DS;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
        }

        #endregion "FetchListing"
    }
}