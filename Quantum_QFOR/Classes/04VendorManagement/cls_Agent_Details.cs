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
//'*  Modified DateTime(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Agent_Details : CommonFeatures
    {
        #region " Fetch Details"

        /// <summary>
        /// Gets the agents pk.
        /// </summary>
        /// <param name="Agent">The agent.</param>
        /// <returns></returns>
        public Int32 GetAgentsPK(string Agent)
        {
            try
            {
                string strSQL = null;
                Int32 AgentPK = default(Int32);
                WorkFlow objWF = new WorkFlow();
                strSQL = "select agent_mst_pk from agent_mst_tbl where agent_id='" + Agent + "'";
                AgentPK = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
                return AgentPK;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 13/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetch_s the commision_ amt.
        /// </summary>
        /// <param name="NoOfBls">The no of BLS.</param>
        /// <param name="FROM_DATE">The fro m_ date.</param>
        /// <param name="TO_DATE">The t o_ date.</param>
        /// <param name="Agent">The agent.</param>
        /// <param name="VslVoy">The VSL voy.</param>
        /// <param name="Process">The process.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="TotalCommAmt">The total comm amt.</param>
        public void Fetch_Commision_Amt(string NoOfBls, string FROM_DATE, string TO_DATE, string Agent, string VslVoy, string Process, Int32 CurrentPage, Int32 TotalPage, string TotalCommAmt)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();

                DataSet dsAll = null;
                Array arr = null;
                Int32 AgentPK = default(Int32);
                string VslVoy1 = null;

                AgentPK = GetAgentsPK(Agent);
                arr = VslVoy.Split('/');
                VslVoy = arr.GetValue(0).ToString();
                if (!string.IsNullOrEmpty(VslVoy))
                {
                    VslVoy1 = arr.GetValue(1).ToString();
                }
                else
                {
                    VslVoy1 = "";
                }

                //NoOfBls = "0"
                //TotalCommAmt = "0"

                objWF.MyCommand.Parameters.Clear();
                var _with1 = objWF.MyCommand.Parameters;

                //FROM_DATE
                _with1.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;

                //TO_DATE()
                _with1.Add("TO_DATE", getDefault(TO_DATE, "")).Direction = ParameterDirection.Input;

                _with1.Add("AgentComm", getDefault(AgentPK, "0")).Direction = ParameterDirection.Input;

                _with1.Add("VslVoy", getDefault(VslVoy, "0")).Direction = ParameterDirection.Input;

                _with1.Add("VslVoy1", getDefault(VslVoy1, "0")).Direction = ParameterDirection.Input;

                //M_MASTERPAGESIZE_IN()
                _with1.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;

                _with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;

                //CURRENTPAGE_IN()
                _with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;

                _with1.Add("NoOfBls", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;

                //CURRENTPAGE_IN()

                _with1.Add("TotalCommAmt", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;

                _with1.Add("POL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with1.Add("FGT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                string StrPackage = null;
                string StrProc = null;

                if (Convert.ToInt32(Process) == 0)
                {
                    StrPackage = "FETCH_AGENCY_WISE_COMMISION";
                    StrProc = "Fetch_Data_Comm_Exp";
                }

                if (Convert.ToInt32(Process) == 1)
                {
                    StrPackage = "FETCH_AGENCY_WISE_COMMISION";
                    StrProc = "Fetch_Data_Comm_Imp";
                }
                dsAll = objWF.GetDataSet(StrPackage, StrProc);
                NoOfBls = Convert.ToString(objWF.MyCommand.Parameters["NoOfBls"].Value);
                TotalCommAmt = Convert.ToString(objWF.MyCommand.Parameters["TotalCommAmt"].Value);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetch_s the data_ agent.
        /// </summary>
        /// <param name="FROM_DATE">The fro m_ date.</param>
        /// <param name="TO_DATE">The t o_ date.</param>
        /// <param name="Agent">The agent.</param>
        /// <param name="VslVoy">The VSL voy.</param>
        /// <param name="Process">The process.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="OPERATOR_PK">The operato r_ pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="NoOfBLs">The no of b ls.</param>
        /// <param name="NoOfAWBLs">The no of awb ls.</param>
        /// <param name="TotalCommAmt">The total comm amt.</param>
        /// <param name="TotalProfitAmt">The total profit amt.</param>
        /// <param name="Commission">The commission.</param>
        /// <param name="Profit">The profit.</param>
        /// <param name="CurrPk">The curr pk.</param>
        /// <returns></returns>
        public DataSet Fetch_Data_Agent(string FROM_DATE, string TO_DATE, string Agent, string VslVoy, string Process, Int32 CurrentPage, Int32 TotalPage, Int32 OPERATOR_PK, long BizType, string NoOfBLs,
        string NoOfAWBLs, string TotalCommAmt, string TotalProfitAmt, int Commission, int Profit, string CurrPk)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();

                DataSet dsAll = null;
                Array arr = null;
                Int32 AgentPK = default(Int32);
                string VslVoy1 = null;

                AgentPK = GetAgentsPK(Agent);
                arr = VslVoy.Split('/');
                VslVoy = Convert.ToString(arr.GetValue(0));
                if (!string.IsNullOrEmpty(VslVoy))
                {
                    if (arr.Length >= 1)
                    {
                        VslVoy1 = Convert.ToString(arr.GetValue(1));
                    }
                    else
                    {
                        VslVoy1 = "";
                    }
                }
                else
                {
                    VslVoy1 = "";
                }

                objWF.MyCommand.Parameters.Clear();
                var _with2 = objWF.MyCommand.Parameters;

                //FROM_DATE
                _with2.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;

                //TO_DATE()
                _with2.Add("TO_DATE", getDefault(TO_DATE, "")).Direction = ParameterDirection.Input;
                if (string.IsNullOrEmpty(CurrPk) | CurrPk == "0")
                {
                    _with2.Add("AgentComm", getDefault(AgentPK, "0")).Direction = ParameterDirection.Input;
                    _with2.Add("CURRENCY_PK", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with2.Add("AgentComm", getDefault(OPERATOR_PK, "0")).Direction = ParameterDirection.Input;
                    _with2.Add("CURRENCY_PK", CurrPk).Direction = ParameterDirection.Input;
                }

                _with2.Add("VslVoy", getDefault(VslVoy, "")).Direction = ParameterDirection.Input;

                _with2.Add("VslVoy1", getDefault(VslVoy1, "")).Direction = ParameterDirection.Input;

                //M_MASTERPAGESIZE_IN()
                _with2.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;

                _with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;

                //CURRENTPAGE_IN()
                _with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;

                //BL_CUR()
                _with2.Add("POL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                //aDDED BY SIVACHANDRAN

                _with2.Add("OPERATOR_PK", getDefault(OPERATOR_PK, "0")).Direction = ParameterDirection.Input;
                _with2.Add("PROCESS_TYPE_IN", Process).Direction = ParameterDirection.Input;
                _with2.Add("BizType_IN", BizType).Direction = ParameterDirection.Input;
                _with2.Add("LOGED_IN_LOC", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with2.Add("NoOfBLs", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                _with2.Add("NoOfAWBLs", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                _with2.Add("TotalCommAmt", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                _with2.Add("TotalProfitAmt", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                //.Add("Commission_IN", Commission).Direction = ParameterDirection.Input
                //.Add("Profit_IN", Profit).Direction = ParameterDirection.Input
                //End Sivachandran
                _with2.Add("FGT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                string StrPackage = null;
                string StrProc = null;

                if (Convert.ToInt32(Process) == 0)
                {
                    ///StrPackage = "FETCH_AGENCY_WISE_COMMISION" c
                    ///StrProc = "Fetch_Data_Agent_Exp"
                    StrPackage = "FETCH_AGENCY_COMMISION_PROFIT";
                    if (string.IsNullOrEmpty(CurrPk) | CurrPk == "0")
                    {
                        StrProc = "Fetch_Data_Agent_Exp_Comm";
                    }
                    else
                    {
                        StrProc = "Fetch_Data_Vendor_Exp_Comm";
                    }
                }
                if (Convert.ToInt32(Process) == 1)
                {
                    ///StrPackage = "FETCH_AGENCY_WISE_COMMISION"
                    ///StrProc = "Fetch_Data_Agent_Imp"
                    StrPackage = "FETCH_AGENCY_COMMISION_PROFIT";
                    if (string.IsNullOrEmpty(CurrPk) | CurrPk == "0")
                    {
                        StrProc = "Fetch_Data_Agent_Exp_Comm";
                    }
                    else
                    {
                        StrProc = "Fetch_Data_Vendor_Exp_Comm";
                    }
                }

                if (Convert.ToInt32(Process) == 2)
                {
                    ///StrPackage = "FETCH_AGENCY_WISE_COMMISION"
                    ///StrProc = "Fetch_Data_Agent_Imp"
                    StrPackage = "FETCH_AGENCY_COMMISION_PROFIT";
                    if (string.IsNullOrEmpty(CurrPk) | CurrPk == "0")
                    {
                        StrProc = "Fetch_Data_Agent_Exp_Comm";
                    }
                    else
                    {
                        StrProc = "Fetch_Data_Vendor_Exp_Comm";
                    }
                }

                if (BizType == 3)
                {
                    StrPackage = "FETCH_AGENCY_COMMISION_PROFIT";
                    if (string.IsNullOrEmpty(CurrPk) | CurrPk == "0")
                    {
                        StrProc = "Fetch_Data_Agent_Exp_Comm_all";
                    }
                }

                dsAll = objWF.GetDataSet(StrPackage, StrProc);
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                NoOfBLs = Convert.ToString((string.IsNullOrEmpty(objWF.MyCommand.Parameters["NoOfBLs"].Value.ToString()) ? " " : objWF.MyCommand.Parameters["NoOfBLs"].Value));
                TotalCommAmt = Convert.ToString((string.IsNullOrEmpty(objWF.MyCommand.Parameters["TotalCommAmt"].Value.ToString()) ? " " : objWF.MyCommand.Parameters["TotalCommAmt"].Value));
                NoOfAWBLs = Convert.ToString((string.IsNullOrEmpty(objWF.MyCommand.Parameters["NoOfAWBLs"].Value.ToString()) ? " " : objWF.MyCommand.Parameters["NoOfAWBLs"].Value));
                TotalProfitAmt = Convert.ToString((string.IsNullOrEmpty(objWF.MyCommand.Parameters["TotalProfitAmt"].Value.ToString()) ? " " : objWF.MyCommand.Parameters["TotalProfitAmt"].Value));
                ///CreateRelation(dsAll)'Commented by sivachandran for Profir Implementation
                return dsAll;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Added by Sivachandran For Report - Agency Commission & Profit Sharing
        /// <summary>
        /// Fetch_s the profit.
        /// </summary>
        /// <param name="FROM_DATE">The fro m_ date.</param>
        /// <param name="TO_DATE">The t o_ date.</param>
        /// <param name="Agent">The agent.</param>
        /// <param name="VslVoy">The VSL voy.</param>
        /// <param name="Process">The process.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="OPERATOR_PK">The operato r_ pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet Fetch_Profit(string FROM_DATE, string TO_DATE, string Agent, string VslVoy, string Process, Int32 CurrentPage, Int32 TotalPage, Int32 OPERATOR_PK, long BizType)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                DataSet dsAll = null;
                Array arr = null;
                Int32 AgentPK = default(Int32);
                string VslVoy1 = null;

                AgentPK = Convert.ToInt32(GetAgentsPK(Agent));
                arr = VslVoy.Split('/');
                VslVoy = arr.GetValue(0).ToString();
                if (!string.IsNullOrEmpty(VslVoy))
                {
                    VslVoy1 = arr.GetValue(1).ToString();
                }
                else
                {
                    VslVoy1 = "";
                }

                objWF.MyCommand.Parameters.Clear();
                var _with3 = objWF.MyCommand.Parameters;
                _with3.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;

                _with3.Add("TO_DATE", getDefault(TO_DATE, "")).Direction = ParameterDirection.Input;

                _with3.Add("AgentComm", getDefault(AgentPK, "0")).Direction = ParameterDirection.Input;

                _with3.Add("VslVoy", getDefault(VslVoy, "")).Direction = ParameterDirection.Input;

                _with3.Add("VslVoy1", getDefault(VslVoy1, "")).Direction = ParameterDirection.Input;

                _with3.Add("M_MASTERPAGESIZE_IN", RecordsPerPage).Direction = ParameterDirection.Input;

                _with3.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;

                _with3.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;

                _with3.Add("CURRENCY_PK", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with3.Add("OPERATOR_PK", getDefault(OPERATOR_PK, "0")).Direction = ParameterDirection.Input;
                _with3.Add("PROCESS_TYPE_IN", Process).Direction = ParameterDirection.Input;
                _with3.Add("BizType_IN", BizType).Direction = ParameterDirection.Input;
                _with3.Add("LOGED_IN_LOC", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with3.Add("FGT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                string StrPackage = null;
                string StrProc = null;

                if (Convert.ToInt32(Process) == 0)
                {
                    StrPackage = "FETCH_AGENCY_COMMISION_PROFIT";
                    StrProc = "Fetch_Agency_Profit";
                }
                if (Convert.ToInt32(Process) == 1)
                {
                    StrPackage = "FETCH_AGENCY_COMMISION_PROFIT";
                    StrProc = "Fetch_Agency_Profit";
                }

                dsAll = objWF.GetDataSet(StrPackage, StrProc);
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                return dsAll;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Creates the relation.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        private void CreateRelation(DataSet dsMain)
        {
            // Get the DataColumn objects from two DataTable objects in a DataSet.
            DataColumn parentCol = null;
            DataColumn childCol = null;

            // Code to get the DataSet not shown here.
            parentCol = dsMain.Tables[0].Columns["REF_NO"];
            childCol = dsMain.Tables[1].Columns["REF_NO"];

            //  parentsecCol = dsMain.Tables(0).Columns("FREIGHT")
            //  childsecCol = dsMain.Tables(1).Columns("FREIGHT")

            // Create DataRelation.
            DataRelation relREF = null;
            relREF = new DataRelation("REF", new DataColumn[] {
                dsMain.Tables[0].Columns["REF_NO"],
                dsMain.Tables[0].Columns["FREIGHT"]
            }, new DataColumn[] {
                dsMain.Tables[1].Columns["REF_NO"],
                dsMain.Tables[1].Columns["FREIGHT"]
            });

            relREF.Nested = true;

            dsMain.Relations.Add(relREF);
        }

        #endregion " Fetch Details"

        #region " Fetch Function "

        // This Method will fetch One Record in DataSet which will have the Given Agent ID
        // if No ID is provided then it will simply return an empty DataSet with the Same Structure
        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="AGENTID">The agentid.</param>
        /// <returns></returns>
        public DataSet FetchAll(string AGENTID = "")
        {
            bool NewRecord = true;
            if (!string.IsNullOrEmpty(AGENTID))
                NewRecord = false;
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            string strCondition = " and 1=2 ";
            if (!NewRecord)
            {
                strCondition = " and ACD.AGENT_MST_FK = '" + AGENTID + "' AND ALM.AGENT_MST_PK = '" + AGENTID + "'";
                strCondition += " AND ALM.LOCATION_MST_FK = LOC.LOCATION_MST_PK(+)";
                strCondition += " AND ADMCNR.COUNTRY_MST_PK(+) = ACD.ADM_COUNTRY_MST_FK ";
                strCondition += " AND CORCNR.COUNTRY_MST_PK(+) = ACD.COR_COUNTRY_MST_FK ";
                strCondition += " AND BILLCNR.COUNTRY_MST_PK(+) = ACD.BILL_COUNTRY_MST_FK";
            }
            strSQL = " SELECT ";
            // Master Table
            strSQL += "ALM.AGENT_MST_PK,           ALM.AGENT_ID,           ALM.AGENT_NAME,";
            strSQL += "ALM.ACTIVE_FLAG,            ALM.ACCOUNT_NO,         ALM.IATA_CODE,";
            strSQL += "ALM.LOCATION_MST_FK,        ALM.VAT_NO,             ALM.IATA_APPROVED,";
            strSQL += "ALM.BUSINESS_TYPE,          ALM.EXP_PROFIT_PER,     ALM.EXP_COMM_PER,";
            strSQL += "ALM.IMP_PROFIT_PER,         ALM.IMP_COMM_PER,ALM.CHANNEL_PARTNER,ALM.CUSTOMS_CODE,ALM.AGENT_TYPE,";
            // ADMINISTRATIVE
            strSQL += "ACD.ADM_ADDRESS_1,          ACD.ADM_ADDRESS_2,      ACD.ADM_ADDRESS_3,";
            strSQL += "ACD.ADM_CITY,               ACD.ADM_ZIP_CODE,       ACD.ADM_COUNTRY_MST_FK,";
            strSQL += "ACD.ADM_CONTACT_PERSON,     ACD.ADM_PHONE_NO_1,     ACD.ADM_PHONE_NO_2,";
            strSQL += "ACD.ADM_FAX_NO,             ACD.ADM_EMAIL_ID,       ACD.ADM_URL,";
            strSQL += "ACD.ADM_SHORT_NAME,         ACD.ADM_SALUTATION,";
            // CORRESPONDENCE
            strSQL += "ACD.COR_ADDRESS_1,          ACD.COR_ADDRESS_2,      ACD.COR_ADDRESS_3,";
            strSQL += "ACD.COR_CITY,               ACD.COR_ZIP_CODE,       ACD.COR_COUNTRY_MST_FK,";
            strSQL += "ACD.COR_CONTACT_PERSON,     ACD.COR_PHONE_NO_1,     ACD.COR_PHONE_NO_2,";
            strSQL += "ACD.COR_FAX_NO,             ACD.COR_EMAIL_ID,       ACD.COR_URL,";
            strSQL += "ACD.COR_SHORT_NAME,         ACD.COR_SALUTATION,   ";
            // BILLING
            strSQL += "ACD.BILL_ADDRESS_1,         ACD.BILL_ADDRESS_2,     ACD.BILL_ADDRESS_3,";
            strSQL += "ACD.BILL_CITY,              ACD.BILL_ZIP_CODE,      ACD.BILL_COUNTRY_MST_FK,";
            strSQL += "ACD.BILL_CONTACT_PERSON,    ACD.BILL_PHONE_NO_1,    ACD.BILL_PHONE_NO_2,";
            strSQL += "ACD.BILL_FAX_NO,            ACD.BILL_EMAIL_ID,      ACD.BILL_URL,";
            strSQL = strSQL + "ACD.BILL_SHORT_NAME,  ACD.BILL_SALUTATION,";
            // Master Table
            strSQL = strSQL + "ALM.VERSION_NO, ";

            strSQL = strSQL + "ADMCNR.COUNTRY_ID AS ADMCOUNTRYID,";
            strSQL = strSQL + "ADMCNR.COUNTRY_NAME AS ADMCOUNTRYNAME,";
            strSQL = strSQL + "BILLCNR.COUNTRY_ID BILLCOUNTRYID,";
            strSQL = strSQL + "BILLCNR.COUNTRY_NAME BILLCOUNTRYNAME,";
            strSQL = strSQL + "CORCNR.COUNTRY_ID CORCOUNTRYID,";
            strSQL = strSQL + "CORCNR.COUNTRY_NAME CORCOUNTRYNAME, ";
            strSQL = strSQL + " ALM.LOCATION_MST_FK,";
            strSQL = strSQL + " LOC.LOCATION_ID,";
            strSQL = strSQL + " LOC.LOCATION_NAME,";
            //code added by sumi to fetch remarks also
            //code added by LATHA to fetch remarks,FAX,PHONE AND EMAIL FROM LOCATION MASTER
            strSQL = strSQL + " Loc.REMARKS,";
            strSQL = strSQL + " Loc.FAX_NO,";
            strSQL = strSQL + " Loc.TELE_PHONE_NO,";
            strSQL = strSQL + " Loc.E_MAIL_ID, ";
            strSQL = strSQL + " ALM.AGENT_REF_NR ";
            //strSQL = strSQL & vbCrLf & " ALM.REMARKS"
            //end code---Sumi
            strSQL = strSQL + " FROM AGENT_MST_TBL ALM,AGENT_CONTACT_DTLS ACD,LOCATION_MST_TBL Loc, ";
            strSQL = strSQL + " COUNTRY_MST_TBL ADMCNR,";
            strSQL = strSQL + " COUNTRY_MST_TBL CORCNR,";
            strSQL = strSQL + " COUNTRY_MST_TBL BILLCNR";

            strSQL = strSQL + "WHERE ( 1 = 1) ";

            strSQL = strSQL + strCondition;

            try
            {
                DataSet ds = null;
                ds = objWF.GetDataSet(strSQL);
                // Adding a blank row
                if (NewRecord == true)
                {
                    ds.Tables[0].Rows.Add(ds.Tables[0].NewRow());
                    ds.Tables[0].Rows[0]["ACTIVE_FLAG"] = 1;
                }
                return ds;
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

        /// <summary>
        /// Fetches the location.
        /// </summary>
        /// <returns></returns>
        public string FetchLocation()
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                string LOC = null;
                strSQL = "";
                strSQL = "select Location_Id from location_mst_tbl where location_mst_pk=' " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + " '";
                LOC = objWF.ExecuteScaler(strSQL);
                return LOC;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the inv DTLS.
        /// </summary>
        /// <param name="FROM_DATE">The fro m_ date.</param>
        /// <param name="TO_DATE">The t o_ date.</param>
        /// <param name="Agent">The agent.</param>
        /// <param name="VslVoy">The VSL voy.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchInvDtls(string FROM_DATE, string TO_DATE, string Agent, string VslVoy, string Process)
        {
            string strSQL = null;
            DataSet ds = null;
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            Int32 strReturn = default(Int32);

            Array arr = null;
            string VslVoy1 = null;
            arr = VslVoy.Split('/');
            VslVoy = Convert.ToString(arr.GetValue(0));
            if (!string.IsNullOrEmpty(VslVoy))
            {
                VslVoy1 = Convert.ToString(arr.GetValue(1));
            }
            else
            {
                VslVoy1 = "";
            }

            if (!string.IsNullOrEmpty(Agent))
            {
                Agent = Convert.ToString(GetAgentsPK(Agent));
            }

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;

                if (Convert.ToInt32(Process) == 0)
                {
                    SCM.CommandText = objWF.MyUserName + ".FETCH_AGENCY_COMM_RPT.Fetch_Data_Agent_Exp";
                }
                else
                {
                    SCM.CommandText = objWF.MyUserName + ".FETCH_AGENCY_COMM_RPT.Fetch_Data_Agent_Imp";
                }

                var _with4 = SCM.Parameters;
                _with4.Add("FROM_DATE", getDefault(FROM_DATE, "01/01/1111")).Direction = ParameterDirection.Input;
                _with4.Add("TO_DATE", getDefault(TO_DATE, "")).Direction = ParameterDirection.Input;

                _with4.Add("AgentComm", getDefault(Agent, "0")).Direction = ParameterDirection.Input;
                _with4.Add("VslVoy", getDefault(VslVoy, "0")).Direction = ParameterDirection.Input;
                _with4.Add("VslVoy1", getDefault(VslVoy1, "0")).Direction = ParameterDirection.Input;

                _with4.Add("RETURN_VAL", OracleDbType.NVarchar2, 3000, "RETURN_VAL").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VAL"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToInt32(SCM.Parameters["RETURN_VAL"].Value);

                strSQL = "";
                strSQL = "select * from temp_agency_commision_tbl_rpt";
                ds = objWF.GetDataSet(strSQL);
                return ds;
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
            finally
            {
                objWF.MyCommand.Connection.Close();
            }
        }

        #endregion " Fetch Function "

        #region " General Functions "

        // Function to get City Name provided the City Key
        /// <summary>
        /// Selects the city.
        /// </summary>
        /// <param name="fkAdmincity">The fk admincity.</param>
        /// <returns></returns>
        public string SelectCity(int fkAdmincity)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select CITY_NAME from CITY_MST_TBL where CITY_PK = " + fkAdmincity + " ";
                string cityAdmin = null;
                cityAdmin = objWF.ExecuteScaler(strSQL);
                return cityAdmin;
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

        // Function to get Country Name provided the Country Key
        /// <summary>
        /// Selects the country.
        /// </summary>
        /// <param name="fkAdmincountry">The fk admincountry.</param>
        /// <returns></returns>
        public string SelectCountry(int fkAdmincountry)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select COUNTRY_NAME from COUNTRY_MST_TBL where COUNTRY_MST_PK = " + fkAdmincountry + " ";
                string cityAdmin = null;
                cityAdmin = objWF.ExecuteScaler(strSQL);
                return cityAdmin;
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

        // Function to get Location Name provided the Location Key
        /// <summary>
        /// Selects the location.
        /// </summary>
        /// <param name="fkLocation">The fk location.</param>
        /// <returns></returns>
        public string SelectLocation(int fkLocation)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = "select LOCATION_NAME from LOCATION_MST_TBL where LOCATION_MST_PK = " + fkLocation + " ";
                string LocName = null;
                LocName = objWF.ExecuteScaler(strSQL);
                return LocName;
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

        /// <summary>
        /// Ifs the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return col;
            }
        }

        /// <summary>
        /// Ifs the database zero.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <param name="Zero">The zero.</param>
        /// <returns></returns>
        private object ifDBZero(object col, Int16 Zero = 0)
        {
            if (Convert.ToInt32(col) == Zero)
            {
                return "";
            }
            else
            {
                return col;
            }
        }

        #endregion " General Functions "

        #region " Freight Element Function "

        // this function is written for four tables..
        // AGENT_CNT_EXP_PS_ELEM, AGENT_CNT_EXP_CM_ELEM, AGENT_CNT_IMP_PS_ELEM & AGENT_CNT_IMP_CM_ELEM
        // Any of these will be passed as TableName. The structure is almost similar
        // This will return a datatable consisting records for all selected and not selected
        // Freight elements for a particular agent code.
        // Following is teh structure of returning DataTable.
        //   > Freight Element ID
        //   > Freight Element Name
        //   > Selected
        //   > Freight Element Mst Pk
        //   > Upd Flag

        /// <summary>
        /// Selecteds the freight elements.
        /// </summary>
        /// <param name="TablNo">The tabl no.</param>
        /// <param name="AgentCode">The agent code.</param>
        /// <param name="BusinessType">Type of the business.</param>
        /// <param name="ChargeType">Type of the charge.</param>
        /// <returns></returns>
        public DataTable SelectedFreightElements(Int16 TablNo = 0, string AgentCode = "NULL", string BusinessType = "3", int ChargeType = 1)
        {
            // Getting DataBase Table Name
            string TableName = null;
            string AnotherTable = null;
            switch (TablNo)
            {
                case 0:
                    TableName = "AGENT_CNT_EXP_PS_ELEM";
                    AnotherTable = "AGENT_CNT_EXP_CM_ELEM";
                    break;

                case 1:
                    TableName = "AGENT_CNT_EXP_CM_ELEM";
                    AnotherTable = "AGENT_CNT_EXP_PS_ELEM";
                    break;

                case 2:
                    TableName = "AGENT_CNT_IMP_PS_ELEM";
                    AnotherTable = "AGENT_CNT_IMP_CM_ELEM";
                    break;

                default:
                    TableName = "AGENT_CNT_IMP_CM_ELEM";
                    AnotherTable = "AGENT_CNT_IMP_PS_ELEM";
                    break;
            }

            string strCondition = null;

            // Active Flag Condition
            strCondition = " ACTIVE_FLAG = 1 and ";

            // Business Type Condition
            if (BusinessType == "1")
            {
                strCondition += " BUSINESS_TYPE in(1,3) and ";
            }
            else if (BusinessType == "2")
            {
                strCondition += " BUSINESS_TYPE in(2,3) and ";
            }
            if (ChargeType == 1)
            {
                strCondition += "  CHARGE_TYPE = 1 AND";
            }

            string ConditionOperator = null;
            ConditionOperator = (AgentCode == "NULL" ? " IS " : " = ");
            string strSQL = " Select ";
            strSQL += "    FREIGHT_ELEMENT_ID, ";
            strSQL += "    FREIGHT_ELEMENT_NAME, ";
            strSQL += "    0 SELECTED, ";
            strSQL += "    FREIGHT_ELEMENT_MST_PK, ";
            strSQL += "" + AgentCode + " AGENT_PK, ";
            strSQL += "    0 UPDFLAG ";
            strSQL += "    from ";
            strSQL += "    FREIGHT_ELEMENT_MST_TBL ";
            strSQL += "    where ";
            strSQL += "" + strCondition;
            strSQL += "    FREIGHT_ELEMENT_MST_PK not in ";
            strSQL += "    ( Select ";
            strSQL += "        FREIGHT_ELEMENT_MST_FK from ";
            strSQL += "      " + TableName + " where ";
            strSQL += "        AGENT_MST_FK " + ConditionOperator + AgentCode;
            strSQL += "    ) ";
            strSQL += " union ";
            strSQL += " Select ";
            strSQL += "    FREIGHT_ELEMENT_ID, ";
            strSQL += "    FREIGHT_ELEMENT_NAME, ";
            strSQL += "    1 SELECTED, ";
            strSQL += "    FREIGHT_ELEMENT_MST_PK, ";
            strSQL += "" + AgentCode + " AGENT_PK, ";
            strSQL += "    0 UPDFLAG ";
            strSQL += "    from ";
            strSQL += "    FREIGHT_ELEMENT_MST_TBL ";
            strSQL += "    where ";
            strSQL += "" + strCondition;
            strSQL += "    FREIGHT_ELEMENT_MST_PK in ";
            strSQL += "    ( Select ";
            strSQL += "        FREIGHT_ELEMENT_MST_FK from ";
            strSQL += "      " + TableName + " where ";
            strSQL += "        AGENT_MST_FK " + ConditionOperator + AgentCode;
            strSQL += "    ) ";
            strSQL += " Order By ";
            strSQL += " SELECTED DESC, ";
            strSQL += " FREIGHT_ELEMENT_ID ASC ";

            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(strSQL).Tables[0];
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

        /// <summary>
        /// Fetches the prof sharing and comm.
        /// </summary>
        /// <param name="AGENT_MST_PK">The agen t_ ms t_ pk.</param>
        /// <returns></returns>
        public DataSet FetchProfSharingAndComm(int AGENT_MST_PK)
        {
            WorkFlow objWf = new WorkFlow();
            DataSet dsPS = new DataSet();
            objWf.OpenConnection();
            try
            {
                objWf.MyCommand = new OracleCommand();
                var _with8 = objWf.MyCommand;
                _with8.Connection = objWf.MyConnection;
                _with8.Parameters.Clear();
                _with8.Parameters.Add("AGENT_MST_PK_IN", AGENT_MST_PK).Direction = ParameterDirection.Input;
                _with8.Parameters.Add("FETCH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsPS = objWf.GetDataSet("AGENCY_MST_TBL_PKG", "FETCH_AGENT_PROFIT_SHARING");
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return dsPS;
        }

        /// <summary>
        /// Fetches the prof sharing FRT TRN.
        /// </summary>
        /// <param name="AGENT_CNT_ELEM_PK">The agen t_ cn t_ ele m_ pk.</param>
        /// <param name="PROCESS_TYPE">Type of the proces s_.</param>
        /// <param name="AGENT_BIZ_TYPE">Type of the agen t_ bi z_.</param>
        /// <param name="ONLY_EXISTING">if set to <c>true</c> [onl y_ existing].</param>
        /// <returns></returns>
        public DataTable FetchProfSharingFrtTRN(int AGENT_CNT_ELEM_PK, short PROCESS_TYPE, short AGENT_BIZ_TYPE, bool ONLY_EXISTING = false)
        {
            WorkFlow objWf = new WorkFlow();
            DataTable dtPS = new DataTable();
            objWf.OpenConnection();
            try
            {
                objWf.MyCommand = new OracleCommand();
                var _with9 = objWf.MyCommand;
                _with9.Connection = objWf.MyConnection;
                _with9.Parameters.Clear();
                _with9.Parameters.Add("AGENT_CNT_ELEM_PK_IN", AGENT_CNT_ELEM_PK).Direction = ParameterDirection.Input;
                _with9.Parameters.Add("PROCESS_TYPE_IN", PROCESS_TYPE).Direction = ParameterDirection.Input;
                _with9.Parameters.Add("AGENT_BIZ_TYPE_IN", AGENT_BIZ_TYPE).Direction = ParameterDirection.Input;
                _with9.Parameters.Add("ONLY_EXISTING_IN", (ONLY_EXISTING ? 1 : 0)).Direction = ParameterDirection.Input;
                _with9.Parameters.Add("FETCH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtPS = objWf.GetDataTable("AGENCY_MST_TBL_PKG", "FETCH_AGENT_PROFIT_FRT_TRN");
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return dtPS;
        }

        #endregion " Freight Element Function "

        #region "Fetch Agent"

        // Enhance Search Function...
        /// <summary>
        /// Fetches the agent comm1.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchAgentComm1(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strReq = null;
            string businessType = null;
            string Import = "";
            string Agnt = "";
            string Agent = "";
            string pod = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            strLOC_MST_IN = Convert.ToString(HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            if (arr.Length > 3)
                businessType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                Agnt = Convert.ToString(arr.GetValue(4));
            //Added by Manoharan 06Feb2007: for XBkg & CoLoad Agent enhance search
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                pod = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                Agent = Convert.ToString(arr.GetValue(7));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_AGENT_PKG.GETAGENT_COMMISION";
                var _with10 = SCM.Parameters;
                _with10.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with10.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOC_MST_IN) ? strLOC_MST_IN : "")).Direction = ParameterDirection.Input;
                _with10.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with10.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with10.Add("AGENT_TYPE_IN", ifDBNull(Agnt)).Direction = ParameterDirection.Input;
                //Added by Manoharan 06Feb2007: for XBkg & CoLoad Agent enhance search
                _with10.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input;
                _with10.Add("POD_FK_IN", ifDBNull(pod)).Direction = ParameterDirection.Input;
                _with10.Add("AGENT_IN", ifDBNull(Agent)).Direction = ParameterDirection.Input;
                //    .Add("PROCESS_IN", ifDBNull(PROCESS)).Direction = ParameterDirection.Input
                _with10.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleBlob clob = null;
                clob = (OracleBlob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
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

        /// <summary>
        /// Fetches the agent comm.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchAgentComm(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strReq = null;
            string businessType = null;
            string Import = "";
            string Agnt = "";
            string Agent = "";
            string pod = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            strLOC_MST_IN = Convert.ToString(HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            if (arr.Length > 3)
                businessType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                Agnt = Convert.ToString(arr.GetValue(4));
            //Added by Manoharan 06Feb2007: for XBkg & CoLoad Agent enhance search
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                pod = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                Agent = Convert.ToString(arr.GetValue(7));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_AGENT_PKG.GETAGENT_COMMISION";
                var _with11 = SCM.Parameters;
                _with11.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with11.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOC_MST_IN) ? strLOC_MST_IN : "")).Direction = ParameterDirection.Input;
                _with11.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with11.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with11.Add("AGENT_TYPE_IN", ifDBNull(Agnt)).Direction = ParameterDirection.Input;
                //Added by Manoharan 06Feb2007: for XBkg & CoLoad Agent enhance search
                _with11.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input;
                _with11.Add("POD_FK_IN", ifDBNull(pod)).Direction = ParameterDirection.Input;
                _with11.Add("AGENT_IN", ifDBNull(Agent)).Direction = ParameterDirection.Input;
                _with11.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
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

        /// <summary>
        /// Fetches the agent.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchAgent(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strReq = null;
            string businessType = null;
            string Import = "";
            string Agnt = "";
            string Agent = "";
            string pod = "";
            string Active = "1";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(0));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                businessType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                Agnt = Convert.ToString(arr.GetValue(4));
            //Added by Manoharan 06Feb2007: for XBkg & CoLoad Agent enhance search
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                pod = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                Agent = Convert.ToString(arr.GetValue(7));
            if (arr.Length > 8)
                Active = Convert.ToString(arr.GetValue(8));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_AGENT_PKG.GETAGENT_COMMON";
                var _with12 = SCM.Parameters;
                _with12.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with12.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOC_MST_IN) ? strLOC_MST_IN : "")).Direction = ParameterDirection.Input;
                _with12.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with12.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with12.Add("AGENT_TYPE_IN", ifDBNull(Agnt)).Direction = ParameterDirection.Input;
                //Added by Manoharan 06Feb2007: for XBkg & CoLoad Agent enhance search
                _with12.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input;
                _with12.Add("POD_FK_IN", ifDBNull(pod)).Direction = ParameterDirection.Input;
                _with12.Add("AGENT_IN", ifDBNull(Agent)).Direction = ParameterDirection.Input;
                _with12.Add("ACTIVE_FLAG_IN", ifDBNull(Active)).Direction = ParameterDirection.Input;
                _with12.Add("RETURN_VALUE", OracleDbType.NVarchar2, 32767, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
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

        /// <summary>
        /// Fetches all agents.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchAllAgents(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strReq = null;
            string businessType = null;
            string PrcessType = null;
            string JobPK = null;
            string CollType = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                businessType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                PrcessType = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                JobPK = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                CollType = Convert.ToString(arr.GetValue(6));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_AGENT_PKG.GET_ALL_AGENTS";
                var _with13 = SCM.Parameters;
                _with13.Add("SEARCH_IN", getDefault(strSERACH_IN, "")).Direction = ParameterDirection.Input;
                _with13.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with13.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with13.Add("PROCESS_TYPE_IN", PrcessType).Direction = ParameterDirection.Input;
                _with13.Add("JOB_CARD_PK_IN", getDefault(JobPK, "")).Direction = ParameterDirection.Input;
                _with13.Add("COLLECTION_TYPE_IN", getDefault(CollType, 0)).Direction = ParameterDirection.Input;
                _with13.Add("LOCATION_PK_IN", HttpContext.Current.Session["LOGED_IN_LOC_FK"]).Direction = ParameterDirection.Input;
                _with13.Add("RETURN_VALUE", OracleDbType.NVarchar2, 32767, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
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

        #endregion "Fetch Agent"

        #region "Update File Name"

        /// <summary>
        /// Updates the name of the file.
        /// </summary>
        /// <param name="AgentPk">The agent pk.</param>
        /// <param name="strFileName">Name of the string file.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public bool UpdateFileName(long AgentPk, string strFileName, Int16 Flag)
        {
            if (strFileName.Trim().Length > 0)
            {
                string RemQuery = null;
                WorkFlow objwk = new WorkFlow();
                if (Flag == 1)
                {
                    RemQuery = " UPDATE AGENT_MST_TBL AMT SET AMT.ATTACHED_FILE_NAME='" + strFileName + "'";
                    RemQuery += " WHERE AMT.AGENT_MST_PK= " + AgentPk;
                    try
                    {
                        objwk.OpenConnection();
                        objwk.ExecuteCommands(RemQuery);
                        return true;
                    }
                    catch (Exception ex)
                    {
                        return false;
                        throw ex;
                        //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
                    }
                    finally
                    {
                        objwk.MyCommand.Connection.Close();
                    }
                }
                else if (Flag == 2)
                {
                    RemQuery = " UPDATE AGENT_MST_TBL AMT SET AMT.ATTACHED_FILE_NAME='" + "" + "'";
                    RemQuery += " WHERE AMT.AGENT_MST_PK= " + AgentPk;
                    try
                    {
                        objwk.OpenConnection();
                        objwk.ExecuteCommands(RemQuery);
                        return true;
                    }
                    catch (OracleException OraExp)
                    {
                        throw OraExp;
                    }
                    catch (Exception ex)
                    {
                        return false;
                        throw ex;
                        //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
                    }
                    finally
                    {
                        objwk.MyCommand.Connection.Close();
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// Fetches the name of the file.
        /// </summary>
        /// <param name="AGENTPK">The agentpk.</param>
        /// <returns></returns>
        public string FetchFileName(long AGENTPK)
        {
            string strSQL = null;
            string strUpdFileName = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " SELECT ";
            strSQL += " AMT.ATTACHED_FILE_NAME FROM AGENT_MST_TBL AMT  WHERE AMT.AGENT_MST_PK = " + AGENTPK;
            try
            {
                strUpdFileName = objWF.ExecuteScaler(strSQL);
                return strUpdFileName;
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

        #endregion "Update File Name"

        #region "Enhance Search Functionality For DP Agent"

        //For Fetching DP Agent With POD's Location
        /// <summary>
        /// Fetches the dp agent.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchDpAgent(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strReq = null;
            string businessType = null;
            string SearchFlag = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                businessType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                SearchFlag = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_DPAGENT_PKG.GETAGENT_COMMON";
                var _with14 = SCM.Parameters;
                _with14.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with14.Add("POD_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with14.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with14.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with14.Add("SEARCH_FLAG_IN", ifDBNull(SearchFlag)).Direction = ParameterDirection.Input;
                _with14.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        #endregion "Enhance Search Functionality For DP Agent"

        //Fetch Load Port Agent
        //By Amit Singh on 10-April-2007

        #region "Enhance Search Functionality For LP Agent"

        /// <summary>
        /// Fetches the lp agent.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchLpAgent(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strLOC_IN = "";
            string strReq = null;
            string businessType = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                businessType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strLOC_IN = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_LPAGENT_PKG.GETLOADPORT_AGENT";
                var _with15 = SCM.Parameters;
                _with15.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with15.Add("POL_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                //.Add("LOC_FK_IN", ifDBNull(CInt(strLOC_IN])).Direction = ParameterDirection.Input
                _with15.Add("LOC_FK_IN", (string.IsNullOrEmpty(strLOC_IN) ? "" : strLOC_IN)).Direction = ParameterDirection.Input;
                _with15.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with15.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with15.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
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

        #endregion "Enhance Search Functionality For LP Agent"

        #region "Fetch and Save Other charges - Grid data"

        /// <summary>
        /// Fetch_s the other_ charge.
        /// </summary>
        /// <param name="AGENTPK">The agentpk.</param>
        /// <returns></returns>
        public DataSet Fetch_Other_Charge(string AGENTPK = "")
        {
            WorkFlow objWf = new WorkFlow();
            System.Text.StringBuilder strsql = new System.Text.StringBuilder();
            DataSet ds = new DataSet();

            strsql.Append("  select ROWNUM SLNO, trans.cost_element_mst_fk COST_PK, cet.cost_element_id COST_ID, ");
            strsql.Append("  cet.cost_element_name COST_DESC, decode(trans.basis,0,'TEU',1,'BOX',2,'BL') BASIS, ");
            strsql.Append("  trans.currency_type_mst_fk CURRENCY_PK, curr.currency_id CURRENCY, trans.amount AMOUNT, ");
            strsql.Append("  trans.agent_tranship_pk GRIDPK, trans.basis BASIS from AGENT_TRANSHIP_TRN trans, agent_mst_tbl agt, ");
            strsql.Append("  cost_element_mst_tbl cet, currency_type_mst_tbl curr where agt.agent_mst_pk = trans.agent_mst_fk ");
            strsql.Append("  and trans.cost_element_mst_fk = cet.cost_element_mst_pk and trans.currency_type_mst_fk = curr.currency_mst_pk ");
            if (!string.IsNullOrEmpty(AGENTPK))
            {
                strsql.Append("  and agt.agent_mst_pk = " + AGENTPK);
            }
            else
            {
                strsql.Append("  and agt.agent_mst_pk = -1");
            }
            try
            {
                ds = objWf.GetDataSet(strsql.ToString());
                return ds;
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

        /// <summary>
        /// Saves the grid data.
        /// </summary>
        /// <param name="DS">The ds.</param>
        /// <param name="AgentPK">The agent pk.</param>
        /// <returns></returns>
        public ArrayList SaveGridData(DataSet DS, string AgentPK)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            try
            {
                var _with16 = insCommand;
                _with16.Connection = objWK.MyConnection;
                _with16.CommandType = CommandType.StoredProcedure;
                _with16.CommandText = objWK.MyUserName + ".AGENCY_MST_TBL_PKG.AGENT_TRANSHIP_TRN_INS";
                var _with17 = _with16.Parameters;
                insCommand.Parameters.Add("AGENT_MST_FK_IN", AgentPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("COST_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "COST_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("BASIS_IN", OracleDbType.Int32, 1, "BASIS").Direction = ParameterDirection.Input;
                insCommand.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("CURRENCY_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_PK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CURRENCY_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommand.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "AMOUNT").Direction = ParameterDirection.Input;
                insCommand.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                var _with18 = updCommand;
                _with18.Connection = objWK.MyConnection;
                _with18.CommandType = CommandType.StoredProcedure;
                _with18.CommandText = objWK.MyUserName + ".AGENCY_MST_TBL_PKG.AGENT_TRANSHIP_TRN_UPD";
                var _with19 = _with18.Parameters;
                updCommand.Parameters.Add("AGENT_TRANSHIP_PK_IN", OracleDbType.Int32, 10, "GRIDPK").Direction = ParameterDirection.Input;
                updCommand.Parameters["AGENT_TRANSHIP_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AGENT_MST_FK_IN", AgentPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("COST_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "COST_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("BASIS_IN", OracleDbType.Varchar2, 1, "BASIS").Direction = ParameterDirection.Input;
                updCommand.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("CURRENCY_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CURRENCY_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "AMOUNT").Direction = ParameterDirection.Input;
                updCommand.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with20 = objWK.MyDataAdapter;

                _with20.InsertCommand = insCommand;
                _with20.InsertCommand.Transaction = TRAN;

                _with20.UpdateCommand = updCommand;
                _with20.UpdateCommand.Transaction = TRAN;

                RecAfct = _with20.Update(DS);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
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

        #endregion "Fetch and Save Other charges - Grid data"

        // "Added by Faheem"

        #region " Fetch All "

        /// <summary>
        /// Fetches the fac calculation.
        /// </summary>
        /// <param name="OpratorPK">The oprator pk.</param>
        /// <param name="VesselPK">The vessel pk.</param>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <param name="MBLDate">The MBL date.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="CARGO_TYPE">Type of the carg o_.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <returns></returns>
        public DataSet FetchFACCalculation(int OpratorPK = 0, string VesselPK = "", int MBLPK = 0, string MBLDate = "", string FromDate = "", string ToDate = "", int POLPK = 0, int PODPK = 0, int CARGO_TYPE = 0, int BIZ_TYPE = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = null;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            try
            {
                //'SEA
                if (BIZ_TYPE == 2)
                {
                    sb.Append(" SELECT * FROM ( SELECT MBL_EXP_TBL_PK,");
                    sb.Append("       MBL_REF_NO,");
                    sb.Append("       MBL_DATE,");
                    sb.Append("       OPERATOR_ID,");
                    sb.Append("       OPERATOR_NAME,");
                    sb.Append("       VOYAGE_TRN_FK,");
                    sb.Append("       VESSEL_NAME,");
                    sb.Append("       VOYAGE,");
                    sb.Append("       POL,");
                    sb.Append("       POD,");
                    sb.Append("       POL_ETD,");
                    sb.Append("       POD_ETA,");
                    sb.Append("       SUM(TEU) TEU,");
                    sb.Append("       CARGO_TYPE,");
                    sb.Append("       SUM(AIF) AIF,");
                    sb.Append("       SUM(BOF_AMT) BOF_AMT,");
                    sb.Append("       FAC_BASIS,");
                    //sb.Append("       SUM(FAC_AMT) FAC_AMT,")
                    sb.Append(" GET_FAC_AMT(OPERATOR_MST_PK, MBL_EXP_TBL_PK, " + BaseCurrFk + ",2) FAC_AMT,");
                    sb.Append("       INV,");
                    sb.Append("       COL,");
                    sb.Append("       OPERATOR_MST_PK");
                    sb.Append("  FROM ( ");
                    sb.Append(" SELECT DISTINCT MBL.MBL_EXP_TBL_PK,");
                    sb.Append("                MBL.MBL_REF_NO,");
                    sb.Append("                TO_CHAR(MBL.MBL_DATE, DATEFORMAT) MBL_DATE,");
                    sb.Append("                OPER.OPERATOR_ID,");
                    sb.Append("                OPER.OPERATOR_NAME,");
                    sb.Append("                MBL.VOYAGE_TRN_FK,");
                    sb.Append("                VVT.VESSEL_NAME,");
                    sb.Append("                VT.VOYAGE,");
                    sb.Append("                POL.PORT_NAME POL,");
                    sb.Append("                POD.PORT_NAME POD,");
                    sb.Append("                VT.POL_ETD,");
                    sb.Append("                VT.POD_ETA,");
                    sb.Append("                (SELECT SUM(NVL(CTMT.TEU_FACTOR, 0))");
                    sb.Append("                   FROM JOB_TRN_CONT   CONT,");
                    sb.Append("                        CONTAINER_TYPE_MST_TBL CTMT");
                    sb.Append("                  WHERE (CONT.JOB_CARD_TRN_FK =");
                    sb.Append("                        JOB_EXP.JOB_CARD_TRN_PK)");
                    sb.Append("                    AND CONT.CONTAINER_TYPE_MST_FK =");
                    sb.Append("                        CTMT.CONTAINER_TYPE_MST_PK(+)) TEU,");
                    sb.Append("                DECODE(BKG_SEA.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                    sb.Append("                SUM(GET_EX_RATE_BUY(CURR_TP.CURRENCY_MST_PK, " + BaseCurrFk + ", JOB_EXP.JOBCARD_DATE) *");
                    sb.Append("                    (JCST.ESTIMATED_COST)) AIF,");
                    sb.Append("                SUM(GET_EX_RATE_BUY(CURR_TP.CURRENCY_MST_PK, " + BaseCurrFk + ", JOB_EXP.JOBCARD_DATE) *");
                    sb.Append("                    (SELECT JC.ESTIMATED_COST");
                    sb.Append("                       FROM JOB_TRN_COST JC");
                    sb.Append("                      WHERE JC.JOB_TRN_COST_PK =");
                    sb.Append("                            JCST.JOB_TRN_COST_PK");
                    sb.Append("                          AND JC.COST_ELEMENT_MST_FK = CST.COST_ELEMENT_MST_PK ");
                    sb.Append("                        AND CST.COST_ELEMENT_ID = 'BOF')) BOF_AMT,");
                    sb.Append("                '' FAC_BASIS,");
                    //sb.Append("                SUM(GET_EX_RATE(CURR_TP.CURRENCY_MST_PK, " & BaseCurrFk & ", JOB_EXP.JOBCARD_DATE) *")
                    //sb.Append("                    ((FAC.COMMISSION / 100) * JCST.ESTIMATED_COST)) FAC_AMT,")
                    sb.Append("                '' INV,");
                    sb.Append("                '' COL,OPER.OPERATOR_MST_PK ");
                    sb.Append("  FROM JOB_CARD_TRN  JOB_EXP,");
                    sb.Append("       VESSEL_VOYAGE_TBL     VVT,");
                    sb.Append("       VESSEL_VOYAGE_TRN     VT,");
                    sb.Append("       MBL_EXP_TBL           MBL,");
                    sb.Append("       PORT_MST_TBL          POL,");
                    sb.Append("       PORT_MST_TBL          POD,");
                    sb.Append("       COST_ELEMENT_MST_TBL  CST,");
                    sb.Append("       JOB_TRN_COST  JCST,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL CURR_TP,");
                    sb.Append("       OPERATOR_MST_TBL      OPER,");
                    //sb.Append("       FAC_SETUP_TBL         FAC,")
                    sb.Append("       booking_mst_tbl       BKG_SEA,");
                    sb.Append("       USER_MST_TBL          UMST");
                    sb.Append(" WHERE JOB_EXP.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK");
                    sb.Append("   AND VT.VESSEL_VOYAGE_TBL_FK = VVT.VESSEL_VOYAGE_TBL_PK");
                    sb.Append("   AND MBL.MBL_EXP_TBL_PK = JOB_EXP.MBL_MAWB_FK");
                    sb.Append("   AND JOB_EXP.booking_mst_FK = BKG_SEA.booking_mst_PK");
                    sb.Append("   AND BKG_SEA.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND BKG_SEA.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND JCST.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
                    sb.Append("   AND CST.COST_ELEMENT_MST_PK = JCST.COST_ELEMENT_MST_FK");
                    //sb.Append("   AND JCST.COST_ELEMENT_MST_FK = FAC.COST_ELEMENT_FK")
                    sb.Append("   AND JCST.CURRENCY_MST_FK = CURR_TP.CURRENCY_MST_PK");
                    sb.Append("   AND BKG_SEA.CARRIER_MST_FK = OPER.OPERATOR_MST_PK AND BKG_SEA.BUSINESS_TYPE=2");
                    //sb.Append("   AND OPER.OPERATOR_MST_PK = FAC.OPERATOR_MST_FK")
                    sb.Append("   AND BKG_SEA.CARGO_TYPE = " + CARGO_TYPE);
                    if (OpratorPK > 0)
                    {
                        sb.Append("   AND OPER.OPERATOR_MST_PK = " + OpratorPK);
                    }
                    if (MBLPK > 0)
                    {
                        sb.Append("   AND MBL.MBL_EXP_TBL_PK = " + MBLPK);
                    }
                    if (POLPK > 0)
                    {
                        sb.Append("   AND POL.PORT_MST_PK = " + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        sb.Append("   AND POD.PORT_MST_PK = " + PODPK);
                    }
                    if (!string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
                    {
                        sb.Append("   AND (BKG_SEA.BOOKING_DATE BETWEEN TO_DATE('" + FromDate + "', '" + dateFormat + "') AND");
                        sb.Append("       TO_DATE('" + ToDate + "', '" + dateFormat + "'))");
                    }
                    sb.Append("   AND UMST.USER_MST_PK = JOB_EXP.CREATED_BY_FK");
                    sb.Append("   AND UMST.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                    sb.Append(" GROUP BY MBL_EXP_TBL_PK,");
                    sb.Append("          MBL_REF_NO,");
                    sb.Append("          MBL.MBL_DATE,");
                    sb.Append("          OPER.OPERATOR_MST_PK,OPER.OPERATOR_ID,");
                    sb.Append("          OPER.OPERATOR_NAME,");
                    sb.Append("          MBL.VOYAGE_TRN_FK,");
                    sb.Append("          VVT.VESSEL_NAME,");
                    sb.Append("          VT.VOYAGE,");
                    sb.Append("          POL.PORT_NAME,");
                    sb.Append("          POD.PORT_NAME,");
                    sb.Append("          VT.POL_ETD,");
                    sb.Append("          VT.POD_ETA,");
                    sb.Append("          JOB_EXP.JOB_CARD_TRN_PK,");
                    sb.Append("          BKG_SEA.CARGO_TYPE ");
                    sb.Append(" ) Q ");
                    sb.Append(" GROUP BY MBL_EXP_TBL_PK,");
                    sb.Append("          MBL_REF_NO,");
                    sb.Append("          MBL_DATE,");
                    sb.Append("          OPERATOR_ID,");
                    sb.Append("          OPERATOR_NAME,");
                    sb.Append("          VOYAGE_TRN_FK,");
                    sb.Append("          VESSEL_NAME,");
                    sb.Append("          VOYAGE,");
                    sb.Append("          POL,");
                    sb.Append("          POD,");
                    sb.Append("          POL_ETD,");
                    sb.Append("          POD_ETA,");
                    sb.Append("          CARGO_TYPE,");
                    sb.Append("          INV,");
                    sb.Append("          COL,");
                    sb.Append("          OPERATOR_MST_PK) WHERE FAC_AMT IS NOT NULL ");
                    if (VesselPK != "0")
                    {
                        sb.Append("   AND VOYAGE_TRN_FK = " + VesselPK);
                    }
                    sb.Append("         ORDER BY  FAC_AMT DESC ");

                    DS = objWF.GetDataSet(sb.ToString());
                    //'Parent Table
                    sb.Remove(0, sb.Length);

                    sb.Append("SELECT DISTINCT MBL.MBL_EXP_TBL_PK,");
                    sb.Append("                JOB_EXP.JOB_CARD_TRN_PK,");
                    sb.Append("                NVL(HBL.HBL_EXP_TBL_PK,0)HBL_EXP_TBL_PK,");
                    sb.Append("                HBL.HBL_REF_NO,");
                    sb.Append("                TO_CHAR(HBL.HBL_DATE, DATEFORMAT) HBL_DATE,");
                    sb.Append("                CASE WHEN MBL.CARGO_TYPE=4 THEN ");
                    sb.Append("                  COMM.COMMODITY_ID");
                    sb.Append("                ELSE");
                    sb.Append("                  CMT.CONTAINER_TYPE_MST_ID");
                    sb.Append("                END CONTAINER_TYPE_MST_ID,");
                    sb.Append("                CASE WHEN MBL.CARGO_TYPE=4 THEN");
                    sb.Append("                  COMM.COMMODITY_NAME ");
                    sb.Append("                ELSE");
                    sb.Append("                  JCT.CONTAINER_NUMBER");
                    sb.Append("                END CONTAINER_NUMBER,");
                    //sb.Append("                CMT.CONTAINER_TYPE_MST_ID,")
                    //sb.Append("                JCT.CONTAINER_NUMBER,")
                    sb.Append("                JCT.SEAL_NUMBER,");
                    sb.Append("                '' COMMODITY,");
                    sb.Append("                JCT.NET_WEIGHT,");
                    sb.Append("                JCT.VOLUME_IN_CBM,");
                    sb.Append("                SUM(GET_EX_RATE_BUY(CURR_TP.CURRENCY_MST_PK, " + BaseCurrFk + ", JOB_EXP.JOBCARD_DATE) *");
                    sb.Append("                    (SELECT JC.ESTIMATED_COST");
                    sb.Append("                       FROM JOB_TRN_COST JC");
                    sb.Append("                      WHERE JC.JOB_TRN_COST_PK =");
                    sb.Append("                            JCST.JOB_TRN_COST_PK");
                    sb.Append("                          AND JC.COST_ELEMENT_MST_FK = CST.COST_ELEMENT_MST_PK ");
                    sb.Append("                        AND CST.COST_ELEMENT_ID = 'BOF')) BOF_AMT,");
                    sb.Append("                '' FAC_BASIS,");
                    sb.Append("                SUM(GET_EX_RATE_BUY(CURR_TP.CURRENCY_MST_PK, " + BaseCurrFk + ", JOB_EXP.JOBCARD_DATE) *");
                    sb.Append("                    ((FAC.COMMISSION / 100) * JCST.ESTIMATED_COST)) FAC_AMT,");
                    sb.Append("                '' INV,");
                    sb.Append("                '' COL,JOB_EXP.COMMODITY_GROUP_FK, JCT.COMMODITY_MST_FKS ");
                    sb.Append("  FROM JOB_CARD_TRN   JOB_EXP,");
                    sb.Append("       VESSEL_VOYAGE_TBL      VVT,");
                    sb.Append("       VESSEL_VOYAGE_TRN      VT,");
                    sb.Append("       MBL_EXP_TBL            MBL,");
                    sb.Append("       HBL_EXP_TBL            HBL,");
                    sb.Append("       PORT_MST_TBL           POL,");
                    sb.Append("       PORT_MST_TBL           POD,");
                    sb.Append("       COST_ELEMENT_MST_TBL   CST,");
                    sb.Append("       JOB_TRN_COST   JCST,");
                    sb.Append("       CONTAINER_TYPE_MST_TBL CMT,");
                    sb.Append("       COMMODITY_MST_TBL COMM, ");
                    sb.Append("       JOB_TRN_CONT   JCT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL  CURR_TP,");
                    sb.Append("       OPERATOR_MST_TBL       OPER,");
                    sb.Append("       FAC_SETUP_TBL          FAC,");
                    sb.Append("       booking_mst_tbl        BKG_SEA,");
                    sb.Append("       USER_MST_TBL           UMST");
                    sb.Append(" WHERE JOB_EXP.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK");
                    sb.Append("   AND VT.VESSEL_VOYAGE_TBL_FK = VVT.VESSEL_VOYAGE_TBL_PK");
                    sb.Append("   AND JCT.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
                    sb.Append("   AND HBL.JOB_CARD_SEA_EXP_FK(+) = JOB_EXP.JOB_CARD_TRN_PK");
                    sb.Append("   AND JCT.CONTAINER_TYPE_MST_FK = CMT.CONTAINER_TYPE_MST_PK(+)");
                    sb.Append("   AND JCT.COMMODITY_MST_FK = COMM.COMMODITY_MST_PK(+) ");
                    sb.Append("   AND MBL.MBL_EXP_TBL_PK = JOB_EXP.MBL_MAWB_FK");
                    sb.Append("   AND JOB_EXP.booking_mst_FK = BKG_SEA.booking_mst_PK");
                    sb.Append("   AND BKG_SEA.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND BKG_SEA.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND JCST.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
                    sb.Append("   AND CST.COST_ELEMENT_MST_PK = JCST.COST_ELEMENT_MST_FK");
                    sb.Append("   AND JCST.COST_ELEMENT_MST_FK = FAC.COST_ELEMENT_FK");
                    sb.Append("   AND JCST.CURRENCY_MST_FK = CURR_TP.CURRENCY_MST_PK");
                    sb.Append("   AND BKG_SEA.CARRIER_MST_FK = OPER.OPERATOR_MST_PK AND BKG_SEA.BUSINESS_TYPE=2");
                    sb.Append("   AND OPER.OPERATOR_MST_PK = FAC.OPERATOR_MST_FK");
                    sb.Append("   AND HBL.HBL_REF_NO IS NOT NULL ");
                    sb.Append("   AND BKG_SEA.CARGO_TYPE = " + CARGO_TYPE);
                    if (OpratorPK > 0)
                    {
                        sb.Append("   AND OPER.OPERATOR_MST_PK = " + OpratorPK);
                    }
                    if (MBLPK > 0)
                    {
                        sb.Append("   AND MBL.MBL_EXP_TBL_PK = " + MBLPK);
                    }
                    if (POLPK > 0)
                    {
                        sb.Append("   AND POL.PORT_MST_PK = " + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        sb.Append("   AND POD.PORT_MST_PK = " + PODPK);
                    }
                    if (VesselPK != "0")
                    {
                        sb.Append("   AND VT.VOYAGE_TRN_PK = " + VesselPK);
                    }
                    if (!string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
                    {
                        sb.Append("   AND (BKG_SEA.BOOKING_DATE BETWEEN TO_DATE('" + FromDate + "', '" + dateFormat + "') AND");
                        sb.Append("       TO_DATE('" + ToDate + "', '" + dateFormat + "'))");
                    }
                    sb.Append("   AND UMST.USER_MST_PK = JOB_EXP.CREATED_BY_FK");
                    sb.Append("   AND UMST.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                    sb.Append(" GROUP BY MBL.MBL_EXP_TBL_PK,");
                    sb.Append("          JOB_EXP.JOB_CARD_TRN_PK,");
                    sb.Append("          HBL.HBL_EXP_TBL_PK,");
                    sb.Append("          HBL.HBL_REF_NO,");
                    sb.Append("          HBL.HBL_DATE,");
                    sb.Append("          CMT.CONTAINER_TYPE_MST_ID,");
                    sb.Append("          JCT.CONTAINER_NUMBER,");
                    sb.Append("          JCT.SEAL_NUMBER,");
                    sb.Append("          JCT.NET_WEIGHT,");
                    sb.Append("          JCT.VOLUME_IN_CBM,");
                    sb.Append(" MBL.CARGO_TYPE, COMM.COMMODITY_ID,COMM.COMMODITY_NAME,JOB_EXP.COMMODITY_GROUP_FK,JCT.COMMODITY_MST_FKS ");

                    DS.Tables.Add(objWF.GetDataTable(sb.ToString()));
                    //Child Table

                    DataRelation FACRel = new DataRelation("FACRelation", DS.Tables[0].Columns["MBL_EXP_TBL_PK"], DS.Tables[1].Columns["MBL_EXP_TBL_PK"], true);
                    DS.Relations.Add(FACRel);
                    return DS;
                    //'AIR
                }
                else
                {
                    sb.Append("SELECT *");
                    sb.Append("  FROM (SELECT MAWB_EXP_TBL_PK,");
                    sb.Append("               MAWB_REF_NO,");
                    sb.Append("               MAWB_DATE,");
                    sb.Append("               AIRLINE_ID,");
                    sb.Append("               AIRLINE_MST_PK,");
                    sb.Append("               AIRLINE_NAME,");
                    sb.Append("               VOYAGE_FLIGHT_NO,");
                    sb.Append("               POL,");
                    sb.Append("               POD,");
                    sb.Append("               DEPARTURE_DATE,");
                    sb.Append("               ARRIVAL_DATE,");
                    sb.Append("               SUM(CHARG_WT) CHARG_WT,");
                    sb.Append("               SUM(AIF) AIF,");
                    sb.Append("               SUM(AFC_AMT) AFC_AMT,");
                    sb.Append("               '' FAC_BASIS,");
                    sb.Append("               GET_FAC_AMT(AIRLINE_MST_PK, MAWB_EXP_TBL_PK, " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ", 1) FAC_AMT,");
                    sb.Append("               '' INV,");
                    sb.Append("               '' COL,CSR");
                    sb.Append("          FROM (SELECT DISTINCT MAWB.MAWB_EXP_TBL_PK,");
                    sb.Append("                                MAWB.MAWB_REF_NO,");
                    sb.Append("                                TO_CHAR(MAWB.MAWB_DATE, DATEFORMAT) MAWB_DATE,");
                    sb.Append("                                OPER.AIRLINE_MST_PK,");
                    sb.Append("                                OPER.AIRLINE_ID,");
                    sb.Append("                                OPER.AIRLINE_NAME,");
                    sb.Append("                                JOB_EXP.VOYAGE_FLIGHT_NO,");
                    sb.Append("                                POL.PORT_NAME POL,");
                    sb.Append("                                POD.PORT_NAME POD,");
                    sb.Append("                                TO_CHAR(JOB_EXP.DEPARTURE_DATE, DATEFORMAT) DEPARTURE_DATE,");
                    sb.Append("                                TO_CHAR(JOB_EXP.ARRIVAL_DATE, DATEFORMAT)  ARRIVAL_DATE ,");
                    sb.Append("                                (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))");
                    sb.Append("                                   FROM JOB_TRN_CONT CONT");
                    sb.Append("                                  WHERE CONT.JOB_CARD_TRN_FK =");
                    sb.Append("                                        JOB_EXP.JOB_CARD_TRN_PK) CHARG_WT,");
                    sb.Append("                                SUM(GET_EX_RATE_BUY(CURR_TP.CURRENCY_MST_PK," + BaseCurrFk + ",JOB_EXP.JOBCARD_DATE)*");
                    sb.Append("                                                         (JCST.ESTIMATED_COST)) AIF,");
                    sb.Append("                                SUM(GET_EX_RATE_BUY(CURR_TP.CURRENCY_MST_PK," + BaseCurrFk + ",JOB_EXP.JOBCARD_DATE)*");
                    sb.Append("                                    (SELECT DISTINCT JC.ESTIMATED_COST");
                    sb.Append("                                       FROM JOB_TRN_COST JC");
                    sb.Append("                                      WHERE JC.JOB_TRN_COST_PK =");
                    sb.Append("                                            JCST.JOB_TRN_COST_PK");
                    sb.Append("                          AND JC.COST_ELEMENT_MST_FK = CST.COST_ELEMENT_MST_PK ");
                    sb.Append("                                        AND CST.COST_ELEMENT_ID = 'AFC')) AFC_AMT,MAWB.CSR");
                    sb.Append("                  FROM JOB_CARD_TRN  JOB_EXP,");
                    sb.Append("                       MAWB_EXP_TBL          MAWB,");
                    sb.Append("                       PORT_MST_TBL          POL,");
                    sb.Append("                       PORT_MST_TBL          POD,");
                    sb.Append("                       COST_ELEMENT_MST_TBL  CST,");
                    sb.Append("                       JOB_TRN_COST  JCST,");
                    sb.Append("                       CURRENCY_TYPE_MST_TBL CURR_TP,");
                    sb.Append("                       AIRLINE_MST_TBL       OPER,");
                    sb.Append("                       booking_mst_tbl       BKG_AIR,");
                    sb.Append("                       USER_MST_TBL          UMST");
                    sb.Append("                 WHERE MAWB.MAWB_EXP_TBL_PK = JOB_EXP.MBL_MAWB_FK");
                    sb.Append("                   AND JOB_EXP.booking_mst_FK = BKG_AIR.booking_mst_PK");
                    sb.Append("                   AND BKG_AIR.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("                   AND BKG_AIR.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("                   AND JCST.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
                    sb.Append("                   AND CST.COST_ELEMENT_MST_PK = JCST.COST_ELEMENT_MST_FK");
                    sb.Append("                   AND JCST.CURRENCY_MST_FK = CURR_TP.CURRENCY_MST_PK");
                    sb.Append("                   AND MAWB.AIRLINE_MST_FK = OPER.AIRLINE_MST_PK AND BKG_AIR.BUSINESS_TYPE=1");
                    if (OpratorPK > 0)
                    {
                        sb.Append("   AND OPER.AIRLINE_MST_PK = " + OpratorPK);
                    }
                    if (MBLPK > 0)
                    {
                        sb.Append("   AND MAWB.MAWB_EXP_TBL_PK = " + MBLPK);
                    }
                    if (POLPK > 0)
                    {
                        sb.Append("   AND POL.PORT_MST_PK = " + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        sb.Append("   AND POD.PORT_MST_PK = " + PODPK);
                    }
                    //sb.Append("                   AND (BKG_AIR.BOOKING_DATE BETWEEN")
                    //sb.Append("                       TO_DATE('11/10/2001', 'dd/MM/yyyy') AND")
                    //sb.Append("                       TO_DATE('21/11/2011', 'dd/MM/yyyy'))")
                    if (!string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
                    {
                        sb.Append("   AND (BKG_AIR.BOOKING_DATE BETWEEN TO_DATE('" + FromDate + "', '" + dateFormat + "') AND");
                        sb.Append("       TO_DATE('" + ToDate + "', '" + dateFormat + "'))");
                    }
                    sb.Append("                   AND UMST.USER_MST_PK = JOB_EXP.CREATED_BY_FK");
                    sb.Append("                   AND UMST.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                    sb.Append("                 GROUP BY MAWB.MAWB_EXP_TBL_PK,");
                    sb.Append("                          MAWB.MAWB_REF_NO,");
                    sb.Append("                          MAWB.MAWB_DATE,");
                    sb.Append("                          OPER.AIRLINE_MST_PK,");
                    sb.Append("                          OPER.AIRLINE_ID,");
                    sb.Append("                          OPER.AIRLINE_NAME,");
                    sb.Append("                          JOB_EXP.VOYAGE_FLIGHT_NO,");
                    sb.Append("                          POL.PORT_NAME,");
                    sb.Append("                          POD.PORT_NAME,");
                    sb.Append("                          JOB_EXP.DEPARTURE_DATE,");
                    sb.Append("                          JOB_EXP.ARRIVAL_DATE,");
                    sb.Append("                          JOB_EXP.JOB_CARD_TRN_PK,MAWB.CSR) Q");
                    sb.Append("         GROUP BY MAWB_EXP_TBL_PK,");
                    sb.Append("                  MAWB_REF_NO,");
                    sb.Append("                  MAWB_DATE,");
                    sb.Append("                  AIRLINE_ID,");
                    sb.Append("                  AIRLINE_MST_PK,");
                    sb.Append("                  AIRLINE_NAME,");
                    sb.Append("                  VOYAGE_FLIGHT_NO,");
                    sb.Append("                  POL,");
                    sb.Append("                  POD,");
                    sb.Append("                  DEPARTURE_DATE,");
                    sb.Append("                  ARRIVAL_DATE,CSR)");
                    sb.Append(" WHERE FAC_AMT IS NOT NULL ");
                    sb.Append(" ORDER BY FAC_AMT DESC ");

                    DS = objWF.GetDataSet(sb.ToString());
                    //'Parent Table
                    sb.Remove(0, sb.Length);

                    sb.Append("SELECT MAWB.MAWB_EXP_TBL_PK,");
                    sb.Append("       JOB_EXP.JOB_CARD_TRN_PK,");
                    sb.Append("       HAWB.HAWB_EXP_TBL_PK,");
                    sb.Append("       HAWB.HAWB_REF_NO,");
                    sb.Append("       TO_CHAR(HAWB.HAWB_DATE, DATEFORMAT) HAWB_DATE,");
                    sb.Append("       ' ' COMMODITY,");
                    sb.Append("       JCT.NET_WEIGHT,");
                    sb.Append("       JCT.CHARGEABLE_WEIGHT,");
                    sb.Append("       JCT.VOLUME_IN_CBM,");
                    sb.Append("       SUM(GET_EX_RATE_BUY(CURR_TP.CURRENCY_MST_PK, " + BaseCurrFk + ", JOB_EXP.JOBCARD_DATE) *");
                    sb.Append("           (SELECT DISTINCT JC.ESTIMATED_COST");
                    sb.Append("              FROM JOB_TRN_COST JC");
                    sb.Append("             WHERE JC.JOB_TRN_COST_PK = JCST.JOB_TRN_COST_PK");
                    sb.Append("             AND JC.COST_ELEMENT_MST_FK = CST.COST_ELEMENT_MST_PK ");
                    sb.Append("               AND CST.COST_ELEMENT_ID = 'AFC')) AFC_AMT,");
                    sb.Append("       ' ' FAC_BASIS,");
                    sb.Append("       SUM(GET_EX_RATE_BUY(CURR_TP.CURRENCY_MST_PK, " + BaseCurrFk + ", JOB_EXP.JOBCARD_DATE) *");
                    sb.Append("           ((FAC.COMMISSION / 100) * JCST.ESTIMATED_COST)) FAC_AMT,");
                    sb.Append("       0 INV,");
                    sb.Append("       0 COL,");
                    sb.Append("       JOB_EXP.COMMODITY_GROUP_FK,");
                    sb.Append("       JCT.COMMODITY_MST_FKS");
                    sb.Append("  FROM JOB_CARD_TRN  JOB_EXP,");
                    sb.Append("       MAWB_EXP_TBL          MAWB,");
                    sb.Append("       HAWB_EXP_TBL          HAWB,");
                    sb.Append("       PORT_MST_TBL          POL,");
                    sb.Append("       PORT_MST_TBL          POD,");
                    sb.Append("       COST_ELEMENT_MST_TBL  CST,");
                    sb.Append("       JOB_TRN_COST  JCST,");
                    sb.Append("       COMMODITY_MST_TBL     COMM,");
                    sb.Append("       JOB_TRN_CONT  JCT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL CURR_TP,");
                    sb.Append("       AIRLINE_MST_TBL       OPER,");
                    sb.Append("       FAC_SETUP_TBL         FAC,");
                    sb.Append("       booking_mst_tbl       BKG_AIR,");
                    sb.Append("       USER_MST_TBL          UMST");
                    sb.Append(" WHERE JCT.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
                    sb.Append("   AND HAWB.JOB_CARD_AIR_EXP_FK = JOB_EXP.JOB_CARD_TRN_PK");
                    sb.Append("   AND JCT.COMMODITY_MST_FK = COMM.COMMODITY_MST_PK(+)");
                    sb.Append("   AND MAWB.MAWB_EXP_TBL_PK = JOB_EXP.MBL_MAWB_FK");
                    sb.Append("   AND JOB_EXP.booking_mst_FK = BKG_AIR.booking_mst_PK");
                    sb.Append("   AND BKG_AIR.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND BKG_AIR.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND JCST.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
                    sb.Append("   AND CST.COST_ELEMENT_MST_PK = JCST.COST_ELEMENT_MST_FK");
                    sb.Append("   AND JCST.COST_ELEMENT_MST_FK = FAC.COST_ELEMENT_FK");
                    sb.Append("   AND JCST.CURRENCY_MST_FK = CURR_TP.CURRENCY_MST_PK");
                    sb.Append("   AND BKG_AIR.CARRIER_MST_FK = OPER.AIRLINE_MST_PK");
                    sb.Append("   AND OPER.AIRLINE_MST_PK = FAC.AIRLINE_MST_FK AND BKG_AIR.BUSINESS_TYPE=1");
                    sb.Append("   AND HAWB.HAWB_REF_NO IS NOT NULL ");
                    if (OpratorPK > 0)
                    {
                        sb.Append("   AND OPER.AIRLINE_MST_PK = " + OpratorPK);
                    }
                    if (MBLPK > 0)
                    {
                        sb.Append("   AND MAWB.MAWB_EXP_TBL_PK = " + MBLPK);
                    }
                    if (POLPK > 0)
                    {
                        sb.Append("   AND POL.PORT_MST_PK = " + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        sb.Append("   AND POD.PORT_MST_PK = " + PODPK);
                    }
                    //sb.Append("   AND (BKG_AIR.BOOKING_DATE BETWEEN TO_DATE('11/10/2001', 'dd/MM/yyyy') AND")
                    //sb.Append("       TO_DATE('21/11/2011', 'dd/MM/yyyy'))")
                    if (!string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
                    {
                        sb.Append("   AND (BKG_AIR.BOOKING_DATE BETWEEN TO_DATE('" + FromDate + "', '" + dateFormat + "') AND");
                        sb.Append("       TO_DATE('" + ToDate + "', '" + dateFormat + "'))");
                    }
                    sb.Append("   AND UMST.USER_MST_PK = JOB_EXP.CREATED_BY_FK");
                    sb.Append("   AND UMST.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                    sb.Append(" GROUP BY MAWB.MAWB_EXP_TBL_PK,");
                    sb.Append("          JOB_EXP.JOB_CARD_TRN_PK,");
                    sb.Append("          HAWB.HAWB_EXP_TBL_PK,");
                    sb.Append("          HAWB.HAWB_REF_NO,");
                    sb.Append("          HAWB.HAWB_DATE,");
                    sb.Append("          JCT.NET_WEIGHT,");
                    sb.Append("          JCT.CHARGEABLE_WEIGHT,");
                    sb.Append("          JCT.VOLUME_IN_CBM,");
                    sb.Append("          JOB_EXP.COMMODITY_GROUP_FK,");
                    sb.Append("          JCT.COMMODITY_MST_FKS");
                    sb.Append("");

                    DS.Tables.Add(objWF.GetDataTable(sb.ToString()));
                    //Child Table

                    DataRelation FACRel = new DataRelation("FACRelation", DS.Tables[0].Columns["MAWB_EXP_TBL_PK"], DS.Tables[1].Columns["MAWB_EXP_TBL_PK"], true);
                    DS.Relations.Add(FACRel);
                    return DS;
                }
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

        #endregion " Fetch All "

        #region "FAC POPUP"         ''This query is using for Both(Air and Sea)  for Grid and Report

        /// <summary>
        /// Facs the pop up.
        /// </summary>
        /// <param name="OperatorPK">The operator pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FACPopUp(Int32 OperatorPK, Int32 BizType)
        {
            WorkFlow objWf = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT CMT.COST_ELEMENT_MST_PK,");
            sb.Append("       CMT.COST_ELEMENT_ID,");
            sb.Append("       CMT.COST_ELEMENT_NAME,");
            //sb.Append("       FAC.CHARGE_BASIS,")
            sb.Append("       DECODE(FAC.CHARGE_BASIS, '0', ' ', '1', '%', '2', 'CBM', '3', 'Kgs','4', 'Lumpsum') CHARGE_BASIS,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       FAC.COMMISSION FAC");
            sb.Append("  FROM FAC_SETUP_TBL FAC, COST_ELEMENT_MST_TBL CMT,CURRENCY_TYPE_MST_TBL CTMT");
            sb.Append(" WHERE CMT.COST_ELEMENT_MST_PK = FAC.COST_ELEMENT_FK  AND CTMT.CURRENCY_MST_PK=FAC.CURRENCY_MST_FK ");
            if (BizType == 2)
            {
                sb.Append("   AND FAC.OPERATOR_MST_FK = " + OperatorPK);
            }
            else
            {
                sb.Append("   AND FAC.AIRLINE_MST_FK = " + OperatorPK);
            }
            try
            {
                return objWf.GetDataSet(sb.ToString());
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

        #endregion "FAC POPUP"         ''This query is using for Both(Air and Sea)  for Grid and Report

        #region "Print Air Query"          ''Gangadhar for Print Fac Air PrintFunction

        /// <summary>
        /// Fetches the fac print air.
        /// </summary>
        /// <param name="OpratorPK">The oprator pk.</param>
        /// <param name="VesselPK">The vessel pk.</param>
        /// <param name="MAWBPK">The mawbpk.</param>
        /// <param name="MBLDate">The MBL date.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="CARGO_TYPE">Type of the carg o_.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <returns></returns>
        public DataSet FetchFACPrintAir(int OpratorPK = 0, string VesselPK = "", int MAWBPK = 0, string MBLDate = "", string FromDate = "", string ToDate = "", int POLPK = 0, int PODPK = 0, int CARGO_TYPE = 0, int BIZ_TYPE = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT MAWB.MAWB_EXP_TBL_PK,");
            sb.Append("       MAWB.MAWB_REF_NO,");
            sb.Append("       TO_CHAR(MAWB.MAWB_DATE, DATEFORMAT) MAWB_DATE,");
            sb.Append("       OPER.AIRLINE_MST_PK,");
            sb.Append("       OPER.AIRLINE_ID,");
            sb.Append("       OPER.AIRLINE_NAME,");
            sb.Append("       JOB_EXP.VOYAGE_FLIGHT_NO,");
            sb.Append("       POL.PORT_NAME POL,");
            sb.Append("       POD.PORT_NAME POD,");
            sb.Append("       TO_CHAR(JOB_EXP.DEPARTURE_DATE, DATEFORMAT) DEPARTURE_DATE,");
            sb.Append("       TO_CHAR(JOB_EXP.ARRIVAL_DATE, DATEFORMAT) ARRIVAL_DATE,");
            sb.Append("       (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0))");
            sb.Append("          FROM JOB_TRN_CONT CONT");
            sb.Append("         WHERE CONT.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK) CHARG_WT,");
            sb.Append("       SUM(GET_EX_RATE(CURR_TP.CURRENCY_MST_PK, " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ", SYSDATE) *");
            sb.Append("           (JCST.ESTIMATED_COST)) AIF,");
            sb.Append("       '' FAC_BASIS,");
            sb.Append("       GET_FAC_AMT(AIRLINE_MST_PK, MAWB_EXP_TBL_PK,  " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ", 1) FAC_AMT_P,");
            sb.Append("       '' INV,");
            sb.Append("       '' COL,");
            sb.Append("       JOB_EXP.JOB_CARD_TRN_PK,");
            sb.Append("       HAWB.HAWB_EXP_TBL_PK,");
            sb.Append("       HAWB.HAWB_REF_NO,");
            sb.Append("       TO_CHAR(HAWB.HAWB_DATE, DATEFORMAT) HAWB_DATE,");
            sb.Append("       '' COMMODITY,");
            sb.Append("       JCT.NET_WEIGHT,");
            sb.Append("       JCT.CHARGEABLE_WEIGHT,");
            sb.Append("       JCT.VOLUME_IN_CBM,");
            sb.Append("       SUM(GET_EX_RATE(CURR_TP.CURRENCY_MST_PK, " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ", SYSDATE) *");
            sb.Append("           (SELECT DISTINCT JC.ESTIMATED_COST");
            sb.Append("              FROM JOB_TRN_COST JC");
            sb.Append("             WHERE JC.JOB_TRN_COST_PK = JCST.JOB_TRN_COST_PK");
            sb.Append("             AND JC.COST_ELEMENT_MST_FK = CST.COST_ELEMENT_MST_PK ");
            sb.Append("               AND CST.COST_ELEMENT_ID = 'AFC')) AFC_AMT,");
            sb.Append("       SUM(GET_EX_RATE(CURR_TP.CURRENCY_MST_PK,  " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ", SYSDATE) *");
            sb.Append("           ((FAC.COMMISSION / 100) * JCST.ESTIMATED_COST)) FAC_AMT_C,");
            sb.Append("       JOB_EXP.COMMODITY_GROUP_FK,");
            sb.Append("       JCT.COMMODITY_MST_FKS");
            sb.Append("  FROM JOB_CARD_TRN  JOB_EXP,");
            sb.Append("       MAWB_EXP_TBL          MAWB,");
            sb.Append("       PORT_MST_TBL          POL,");
            sb.Append("       PORT_MST_TBL          POD,");
            sb.Append("       COST_ELEMENT_MST_TBL  CST,");
            sb.Append("       JOB_TRN_COST  JCST,");
            sb.Append("       CURRENCY_TYPE_MST_TBL CURR_TP,");
            sb.Append("       AIRLINE_MST_TBL       OPER,");
            sb.Append("       booking_mst_tbl       BKG_AIR,");
            sb.Append("       USER_MST_TBL          UMST,");
            sb.Append("       JOB_TRN_CONT  JCT,");
            sb.Append("       FAC_SETUP_TBL         FAC,");
            sb.Append("       HAWB_EXP_TBL          HAWB");
            sb.Append(" WHERE MAWB.MAWB_EXP_TBL_PK = JOB_EXP.MBL_MAWB_FK");
            sb.Append("   AND JOB_EXP.booking_mst_FK = BKG_AIR.booking_mst_PK");
            sb.Append("   AND JCT.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
            sb.Append("   AND JCST.COST_ELEMENT_MST_FK = FAC.COST_ELEMENT_FK");
            sb.Append("   AND OPER.AIRLINE_MST_PK = FAC.AIRLINE_MST_FK");
            sb.Append("   AND HAWB.JOB_CARD_AIR_EXP_FK = JOB_EXP.JOB_CARD_TRN_PK");
            sb.Append("   AND BKG_AIR.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND BKG_AIR.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND JCST.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
            sb.Append("   AND CST.COST_ELEMENT_MST_PK = JCST.COST_ELEMENT_MST_FK");
            sb.Append("   AND JCST.CURRENCY_MST_FK = CURR_TP.CURRENCY_MST_PK");
            sb.Append("   AND MAWB.AIRLINE_MST_FK = OPER.AIRLINE_MST_PK");
            sb.Append("   AND BKG_AIR.CARRIER_MST_FK = OPER.AIRLINE_MST_PK");
            //sb.Append("   AND (BKG_AIR.BOOKING_DATE BETWEEN TO_DATE('" & FromDate & "', '" & dateFormat & "') AND")
            //sb.Append("       TO_DATE('" & ToDate & "', '" & dateFormat & "'))")

            if (!((FromDate == null | string.IsNullOrEmpty(FromDate))) & !((ToDate == null | string.IsNullOrEmpty(ToDate))))
            {
                sb.Append(" AND BKG_AIR.BOOKING_DATE BETWEEN TO_DATE('" + FromDate + "', DATEFORMAT) AND TO_DATE('" + ToDate + "', DATEFORMAT)");
            }
            else if ((!string.IsNullOrEmpty(FromDate)) & ((ToDate == null) | string.IsNullOrEmpty(ToDate)))
            {
                sb.Append(" AND BKG_AIR.BOOKING_DATE >= TO_DATE('" + FromDate + "',dateformat) ");
            }
            else if ((!string.IsNullOrEmpty(ToDate)) & ((FromDate == null) | string.IsNullOrEmpty(FromDate)))
            {
                sb.Append("AND BKG_AIR.BOOKING_DATE <= TO_DATE('" + ToDate + "',dateformat) ");
            }

            sb.Append("   AND UMST.USER_MST_PK = JOB_EXP.CREATED_BY_FK");
            sb.Append("   AND UMST.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            sb.Append(" GROUP BY MAWB.MAWB_EXP_TBL_PK,");
            sb.Append("          MAWB.MAWB_REF_NO,");
            sb.Append("          MAWB.MAWB_DATE,");
            sb.Append("          OPER.AIRLINE_MST_PK,");
            sb.Append("          OPER.AIRLINE_ID,");
            sb.Append("          OPER.AIRLINE_NAME,");
            sb.Append("          JOB_EXP.VOYAGE_FLIGHT_NO,");
            sb.Append("          POL.PORT_NAME,");
            sb.Append("          POD.PORT_NAME,");
            sb.Append("          JOB_EXP.DEPARTURE_DATE,");
            sb.Append("          JOB_EXP.ARRIVAL_DATE,");
            sb.Append("          JOB_EXP.JOB_CARD_TRN_PK,");
            sb.Append("          JOB_EXP.JOB_CARD_TRN_PK,");
            sb.Append("          HAWB.HAWB_EXP_TBL_PK,");
            sb.Append("          HAWB.HAWB_REF_NO,");
            sb.Append("          HAWB.HAWB_DATE,");
            sb.Append("          JCT.NET_WEIGHT,");
            sb.Append("          JCT.CHARGEABLE_WEIGHT,");
            sb.Append("          JCT.VOLUME_IN_CBM,");
            sb.Append("          JOB_EXP.COMMODITY_GROUP_FK,");
            sb.Append("          JCT.COMMODITY_MST_FKS");
            if (OpratorPK > 0)
            {
                sb.Append("   AND OPER.AIRLINE_MST_PK = " + OpratorPK);
            }
            if (MAWBPK > 0)
            {
                sb.Append("   AND MAWB.MAWB_EXP_TBL_PK = " + MAWBPK);
            }
            if (POLPK > 0)
            {
                sb.Append("   AND POL.PORT_MST_PK = " + POLPK);
            }
            if (PODPK > 0)
            {
                sb.Append("   AND POD.PORT_MST_PK = " + PODPK);
            }

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception Exp)
            {
                throw Exp;
            }
        }

        #endregion "Print Air Query"          ''Gangadhar for Print Fac Air PrintFunction

        #region "Print Sea Query"  ''Vasava for Print Function

        /// <summary>
        /// Fetches the fac print sea.
        /// </summary>
        /// <param name="OpratorPK">The oprator pk.</param>
        /// <param name="VesselPK">The vessel pk.</param>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <param name="MBLDate">The MBL date.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="CARGO_TYPE">Type of the carg o_.</param>
        /// <param name="BIZ_TYPE">Type of the bi z_.</param>
        /// <returns></returns>
        public DataSet FetchFACPrintSea(int OpratorPK = 0, string VesselPK = "", int MBLPK = 0, string MBLDate = "", string FromDate = "", string ToDate = "", int POLPK = 0, int PODPK = 0, int CARGO_TYPE = 0, int BIZ_TYPE = 0)
        {
            WorkFlow objWF = new WorkFlow();

            int BaseCurrFk = Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"]);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append(" SELECT DISTINCT MBL.MBL_EXP_TBL_PK,");
            sb.Append(" MBL.MBL_REF_NO,");
            sb.Append(" TO_CHAR(MBL.MBL_DATE, DATEFORMAT) MBL_DATE,");
            sb.Append(" OPER.OPERATOR_ID,");
            sb.Append(" OPER.OPERATOR_NAME,");
            sb.Append(" MBL.VOYAGE_TRN_FK,");
            sb.Append(" VVT.VESSEL_NAME,");
            sb.Append(" VT.VOYAGE,");
            sb.Append(" POL.PORT_NAME POL,");
            sb.Append(" POD.PORT_NAME POD,");
            sb.Append(" VT.POL_ETD,");
            sb.Append(" VT.POD_ETA,");
            sb.Append("                (SELECT SUM(NVL(CTMT.TEU_FACTOR, 0))");
            sb.Append("                   FROM JOB_TRN_CONT   CONT,");
            sb.Append("                        CONTAINER_TYPE_MST_TBL CTMT");
            sb.Append("                  WHERE (CONT.JOB_CARD_TRN_FK =");
            sb.Append("                        JOB_EXP.JOB_CARD_TRN_PK)");
            sb.Append("                    AND CONT.CONTAINER_TYPE_MST_FK =");
            sb.Append("                        CTMT.CONTAINER_TYPE_MST_PK(+)) TEU,");
            sb.Append("                DECODE(BKG_SEA.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
            sb.Append("                SUM(DISTINCT GET_EX_RATE_BUY(CURR_TP.CURRENCY_MST_PK, " + BaseCurrFk + ", JOB_EXP.JOBCARD_DATE) *");
            sb.Append("                    (JCST.ESTIMATED_COST)) AIF,");
            sb.Append("                SUM(DISTINCT GET_EX_RATE_BUY(CURR_TP.CURRENCY_MST_PK, " + BaseCurrFk + ", JOB_EXP.JOBCARD_DATE) *");
            sb.Append("                    (SELECT JC.ESTIMATED_COST");
            sb.Append("                       FROM JOB_TRN_COST JC");
            sb.Append("                      WHERE JC.JOB_TRN_COST_PK =");
            sb.Append("                            JCST.JOB_TRN_COST_PK");
            sb.Append("                            AND JC.COST_ELEMENT_MST_FK = CST.COST_ELEMENT_MST_PK ");
            sb.Append("                        AND CST.COST_ELEMENT_ID = 'BOF')) BOF_AMT,");
            sb.Append("                '' FAC_BASIS,");
            //sb.Append("                SUM(GET_EX_RATE(CURR_TP.CURRENCY_MST_PK, " & HttpContext.Current.Session("CURRENCY_MST_PK") & ", SYSDATE) *")
            //sb.Append("                    ((FAC.COMMISSION / 100) * JCST.ESTIMATED_COST)) FAC_AMT,")
            sb.Append("                '' INV,");
            sb.Append("                '' COL,OPER.OPERATOR_MST_PK,");
            //'Parent cols End
            sb.Append("  JOB_EXP.JOB_CARD_TRN_PK,");
            sb.Append("  NVL(HBL.HBL_EXP_TBL_PK,0)HBL_EXP_TBL_PK,");
            sb.Append(" HBL.HBL_REF_NO,");
            sb.Append(" TO_CHAR(HBL.HBL_DATE, DATEFORMAT) HBL_DATE,");
            sb.Append(" '' CONTAINER_TYPE_MST_ID, ");
            sb.Append(" '' CONTAINER_NUMBER, ");
            sb.Append("                '' COMMODITY,");
            sb.Append("                 SUM(JCT.NET_WEIGHT), ");
            sb.Append("                SUM(DISTINCT GET_EX_RATE(CURR_TP.CURRENCY_MST_PK, " + BaseCurrFk + ", JOB_EXP.JOBCARD_DATE) *");
            sb.Append("                    ((FAC.COMMISSION / 100) * ");
            sb.Append("                    (SELECT JC.ESTIMATED_COST");
            sb.Append("                       FROM JOB_TRN_COST JC");
            sb.Append("                      WHERE JC.JOB_TRN_COST_PK =");
            sb.Append("                            JCST.JOB_TRN_COST_PK");
            sb.Append("                            AND JC.COST_ELEMENT_MST_FK = CST.COST_ELEMENT_MST_PK ");
            sb.Append("                        AND CST.COST_ELEMENT_ID = 'BOF')))FAC_AMT,");
            sb.Append("   JOB_EXP.COMMODITY_GROUP_FK");
            if (CARGO_TYPE == 4)
            {
                sb.Append("  ,JCT.COMMODITY_MST_FKS");
            }

            sb.Append(" FROM JOB_CARD_TRN   JOB_EXP,");
            sb.Append(" VESSEL_VOYAGE_TBL      VVT,");
            sb.Append(" VESSEL_VOYAGE_TRN      VT,");
            sb.Append(" MBL_EXP_TBL            MBL,");
            sb.Append(" PORT_MST_TBL           POL,");
            sb.Append(" PORT_MST_TBL           POD,");
            sb.Append(" COST_ELEMENT_MST_TBL   CST,");
            sb.Append(" JOB_TRN_COST   JCST,");
            sb.Append(" CURRENCY_TYPE_MST_TBL  CURR_TP,");
            sb.Append("  OPERATOR_MST_TBL       OPER,");
            sb.Append(" booking_mst_tbl        BKG_SEA,");
            sb.Append(" USER_MST_TBL           UMST,");
            sb.Append(" JOB_TRN_CONT   JCT,");
            sb.Append(" COMMODITY_MST_TBL      COMM,");
            sb.Append(" CONTAINER_TYPE_MST_TBL CMT,");
            sb.Append(" FAC_SETUP_TBL          FAC,");
            sb.Append(" HBL_EXP_TBL            HBL");

            sb.Append(" WHERE JOB_EXP.VOYAGE_TRN_FK = VT.VOYAGE_TRN_PK");
            sb.Append(" AND VT.VESSEL_VOYAGE_TBL_FK = VVT.VESSEL_VOYAGE_TBL_PK");
            sb.Append(" AND MBL.MBL_EXP_TBL_PK = JOB_EXP.MBL_MAWB_FK");
            sb.Append(" AND JOB_EXP.Hbl_Hawb_Fk = HBL.HBL_EXP_TBL_PK(+)");
            sb.Append(" AND JOB_EXP.booking_mst_FK = BKG_SEA.booking_mst_PK");
            sb.Append(" AND BKG_SEA.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append(" AND BKG_SEA.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append(" AND JCST.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
            sb.Append(" AND CST.COST_ELEMENT_MST_PK = JCST.COST_ELEMENT_MST_FK");
            sb.Append(" AND JCST.CURRENCY_MST_FK = CURR_TP.CURRENCY_MST_PK");
            sb.Append(" AND BKG_SEA.CARRIER_MST_FK = OPER.OPERATOR_MST_PK");
            sb.Append(" AND OPER.OPERATOR_MST_PK = FAC.OPERATOR_MST_FK");
            sb.Append(" AND JCT.CONTAINER_TYPE_MST_FK = CMT.CONTAINER_TYPE_MST_PK(+)");
            sb.Append(" AND JCT.JOB_CARD_TRN_FK = JOB_EXP.JOB_CARD_TRN_PK");
            sb.Append(" AND JCT.COMMODITY_MST_FK = COMM.COMMODITY_MST_PK");
            //sb.Append(" AND JCST.COST_ELEMENT_MST_FK = FAC.COST_ELEMENT_FK")
            sb.Append("   AND BKG_SEA.CARGO_TYPE = " + CARGO_TYPE);

            if (OpratorPK > 0)
            {
                sb.Append("   AND OPER.OPERATOR_MST_PK = " + OpratorPK);
            }
            if (MBLPK > 0)
            {
                sb.Append("   AND MBL.MBL_EXP_TBL_PK = " + MBLPK);
            }
            if (POLPK > 0)
            {
                sb.Append("   AND POL.PORT_MST_PK = " + POLPK);
            }
            if (PODPK > 0)
            {
                sb.Append("   AND POD.PORT_MST_PK = " + PODPK);
            }
            if (!string.IsNullOrEmpty(FromDate) & !string.IsNullOrEmpty(ToDate))
            {
                sb.Append("   AND (BKG_SEA.BOOKING_DATE BETWEEN TO_DATE('" + FromDate + "', '" + dateFormat + "') AND");
                sb.Append("       TO_DATE('" + ToDate + "', '" + dateFormat + "'))");
            }
            sb.Append("   AND UMST.USER_MST_PK = JOB_EXP.CREATED_BY_FK");
            sb.Append("   AND UMST.DEFAULT_LOCATION_FK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            sb.Append(" GROUP BY MBL.MBL_EXP_TBL_PK,");
            sb.Append(" MBL.MBL_REF_NO,");
            sb.Append(" MBL.MBL_DATE,");
            sb.Append(" OPER.OPERATOR_ID,");
            sb.Append(" OPER.OPERATOR_NAME,");
            sb.Append(" MBL.VOYAGE_TRN_FK,");
            sb.Append(" VVT.VESSEL_NAME,");
            sb.Append(" VT.VOYAGE,");
            sb.Append(" POL.PORT_NAME,");
            sb.Append("  POD.PORT_NAME,");
            sb.Append(" VT.POL_ETD,");
            sb.Append(" VT.POD_ETA,");
            sb.Append(" JOB_EXP.JOB_CARD_TRN_PK,");
            sb.Append(" BKG_SEA.CARGO_TYPE,");
            sb.Append(" JOB_EXP.COMMODITY_GROUP_FK,");
            if (CARGO_TYPE == 4)
            {
                sb.Append(" JCT.COMMODITY_MST_FKS,");
            }
            sb.Append(" OPER.OPERATOR_MST_PK,");
            sb.Append(" JOB_EXP.JOB_CARD_TRN_PK,");
            sb.Append(" HBL.HBL_EXP_TBL_PK,");
            sb.Append(" HBL.HBL_REF_NO,");
            sb.Append(" HBL.HBL_DATE ");

            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException SqlExp)
            {
                throw SqlExp;
            }
            catch (Exception Exp)
            {
                throw Exp;
            }
        }

        #endregion "Print Sea Query"  ''Vasava for Print Function

        /// <summary>
        /// Fetches the agent invoice.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchAgentInvoice(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strReq = null;
            string businessType = null;
            string Import = "";
            string Agnt = "";
            string Agent = "";
            string pod = "";
            string Active = "1";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                businessType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                Agnt = Convert.ToString(arr.GetValue(4));
            //Added by Manoharan 06Feb2007: for XBkg & CoLoad Agent enhance search
            if (arr.Length > 5)
                Import = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                pod = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                Agent = Convert.ToString(arr.GetValue(7));
            if (arr.Length > 8)
                Active = Convert.ToString(arr.GetValue(8));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_AGENT_PKG.GETAGENT_COMMON_INVOICE";
                var _with21 = SCM.Parameters;
                _with21.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with21.Add("LOCATION_MST_FK_IN", (!string.IsNullOrEmpty(strLOC_MST_IN) ? strLOC_MST_IN : "")).Direction = ParameterDirection.Input;
                _with21.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with21.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with21.Add("AGENT_TYPE_IN", ifDBNull(Agnt)).Direction = ParameterDirection.Input;
                //Added by Manoharan 06Feb2007: for XBkg & CoLoad Agent enhance search
                _with21.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input;
                _with21.Add("POD_FK_IN", ifDBNull(pod)).Direction = ParameterDirection.Input;
                _with21.Add("AGENT_IN", ifDBNull(Agent)).Direction = ParameterDirection.Input;
                _with21.Add("ACTIVE_FLAG_IN", ifDBNull(Active)).Direction = ParameterDirection.Input;
                _with21.Add("RETURN_VALUE", OracleDbType.NVarchar2, 32767, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
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

        #region "Agent Address"

        /// <summary>
        /// Fetches the agent address.
        /// </summary>
        /// <param name="Loc">The loc.</param>
        /// <returns></returns>
        public DataSet FetchAgentAddress(long Loc)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
            StrSqlBuilder.Append("  COP.GST_NO,COP.COMPANY_REG_NO,COP.HOME_PAGE URL, ");
            StrSqlBuilder.Append("  L.LOCATION_ID , L.LOCATION_NAME, ");
            StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,L.E_MAIL_ID,");
            StrSqlBuilder.Append("  L.CITY,CMST.COUNTRY_NAME COUNTRY,L.ZIP, L.LOCATION_MST_PK,L.VAT_NO");
            StrSqlBuilder.Append("  FROM CORPORATE_MST_TBL COP,LOCATION_MST_TBL L,COUNTRY_MST_TBL CMST");
            StrSqlBuilder.Append("  WHERE CMST.COUNTRY_MST_PK(+)=L.COUNTRY_MST_FK AND L.LOCATION_MST_PK = " + Loc + "");
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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

        #endregion "Agent Address"

        #region "Fetch CSR Details "

        /// <summary>
        /// Fas the c_ fetc h_ main.
        /// </summary>
        /// <param name="MAWB_PK">The maw b_ pk.</param>
        /// <returns></returns>
        public DataSet FAC_FETCH_MAIN(string MAWB_PK)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with22 = objWK.MyCommand.Parameters;
                _with22.Add("MAWB_PK_IN", MAWB_PK).Direction = ParameterDirection.Input;
                _with22.Add("CURR_PK_IN", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with22.Add("CUR_MAWB", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                return objWK.GetDataSet("FAC_SETUP_TBL_PKG", "FAC_FETCH_MAIN");
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

        #endregion "Fetch CSR Details "

        /// <summary>
        /// Iaias the code.
        /// </summary>
        /// <returns></returns>
        public object IAIACode()
        {
            WorkFlow objWK = new WorkFlow();
            string IATA = objWK.ExecuteScaler("SELECT L.IATA_CODE FROM CORPORATE_MST_TBL COP, LOCATION_MST_TBL L, COUNTRY_MST_TBL CMST WHERE CMST.COUNTRY_MST_PK(+) = L.COUNTRY_MST_FK  AND L.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK"] + "");
            return IATA;
        }

        /// <summary>
        /// Updates the CSR.
        /// </summary>
        /// <param name="MAWB_PK">The maw b_ pk.</param>
        /// <returns></returns>
        public bool UpdateCSR(string MAWB_PK)
        {
            string SqlCSR = null;
            WorkFlow objWK = new WorkFlow();
            SqlCSR = "  UPDATE MAWB_EXP_TBL M SET M.CSR = 1 WHERE";
            SqlCSR += "  M.MAWB_EXP_TBL_PK IN (" + MAWB_PK + ") ";
            try
            {
                objWK.OpenConnection();
                objWK.ExecuteCommands(SqlCSR);
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
                return false;
                //'Exception Handling Added by Gangadhar on 12/09/2011, PTS ID: SEP-01
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        #region "CSR Status"

        /// <summary>
        /// Fetches the CSR status.
        /// </summary>
        /// <param name="MAWB_PK">The maw b_ pk.</param>
        /// <returns></returns>
        public DataSet FetchCSRStatus(string MAWB_PK)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append(" SELECT M.MAWB_EXP_TBL_PK , M.CSR FROM MAWB_EXP_TBL M WHERE M.MAWB_EXP_TBL_PK IN (" + MAWB_PK + ") ");
            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
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

        #endregion "CSR Status"
    }
}