using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    class cls_FinGeneration : CommonFeatures
    {
        public DataSet TestFetch()
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.AppendLine("SELECT  '' SL_NR,'' REF_PK,'' REF_NR,'' REF_DT,'' CUSTOMER,'' CURR,'' AMOUNT,'' STATUS,'' SEL from dual");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }
        #region "Fetch Grid Details"
        public string FetchAll(int DocType, int CustPK, int CSTPK = 0, int RefPK = 0, string FromDate = "", string ToDate = "", int DocStatus = 0, string Doc_Ref_Nr = "", string Cust_name = "", string biztype = "",
        string processtype = "", string cargotype = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 Flag1 = 0, string SearchType = "")
        {

            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            if (biztype != "0")
            {
                if (biztype == "1")
                {
                    biztype = "AIR";
                }
                else
                {
                    biztype = "SEA";
                }
            }
            if (processtype != "0")
            {
                if (processtype == "1")
                {
                    processtype = "EXPORT";
                }
                else
                {
                    processtype = "IMPORT";
                }
            }
            if (cargotype != "0")
            {
                if (cargotype == "1")
                {
                    cargotype = "FCL";
                }
                else if (cargotype == "2")
                {
                    cargotype = "LCL";
                }
                else
                {
                    cargotype = "BBC";
                }
            }
            try
            {
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("DOC_TYPE_IN", DocType).Direction = ParameterDirection.Input;
                _with1.Add("CUST_PK_IN", CustPK).Direction = ParameterDirection.Input;
                _with1.Add("CST_PK_IN", CSTPK).Direction = ParameterDirection.Input;
                _with1.Add("DOC_REF_PK_IN", RefPK).Direction = ParameterDirection.Input;
                _with1.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDate) ? "" : FromDate)).Direction = ParameterDirection.Input;
                _with1.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDate) ? "" : ToDate)).Direction = ParameterDirection.Input;
                _with1.Add("DOC_STATUS_IN", DocStatus).Direction = ParameterDirection.Input;
                _with1.Add("DOC_REF_NR_IN", Doc_Ref_Nr).Direction = ParameterDirection.Input;
                _with1.Add("CUST_NAME_IN", Cust_name).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", biztype).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_TYPE_IN", processtype).Direction = ParameterDirection.Input;
                _with1.Add("CARGO_TYPE_IN", cargotype).Direction = ParameterDirection.Input;
                _with1.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FINANCE_INTEGRATION_PKG", "FETCH_LISTING");
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }
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
        #endregion

        #region "Fetch Grid Details"
        public DataSet Generate(int DocType, int DocPK = 0, string DocNr = "")
        {

            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            DataRelation Dr = null;
            try
            {
                var _with2 = objWF.MyCommand.Parameters;
                _with2.Add("DOC_TYPE_IN", DocType).Direction = ParameterDirection.Input;
                _with2.Add("DOC_PK_IN", DocPK).Direction = ParameterDirection.Input;
                _with2.Add("DOC_NR_IN", DocNr).Direction = ParameterDirection.Input;
                _with2.Add("HEADER_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Add("REC_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Add("VSL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Add("FRT_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Add("JOB_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Add("CNTR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FINANCE_INTEGRATION_PKG", "GENERATE_DOC");

                //Dr = New DataRelation("INVOICE", DS.Tables(0).Columns("DOC_PK"), DS.Tables(1).Columns("DOC_PK"))
                //Dr.Nested = True
                //DS.Relations.Add(Dr)
                DS.Tables[0].TableName = "HEADER";
                DS.Tables[1].TableName = "RECORD";
                DS.Tables[2].TableName = "VESSEL";
                DS.Tables[3].TableName = "FREIGHT";
                DS.Tables[4].TableName = "JOB";
                DS.Tables[5].TableName = "CONTAINER";

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
        public DataSet GenerateColOrPay(int DocType, int DocPK = 0)
        {

            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            DataRelation Dr = null;
            try
            {
                var _with3 = objWF.MyCommand.Parameters;
                _with3.Add("DOC_TYPE_IN", DocType).Direction = ParameterDirection.Input;
                _with3.Add("DOC_PK_IN", DocPK).Direction = ParameterDirection.Input;
                _with3.Add("HEADER_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with3.Add("REC_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with3.Add("STL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with3.Add("CHEQUE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FINANCE_INTEGRATION_PKG", "GENERATE_DOC_COL_PAY");

                DS.Tables[0].TableName = "HEADER";
                DS.Tables[1].TableName = "RECORD";
                DS.Tables[2].TableName = "SETTLE";
                DS.Tables[3].TableName = "CHEQUE";
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
        #endregion

        #region "Update Fin Status"
        public string updateFinStatus(string DocpKs, int DocType, string DocNrs)
        {
            string strSql = "";
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSql = " UPDATE QILS_EX_ARP_NOTES_HEADER SET FIN_DOC_STATUS = 1 WHERE NOTES_NO IN (" + DocNrs + ")";
                objWF.ExecuteCommands(strSql);
                return JsonConvert.SerializeObject(objWF.ExecuteCommands(strSql), Formatting.Indented);
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
        #endregion

        #region "Generate Excel"
        public DataSet GenerateExcel(int DocType, string DocPKs = "")
        {

            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();
            DataRelation Dr = null;
            try
            {
                var _with4 = objWF.MyCommand.Parameters;
                _with4.Add("DOC_TYPE_IN", DocType).Direction = ParameterDirection.Input;
                _with4.Add("DOC_PKS_IN", DocPKs).Direction = ParameterDirection.Input;
                _with4.Add("DATA_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FINANCE_INTEGRATION_PKG", "GENERATE_EXCEL");
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
        #endregion
    }
}
