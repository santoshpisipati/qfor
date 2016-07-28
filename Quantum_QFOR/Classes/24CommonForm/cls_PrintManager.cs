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

using CrystalDecisions.CrystalReports.Engine;
using CrystalDecisions.Shared;
using Microsoft.VisualBasic;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.IO;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_PrintManager : CommonFeatures
    {
        public cls_BlClauseForHblMbl objClsBlClause = new cls_BlClauseForHblMbl();

        CommonFeatures objrep = new CommonFeatures();
        #region "Document Options"
        public object FetchDocumentOptions(int business_type, int process_type, int STATIONARY_TYPE)
        {

            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append( "SELECT DPMT.DOCUMENT_NAME DOCUMENT_NAME,' ' COPIES, 'false' Print, DPMT.DOCUMENT_ID DOCID");
            strSQL.Append( "FROM DOCUMENT_PRINT_MGR_TBL  DPMT");
            strSQL.Append( "WHERE DPMT.BUSINESS_TYPE = " + business_type);
            strSQL.Append( "AND DPMT.PROCESS_TYPE = " + process_type);
            strSQL.Append( "AND DPMT.STATIONARY_TYPE = " + STATIONARY_TYPE);
            strSQL.Append( "ORDER BY DPMT.PRIORITY ASC");

            try
            {
                return objWF.GetDataSet(strSQL.ToString());
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

        #region "FetchExportJobRef Enhanced Search"
        public string FetchExportJobRef(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strLOCATION_IN = null;
            string strCust = "0";
            string strFCL = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strBusiType = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOCATION_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                strCust = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                strFCL = Convert.ToString(arr.GetValue(5));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_REF_PRINT_MGR_PKG.GET_JOB_REF_EXP_PM";
                var _with1 = SCM.Parameters;
                _with1.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with1.Add("CUST_MST_FK_IN", (strCust == "0" ? "" : strCust)).Direction = ParameterDirection.Input;
                _with1.Add("IS_FCL_IN", ifDBNull(strFCL)).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
        #endregion

        #region "FetchImportJobRef Enhanced Search"
        public string FetchImportJobRef(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string jobtype = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBusiType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                jobtype = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_REF_PRINT_MGR_PKG.GET_JOB_REF_IMP_PM_LOC";
                var _with2 = SCM.Parameters;
                _with2.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_MST_FK_IN", loc).Direction = ParameterDirection.Input;
                _with2.Add("JOB_TYPE_IN", jobtype).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob.ToString());
                strReturn = strReader.ReadToEnd();
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
        #endregion

        #region "FetchImportJobRef_DO Enhanced Search"
        public string FetchImportJobRef_DO(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string LCL_FCL = "0";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBusiType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                LCL_FCL = Convert.ToString(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_REF_PRINT_MGR_PKG.GET_JOB_REF_IMP_PM_LOC_DO";
                var _with3 = SCM.Parameters;
                _with3.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with3.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with3.Add("FCL_LCL_IN", LCL_FCL).Direction = ParameterDirection.Input;
                _with3.Add("LOCATION_MST_FK_IN", strLOC_MST_IN).Direction = ParameterDirection.Input;
                _with3.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob.ToString());
                strReturn = strReader.ReadToEnd();
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
        #endregion

        #region "FetchImportJobRef_DOList Enhanced Search"
        public string FetchImportJobRef_DOList(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBusiType = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_REF_PRINT_MGR_PKG.GET_JOB_REF_IMP_PM_LOC_DOLIST";
                var _with4 = SCM.Parameters;
                _with4.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with4.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with4.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION_MST_FK_IN", strLOC_MST_IN).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1500, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
        #endregion

        #region "FetchImportJobRef_CAN Enhanced Search"
        public string FetchImportJobRef_CAN(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            string C5 = null;
            string C6 = null;
            string C7 = null;
            string C8 = null;
            string C9 = null;
            string C10 = null;
            var strNull = DBNull.Value;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBusiType = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 5)
            {
                C5 = Convert.ToString(arr.GetValue(5));
            }
            if (arr.Length > 6)
            {
                C6 = Convert.ToString(arr.GetValue(6));
            }
            if (arr.Length > 7)
            {
                C7 = Convert.ToString(arr.GetValue(7));
            }
            if (arr.Length > 8)
            {
                C8 = Convert.ToString(arr.GetValue(8));
            }
            if (arr.Length > 9)
            {
                C9 = Convert.ToString(arr.GetValue(9));
            }
            if (arr.Length > 10)
            {
                C10 = Convert.ToString(arr.GetValue(10));
            }
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_REF_PRINT_MGR_PKG.GET_JOB_REF_IMP_LOC_CAN";
                var _with5 = SCM.Parameters;
                _with5.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with5.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with5.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with5.Add("LOCATION_MST_FK_IN", strLOC_MST_IN).Direction = ParameterDirection.Input;
                _with5.Add("MASTERJOBCARD_PK_IN", Convert.ToString(arr.GetValue(4))).Direction = ParameterDirection.Input;
                _with5.Add("PARAM5_IN", (string.IsNullOrEmpty(C5) ? "" : C5)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM6_IN", (string.IsNullOrEmpty(C6) ? "" : C6)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM7_IN", (string.IsNullOrEmpty(C7) ? "" : C7)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM8_IN", (string.IsNullOrEmpty(C8) ? "" : C8)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM9_IN", (string.IsNullOrEmpty(C9) ? "" : C9)).Direction = ParameterDirection.Input;
                _with5.Add("PARAM10_IN", (string.IsNullOrEmpty(C10) ? "" : C10)).Direction = ParameterDirection.Input;
                _with5.Add("RETURN_VALUE", OracleDbType.Long, 32767, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
        #endregion

        #region "FetchMatser_JobRefNo_Imp_For_CAN Enhanced Search"
        public string FetchMatser_JobRefNo_Imp_For_CAN(string strCond, string loc = "0")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strLOC_MST_IN = "";
            string strBusiType = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strBusiType = Convert.ToString(arr.GetValue(3));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_REF_PRINT_MGR_PKG.GET_MASTERJOB_REF_IMP_LOC_CAN";
                var _with6 = SCM.Parameters;
                _with6.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with6.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with6.Add("BUSINESS_TYPE_IN", strBusiType).Direction = ParameterDirection.Input;
                _with6.Add("LOCATION_MST_FK_IN", strLOC_MST_IN).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", OracleDbType.NVarchar2, 2000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
        #endregion

        #region " Supporting Function "
        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return col;
            }
        }
        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, DBNull.Value))
            {
                return "";
            }
            return col;
        }
        #endregion

        #region "Shipping standard note"
        public object SSNExpSea(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            cls_JobCardView objclsJobCardView = new cls_JobCardView();
            ReportDocument repDoc = new ReportDocument();
            DataSet MainDS = new DataSet();
            DataSet CntDS = new DataSet();
            Int32 TotVol = 0;
            Int32 TotGrossWt = 0;

            try
            {
                repDoc.Load(rptPath + "\\rptStandaredShippingNote.rpt");
                MainDS = objclsJobCardView.FetchSSN(Convert.ToString(JOBPK));
                if (MainDS.Tables[0].Rows.Count > 0)
                {
                    MainDS.ReadXmlSchema(rptPath + "\\StandardShippingNote.xsd");
                    TotGrossWt = objclsJobCardView.FetchGrossWt(Convert.ToString(JOBPK));
                    TotVol = objclsJobCardView.FetchVolume(Convert.ToString(JOBPK));
                    repDoc.SetDataSource(MainDS);

                    repDoc.SetParameterValue(0, TotGrossWt);
                    repDoc.SetParameterValue(1, TotVol);
                }
                else
                {
                    repDoc = null;
                }
                return repDoc;

            }
            catch (Exception ex)
            {
                repDoc = null;
                return repDoc;
            }
        }
        public object SSNExpAir(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            clsJobCardAirExport objJobCardAir = new clsJobCardAirExport();
            CommonFeatures objrep = new CommonFeatures();
            //Added By Prakash Chandra on 16/12/2008 for task: Hard coded Report fields.
            ReportDocument repDoc = new ReportDocument();
            DataSet MainDS = new DataSet();
            DataSet CntDS = new DataSet();
            Int32 TotVol = 0;
            Int32 TotGrossWt = 0;
            try
            {
                repDoc.Load(rptPath + "\\rptStandaredShippingNote.rpt");
                MainDS = objJobCardAir.FetchSSN(Convert.ToString(JOBPK));
                if (MainDS.Tables[0].Rows.Count > 0)
                {
                    MainDS.ReadXmlSchema(rptPath + "\\StandardShippingNote.xsd");
                    TotGrossWt = objJobCardAir.FetchGrossWt(Convert.ToString(JOBPK));
                    TotVol = objJobCardAir.FetchVolume(Convert.ToString(JOBPK));
                    repDoc.SetDataSource(MainDS);

                    repDoc.SetParameterValue(0, TotGrossWt);
                    repDoc.SetParameterValue(1, TotVol);
                }
                else
                {
                    repDoc = null;
                }
                return repDoc;
            }
            catch (Exception ex)
            {
                repDoc = null;
                return repDoc;
            }
        }
        #endregion

        #region "Header Document Export Sea"
        public object HeaderDocumentExpSea(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            ReportDocument repDoc = new ReportDocument();
            DataSet MainRptDS = new DataSet();
            DataSet CntDS = new DataSet();
            DataSet AddressDS = new DataSet();
            int i = 0;
            int j = 0;
            string CONTAINER = "";
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            cls_JobCardSearch objJobCardSea = new cls_JobCardSearch();
            clsQuotationReport objQuotReport = new clsQuotationReport();
            ReportDocument repDocUsr = new ReportDocument();
            string Str = null;
            DataSet im = new DataSet();
            try
            {
                repDoc.Load(mapPath + "\\HeaderDocument.rpt");
                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                MainRptDS = objJobCardSea.FetchSeaAcknowledgement(Convert.ToString(JOBPK));
                CntDS = objJobCardSea.FetchSeaContainers(Convert.ToString(JOBPK));
                for (i = 0; i <= CntDS.Tables[0].Rows.Count - 1; i++)
                {
                    if (!string.IsNullOrEmpty(CntDS.Tables[0].Rows[i]["CONTAINER"].ToString()))
                    {
                        CONTAINER = CONTAINER + CntDS.Tables[0].Rows[i]["CONTAINER"] + ",";
                    }
                }
                if ((CONTAINER.LastIndexOf(",") != -1))
                {
                    CONTAINER = CONTAINER.Remove(CONTAINER.LastIndexOf(","), 1);
                }
                MainRptDS.ReadXmlSchema(mapPath + "\\Main_Acknowledgement.xsd");

                AddressDS.ReadXmlSchema(rptPath + "\\TN_Location.xsd");

                repDoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);
                repDoc.SetDataSource(MainRptDS);

                repDoc.SetParameterValue(0, (CONTAINER == null ? "" : CONTAINER));
                repDoc.SetParameterValue(1, 1);
                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Header Document Import Sea"
        public object HeaderDocumentImpSea(long JobCardPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            cls_JobCardSearch objHeader = new cls_JobCardSearch();
            clsQuotationReport objQuotReport = new clsQuotationReport();
            ReportDocument repDoc = new ReportDocument();
            DataSet MainDS = new DataSet();
            DataSet CntDS = new DataSet();
            DataSet AddressDS = new DataSet();

            int I = 0;
            int j = 0;
            string CONTAINER = "";

            try
            {
                repDoc.Load(mapPath + "\\HeaderDocument.rpt");
                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

                AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                MainDS = objHeader.FetchSeaImpHeaderDocment(Convert.ToInt32(JobCardPK));
                MainDS.Tables[0].Columns.Add("Containers", typeof(string));
                CntDS = objHeader.FetchSeaImpContainers(Convert.ToString(JobCardPK));
                for (I = 0; I <= CntDS.Tables[0].Rows.Count - 1; I++)
                {
                    if (!string.IsNullOrEmpty(CntDS.Tables[0].Rows[I]["CONTAINER"].ToString()))
                    {
                        CONTAINER = CONTAINER + CntDS.Tables[0].Rows[I]["CONTAINER"] + ",";
                    }
                }
                if ((CONTAINER.LastIndexOf(",") != -1))
                {
                    CONTAINER = CONTAINER.Remove(CONTAINER.LastIndexOf(","), 1);
                }
                MainDS.ReadXmlSchema(mapPath + "\\Main_Acknowledgement.xsd");
                AddressDS.ReadXmlSchema(rptPath + "\\TN_Location.xsd");
                repDoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);
                repDoc.SetDataSource(MainDS);
                repDoc.SetParameterValue(0, (CONTAINER == null ? "" : CONTAINER));
                repDoc.SetParameterValue(1, 2);
                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Geader Document Export Air"
        public object HeaderDocumentExpAir(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            ReportDocument repDoc = new ReportDocument();
            DataSet MainDS = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet CntDS = new DataSet();
            int i = 0;
            string CONTAINER = "";
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            clsJobCardAirExport objJobCardAir = new clsJobCardAirExport();
            clsQuotationReport objQuotReport = new clsQuotationReport();
            int j = 0;

            try
            {
                repDoc.Load(mapPath + "\\HeaderDocument.rpt");
                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                MainDS = objJobCardAir.FetchAirAcknowledgement(Convert.ToString(JOBPK));
                MainDS.Tables[0].Columns.Add("Containers", typeof(string));
                CntDS = objJobCardAir.FetchSeaContainers(Convert.ToString(JOBPK));
                for (i = 0; i <= CntDS.Tables[0].Rows.Count - 1; i++)
                {
                    if (!string.IsNullOrEmpty(CntDS.Tables[0].Rows[i]["CONTAINER"].ToString()))
                    {
                        CONTAINER = CONTAINER + CntDS.Tables[0].Rows[i]["CONTAINER"] + ",";
                    }
                }
                if ((CONTAINER.LastIndexOf(",") != -1))
                {
                    CONTAINER = CONTAINER.Remove(CONTAINER.LastIndexOf(","), 1);
                }
                MainDS.ReadXmlSchema(mapPath + "\\Main_Acknowledgement.xsd");
                AddressDS.ReadXmlSchema(rptPath + "\\TN_Location.xsd");
                repDoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);
                repDoc.SetDataSource(MainDS);

                repDoc.SetParameterValue(0, (CONTAINER == null ? "" : CONTAINER));
                repDoc.SetParameterValue(1, 1);
                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Header Document Import Air"
        public object HeaderDocumentImpAir(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            clsJobCardAirExport objJobCardAir = new clsJobCardAirExport();
            cls_JobCardSearch objJobCardSea = new cls_JobCardSearch();
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            clsQuotationReport objQuotReport = new clsQuotationReport();
            ReportDocument repDoc = new ReportDocument();
            DataSet MainDS = new DataSet();
            DataSet CntDS = new DataSet();
            DataSet AddressDS = new DataSet();
            int i = 0;
            int j = 0;
            string CONTAINER = "";
            try
            {
                repDoc.Load(mapPath + "\\HeaderDocument.rpt");
                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                //end
                MainDS = objJobCardAir.FetchImpHeaderDocment(Convert.ToInt32(JOBPK));
                MainDS.Tables[0].Columns.Add("Containers", typeof(string));
                CntDS = objJobCardAir.FetchAIRImpContainers(Convert.ToString(JOBPK));
                for (i = 0; i <= CntDS.Tables[0].Rows.Count - 1; i++)
                {
                    if (!string.IsNullOrEmpty(CntDS.Tables[0].Rows[i]["CONTAINER"].ToString()))
                    {
                        CONTAINER = CONTAINER + CntDS.Tables[0].Rows[i]["CONTAINER"] + ",";
                    }
                }
                if ((CONTAINER.LastIndexOf(",") != -1))
                {
                    CONTAINER = CONTAINER.Remove(CONTAINER.LastIndexOf(","), 1);
                }
                MainDS.ReadXmlSchema(mapPath + "\\Main_Acknowledgement.xsd");
                AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);

                AddressDS.ReadXmlSchema(rptPath + "\\TN_Location.xsd");
                repDoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);
                repDoc.SetDataSource(MainDS);
                repDoc.SetParameterValue(0, (CONTAINER == null ? "" : CONTAINER));
                repDoc.SetParameterValue(1, 2);
                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Arrival Notice Import Sea"
        public object ArrivalNoticeImpAirSea(string JobCardPK, long logged_in_loc_fk, string rptPath, Int32 BizType, string PrePrinted = "")
        {
            object functionReturnValue = null;
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            Cls_Arrival_Notice objArrival = new Cls_Arrival_Notice();
            ReportDocument repDoc = new ReportDocument();
            DataSet MainRptDS = new DataSet();
            DataSet SubRptDS = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet CargoDS = new DataSet();
            int i = 0;
            int Type = 0;
            string Containers = null;
            string Fre_Currency = null;
            string JOBNO = null;
            string Fre_Amt = null;
            DataSet dsClause = new DataSet();
            string FormFlag = null;
            string CanNumber = null;
            string PodPK = null;
            int Clause = 0;

            try
            {
                AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                AddressDS.ReadXmlSchema(rptPath + "\\TN_Location.xsd");
                CanNumber = GetCan(JobCardPK);
                if ((CanNumber == null))
                {
                    return functionReturnValue;
                }
                if (BizType == 2)
                {
                    MainRptDS = objArrival.FetchJobCardSeaDetails(JobCardPK);
                    SubRptDS = objArrival.FetchCollectChargesSeaDetails(JobCardPK);
                    CargoDS = objArrival.FetchSeaContainers(JobCardPK);
                    Type = 2;
                    FormFlag = "Sea";
                }
                else
                {
                    MainRptDS = objArrival.FetchJobCardAirDetails(JobCardPK);
                    SubRptDS = objArrival.FetchCollectChargesAirDetails(JobCardPK);
                    CargoDS = objArrival.FetchAirPalette(JobCardPK);
                    Type = 1;
                    FormFlag = "Air";
                }
                PodPK = objArrival.GetPodPK(JobCardPK, Convert.ToString(BizType));
                if (SubRptDS.Tables[0].Rows.Count > 0)
                {
                    Fre_Amt = Convert.ToString(SubRptDS.Tables[0].Rows[0]["FREIGHT_AMT"]);
                    Fre_Currency = Convert.ToString(SubRptDS.Tables[0].Rows[0]["CURRENCY_ID"]);
                }
                else
                {
                    Fre_Amt = "";
                    Fre_Currency = "";
                }
                MainRptDS.Tables[0].Columns.Add("Containers", typeof(string));
                Array PkArr = null;
                string strContainers = "";
                PkArr = JobCardPK.Split(',');
                Int16 RowCnt = default(Int16);
                Int16 pkCount = default(Int16);
                for (pkCount = 0; pkCount <= PkArr.Length - 1; pkCount++)
                {
                    for (RowCnt = 0; RowCnt <= MainRptDS.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        if (MainRptDS.Tables[0].Rows[RowCnt]["JOBCARDPK"] == PkArr.GetValue(pkCount))
                        {
                            Int16 cntRNo = default(Int16);
                            for (cntRNo = 0; cntRNo <= CargoDS.Tables[0].Rows.Count - 1; cntRNo++)
                            {
                                if (CargoDS.Tables[0].Rows[cntRNo]["JOBPK"] == PkArr.GetValue(pkCount))
                                {
                                    strContainers += Convert.ToString(getDefault(CargoDS.Tables[0].Rows[cntRNo]["CONTAINER"], "")).Trim() + ",";
                                }
                            }
                            MainRptDS.Tables[0].Rows[RowCnt]["Containers"] = strContainers.TrimEnd(',');
                            strContainers = "";
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }
                }

                if (PrePrinted == "PrePrinted")
                {
                    repDoc.Load(rptPath + "\\ArrivalNotice1_New.rpt");
                }
                else
                {
                    repDoc.Load(rptPath + "\\ArrivalNotice.rpt");
                }


                repDoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);
                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

                dsClause = objClsBlClause.FetchBlClausesForHBL("", 6, 1, 1, PodPK, "", Convert.ToInt64(JobCardPK), DateTime.Now.Date.ToString(), ((FormFlag == null) ? "" : FormFlag));
                dsClause.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\PrintBLClause.xsd");
                repDoc.OpenSubreport("rptBLClause").SetDataSource(dsClause);

                repDoc.SetDataSource(MainRptDS);
                if (dsClause.Tables.Count > 0)
                {
                    if (dsClause.Tables[0].Rows.Count > 0)
                    {
                        Clause = 1;
                    }
                }
                if (PrePrinted == "PrePrinted")
                {
                    repDoc.SetParameterValue("Currency", "");
                    repDoc.SetParameterValue("Biztype", BizType);
                }
                else
                {
                    repDoc.SetParameterValue("Container", "");
                }
                repDoc.SetParameterValue("Clause", Clause);
                repDoc.SetParameterValue("CustRefNr", (string.IsNullOrEmpty(MainRptDS.Tables[0].Rows[0]["CUSTOM_REF_NO"].ToString()) ? "" : MainRptDS.Tables[0].Rows[0]["CUSTOM_REF_NO"]));
                repDoc.SetParameterValue("CustRefDt", (string.IsNullOrEmpty(MainRptDS.Tables[0].Rows[0]["CUSTOM_REF_DT"].ToString()) ? "" : MainRptDS.Tables[0].Rows[0]["CUSTOM_REF_DT"]));
                repDoc.SetParameterValue("CustItemNr", (string.IsNullOrEmpty(MainRptDS.Tables[0].Rows[0]["CUSTOM_ITEM_NR"].ToString()) ? "" : MainRptDS.Tables[0].Rows[0]["CUSTOM_ITEM_NR"]));
                repDoc.SetParameterValue(0, getDefault(AddressDS.Tables[0].Rows[0]["corporate_name"], ""));
                repDoc.SetParameterValue(1, Type);
                repDoc.SetParameterValue(2, Fre_Amt);
                repDoc.SetParameterValue(3, Fre_Currency);
                repDoc.SetParameterValue("RemDate", "");
                repDoc.SetParameterValue("RemText", "");
                repDoc.SetParameterValue("R1_Date", "");
                getReportControls(repDoc, "QFOR3040");
                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }
        #endregion

        #region "GET CAN NUMBER"
        public string GetCan(string JOBPK)
        {
            WorkFlow Objwk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("  SELECT CAN.CAN_REF_NO FROM CAN_MST_TBL CAN WHERE CAN.JOB_CARD_FK=" + JOBPK);
            try
            {
                return Objwk.ExecuteScaler(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }
        #endregion

        #region "Payment Requisition Form Air/Sea"
        public ReportDocument PaymentRequisitionFormAirSea(string JOBPPK, long logged_in_loc_fk, string rptPath, Int32 BizType, string User_Name, bool Export)
        {
            ReportDocument repDoc = new ReportDocument();
            DataSet MainRptDS = new DataSet();
            DataSet AddressDS = new DataSet();
            Int16 RowCount = default(Int16);
            Int16 Type = default(Int16);
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            Cls_Payment_Requisition objPayment = new Cls_Payment_Requisition();
            try
            {
                AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                if (Export == true)
                {
                    if (BizType == 2)
                    {
                        MainRptDS = objPayment.FetchSeaExpPaymentReportPrintManager(JOBPPK);
                    }
                    else
                    {
                        MainRptDS = objPayment.FetchAirExpPaymentReportPrintManager(JOBPPK);
                    }
                }
                else
                {
                    if (BizType == 2)
                    {
                        MainRptDS = objPayment.FetchSeaImpPaymentReportPrintManager(JOBPPK);
                    }
                    else
                    {
                        MainRptDS = objPayment.FetchAirImpPaymentReportPrintManager(JOBPPK);
                    }
                }

                repDoc.Load(rptPath + "\\rptPaymentRequisition.rpt");
                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                repDoc.SetDataSource(MainRptDS);
                repDoc.SetParameterValue(0, getDefault(AddressDS.Tables[0].Rows[0]["corporate_name"], ""));
                repDoc.SetParameterValue(1, User_Name);
                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        #endregion

        #region " Imagefile function for LOGO Image"
        public static DataSet ImageTable(string ImageFile)
        {
            DataSet dsBody = new DataSet();
            DataSet GridDS = new DataSet();
            DataTable data = new DataTable();
            DataRow row = null;
            data.TableName = "Images";
            data.Columns.Add("img", System.Type.GetType("System.Byte[]"));
            FileStream fs = new FileStream(ImageFile, FileMode.Open);
            BinaryReader br = new BinaryReader(fs);
            row = data.NewRow();
            row[0] = br.ReadBytes(Convert.ToInt32(br.BaseStream.Length));
            data.Rows.Add(row);
            dsBody.Tables.Add(data);
            br = null;
            fs.Close();
            fs = null;
            return dsBody;
        }
        public DataSet ds_image()
        {
            string ImgLogoFileName = Convert.ToString(HttpContext.Current.Session["ImageFile"]);
            DataSet dsImage = new DataSet();
            string ImgLogoPath = HttpContext.Current.Server.MapPath("..\\..") + "\\Logos\\" + ImgLogoFileName;
            if (!File.Exists(ImgLogoPath))
                ImgLogoPath = HttpContext.Current.Server.MapPath("..\\..") + "\\Logos\\defaultLogo.jpg";
            dsImage = ImageTable(ImgLogoPath);
            dsImage.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\ckim.xsd");
            return dsImage;
        }
        #endregion

        #region "Certificate Of Shipment Air/Sea"
        public object CertificateOfShipmentAirSea(string JobRefs, long logged_in_loc_fk, string rptPath, Int32 BizType)
        {
            ReportDocument Rep = new ReportDocument();
            DataSet dsLoc = null;
            DataSet dsMain = null;
            DataSet dsmarksdesc = null;
            DataSet dssummary = null;
            string VslVoy_Flight = null;
            string Cont_Pal = null;
            string h_con = null;
            string F_V = null;
            string Hbl_Hawb = null;
            string Mbl_Mawb = null;
            Int16 I = default(Int16);
            Int16 j = default(Int16);
            Int16 Type = default(Int16);
            Cls_Exp_Certificate ObjClsExpCertificate = new Cls_Exp_Certificate();
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            string InvoiceRefNo = "";
            clsQuotationReport objQuotReport = new clsQuotationReport();
            try
            {
                if (JobRefs == null | JobRefs == "undefined")
                {
                    throw new System.Exception("Please select the Records(s) again");
                }
                Rep.Load(rptPath + "\\rptExpCertificate.rpt");
                dsLoc = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                dsLoc.ReadXmlSchema(rptPath + "\\TN_Location.xsd");
                if (BizType == 2)
                {
                    Rep.Load(rptPath + "\\rptExpCertificate.rpt");
                    dsMain = ObjClsExpCertificate.FetchSeaMain(JobRefs);
                    dsmarksdesc = ObjClsExpCertificate.FetchSeaContainers(JobRefs);
                    dssummary = ObjClsExpCertificate.FetchCollectChargesSeaDetails(JobRefs);
                    VslVoy_Flight = "";
                    Cont_Pal = "Container No";
                    h_con = "Container Details:";
                    F_V = "Vessel-Voyage";
                    Hbl_Hawb = "HBL Ref No";
                    Mbl_Mawb = "MBL Ref No";
                    Type = 2;

                    dsMain.Tables[0].Columns.Add("Containers", typeof(string));
                    Array PkArr = null;
                    string strContainers = "";
                    PkArr = JobRefs.Split(',');
                    Int16 RowCnt = default(Int16);
                    Int16 pkCount = default(Int16);
                    for (pkCount = 0; pkCount <= PkArr.Length - 1; pkCount++)
                    {
                        for (RowCnt = 0; RowCnt <= dsMain.Tables[0].Rows.Count - 1; RowCnt++)
                        {
                            if (dsMain.Tables[0].Rows[RowCnt]["JOBCARDPK"] == PkArr.GetValue(pkCount))
                            {
                                Int16 cntRNo = default(Int16);
                                for (cntRNo = 0; cntRNo <= dsmarksdesc.Tables[0].Rows.Count - 1; cntRNo++)
                                {
                                    if (dsmarksdesc.Tables[0].Rows[cntRNo]["JOBPK"] == PkArr.GetValue(pkCount))
                                    {
                                        strContainers += Convert.ToString(getDefault(dsmarksdesc.Tables[0].Rows[cntRNo]["CONTAINER"], "")).Trim() + ",";
                                    }
                                }
                                dsMain.Tables[0].Rows[RowCnt]["Containers"] = strContainers.TrimEnd(',');
                                strContainers = "";
                                break; // TODO: might not be correct. Was : Exit For
                            }
                        }
                    }
                }
                else
                {
                    Rep.Load(rptPath + "\\rptAirCertificate.rpt");
                    dsMain = ObjClsExpCertificate.FetchAirFlightMain(JobRefs);
                    dssummary = ObjClsExpCertificate.FetchAirInvoice(JobRefs);
                    dsMain.Tables[0].Columns.Add("INVOICENO", typeof(string));
                    Array PkArr = null;
                    string strContainers = "";
                    PkArr = JobRefs.Split(',');
                    Int16 RowCnt = default(Int16);
                    Int16 pkCount = default(Int16);
                    for (pkCount = 0; pkCount <= PkArr.Length - 1; pkCount++)
                    {
                        for (RowCnt = 0; RowCnt <= dsMain.Tables[0].Rows.Count - 1; RowCnt++)
                        {
                            if (dsMain.Tables[0].Rows[RowCnt]["JOBCARDPK"] ==  PkArr.GetValue(pkCount))
                            {
                                Int16 cntRNo = default(Int16);
                                for (cntRNo = 0; cntRNo <= dssummary.Tables[0].Rows.Count - 1; cntRNo++)
                                {
                                    if (dssummary.Tables[0].Rows[cntRNo]["JOBCARDPK"] ==  PkArr.GetValue(pkCount))
                                    {
                                        InvoiceRefNo += Convert.ToString(getDefault(dssummary.Tables[0].Rows[cntRNo]["INVOICENO"], "")).Trim() + ",";
                                    }
                                }
                                dsMain.Tables[0].Rows[RowCnt]["INVOICENO"] = InvoiceRefNo.TrimEnd(',');
                                InvoiceRefNo = "";
                                break; // TODO: might not be correct. Was : Exit For
                            }

                        }
                    }
                }
                Rep.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

                if (BizType == 2)
                {
                    dsMain.ReadXmlSchema(rptPath + "\\CS_MAIN.xsd");
                    dsmarksdesc.ReadXmlSchema(rptPath + "\\CS_Containers.xsd");
                    dssummary.ReadXmlSchema(rptPath + "\\CS_Freight.xsd");
                    Rep.OpenSubreport("rptCSlocation").SetDataSource(dsLoc);
                    Rep.SetDataSource(dsMain);
                    Rep.SetParameterValue(0, dsLoc.Tables[0].Rows[0][0]);
                    Rep.SetParameterValue(1, dsLoc.Tables[0].Rows[0]["ADDRESS_LINE1"]);
                    Rep.SetParameterValue(2, Cont_Pal);
                    Rep.SetParameterValue(3, h_con);
                    Rep.SetParameterValue(4, F_V);
                    Rep.SetParameterValue(5, Hbl_Hawb);
                    Rep.SetParameterValue(6, Mbl_Mawb);
                    Rep.SetParameterValue(7, Type);
                    getReportControls(Rep, "QFOR3042", 2);
                }
                else
                {
                    dsMain.ReadXmlSchema(rptPath + "\\ExpAirCertificate.xsd");
                    Rep.OpenSubreport("Address").SetDataSource(dsLoc);
                    Rep.SetDataSource(dsMain);
                    Rep.SetParameterValue(0, getDefault(dsLoc.Tables[0].Rows[0]["CORPORATE_NAME"], ""));
                    Rep.SetParameterValue(1, InvoiceRefNo);
                    getReportControls(Rep, "QFOR3042", 1);
                }
                return Rep;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Movement Load List Export Sea"
        public object MovementLoadListExpSea(long JOBPK, long logged_in_loc_fk, string mapPath)
        {
            ReportDocument repDoc = new ReportDocument();
            DataSet MainDS = new DataSet();
            DataSet AddressDS = new DataSet();
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            cls_JobCardSearch objHeader = new cls_JobCardSearch();
            try
            {
                AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                repDoc.Load(mapPath + "\\rptMovementLoadingList.rpt");
                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

                MainDS = objHeader.FetchMovementListing(Convert.ToInt32(JOBPK));
                MainDS.ReadXmlSchema(mapPath + "\\MovementLoading.xsd");
                AddressDS.ReadXmlSchema(mapPath + "\\AddressDtls.xsd");
                repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);

                repDoc.OpenSubreport("AgentDtls");
                repDoc.SetDataSource(MainDS);
                repDoc.SetParameterValue("Type", 2);
                getReportControls(repDoc, "QFOR4040", 1);
                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Movement Load List Export Air"
        public object MovementLoadListExpAir(long JOBPK, long logged_in_loc_fk, string mapPath)
        {
            ReportDocument repDoc = new ReportDocument();
            DataSet MainDS = new DataSet();
            DataSet AddressDS = new DataSet();
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            clsJobCardAirExport objJobCardAir = new clsJobCardAirExport();

            try
            {
                AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                repDoc.Load(mapPath + "\\rptMovementLoadingList.rpt");
                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                MainDS = objJobCardAir.FetchMovementListing(Convert.ToInt32(JOBPK));
                MainDS.ReadXmlSchema(mapPath + "\\MovementLoading.xsd");
                AddressDS.ReadXmlSchema(mapPath + "\\AddressDtls.xsd");
                repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);

                repDoc.OpenSubreport("AgentDtls");
                repDoc.SetDataSource(MainDS);
                repDoc.SetParameterValue("Type", 1);
                getReportControls(repDoc, "QFOR4040", 1);
                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "HBL"
        public object HBL(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath, string strMsg, string PrePrinted = "")
        {
            cls_HBL_List ObjClsHBLprinting = new cls_HBL_List();
            WorkFlow objEmp = new WorkFlow();
            ReportDocument rptdoc = new ReportDocument();
            DataSet dsmain = null;
            DataSet dsdetails = null;
            DataSet dsPackage = null;
            DataSet dsDelAddr = null;
            DataSet dsConDet = new DataSet();
            DataSet dshbl = new DataSet();
            DataSet dsExtra = null;
            DataSet dsLoc = null;
            DataSet dsBlClauses = null;
            DataSet dsBlClausesNew = null;
            string NoOfPackages = "0";
            string ContainerDet = "";
            string strBlClauses = "";
            string PODPK = null;
            string CommPK = null;
            string FormFlag = null;
            int I = 0;
            int j = 0;
            string BarCode = "";
            string HBKReturnValue = null;
            Int32 hblvalue = 0;
            cls_HBL_Entry objHBLEntry = new cls_HBL_Entry();
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            Int32 HBLPk = default(Int32);
            clsQuotationReport objQuotReport = new clsQuotationReport();
            string currID = null;
            currID = Convert.ToString(HttpContext.Current.Session["CURRENCY_ID"]);
            try
            {
                HBKReturnValue = objEmp.ExecuteScaler("SELECT j.HBL_HAWB_FK HBL_EXP_TBL_FK FROM JOB_CARD_TRN j WHERE j.job_card_trn_pk = " + JOBPK);
                HBLPk = (!string.IsNullOrEmpty(HBKReturnValue) ? Convert.ToInt32(HBKReturnValue) : 0);

                objEmp = null;
                if (HBLPk > 0)
                {
                    dsmain = ObjClsHBLprinting.FetchMainHBL(Convert.ToString(HBLPk),"" , Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                    if (dsmain.Tables[0].Rows.Count > 0)
                    {
                        if (string.IsNullOrEmpty(Convert.ToString(dsmain.Tables[0].Rows[0]["HBLDATE"])))
                        {
                            strMsg = "Actual departure date is not available for HBL Printing,";
                            rptdoc = null;
                            return rptdoc;
                        }
                    }
                    dsPackage = ObjClsHBLprinting.FetchPackages(Convert.ToString(HBLPk));
                    for (I = 0; I <= dsPackage.Tables[0].Rows.Count - 1; I++)
                    {
                        if (I == 0)
                        {
                            NoOfPackages = Convert.ToString(dsPackage.Tables[0].Rows[I]["pack"]);
                        }
                        else
                        {
                            NoOfPackages = NoOfPackages + "," + dsPackage.Tables[0].Rows[I]["pack"];
                        }
                    }

                    if (NoOfPackages.Length <= 1)
                    {
                        NoOfPackages = "0";
                    }

                    if (PrePrinted == "PrePrinted")
                    {
                        rptdoc.Load(rptPath + "\\rptHBLPrinting_FIATA_New.rpt");
                    }
                    else
                    {
                        rptdoc.Load(rptPath + "\\rptHBLPrinting_FIATA.rpt");
                    }
                    rptdoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                    dsmain.ReadXmlSchema(rptPath + "\\HBLMAIN.XSD");

                    dsdetails = ObjClsHBLprinting.FetchHBLDetails(Convert.ToString(HBLPk));
                    dsdetails.ReadXmlSchema(rptPath + "\\HBLDET.XSD");

                    dsBlClauses = ObjClsHBLprinting.FetchBlClauses(Convert.ToString(HBLPk));
                    dsBlClauses.ReadXmlSchema(rptPath + "\\BlClausesForHBL.XSD");

                    dsDelAddr = ObjClsHBLprinting.FetchDelAddress(Convert.ToString(HBLPk));
                    dsDelAddr.ReadXmlSchema(rptPath + "\\HBLDelAddr.xsd");

                    dsExtra = ObjClsHBLprinting.FetchDescExtra(Convert.ToString(HBLPk));
                    dsExtra.ReadXmlSchema(rptPath + "\\HBLDescExtra.xsd");

                    dsLoc = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                    dsLoc.ReadXmlSchema(rptPath + "\\TN_Location.xsd");

                    dsConDet = ObjClsHBLprinting.Get_ConDet(Convert.ToString(HBLPk));
                    dsConDet.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\CONDET.XSD");

                    dsBlClausesNew = objClsBlClause.FetchBlClausesForHBL("", 4, 1, 1, ((PODPK == null) ? "0" : PODPK), ((CommPK == null) ? "0" : CommPK), HBLPk, "", ((FormFlag == null) ? "" : FormFlag));
                    dsBlClausesNew.ReadXmlSchema(HttpContext.Current.Server.MapPath("../Documentation") + "\\PrintBLClause.XSD");


                    for (I = 0; I <= dsBlClauses.Tables[0].Rows.Count - 1; I++)
                    {
                        if (I == 0)
                        {
                            strBlClauses = Convert.ToString(dsBlClauses.Tables[0].Rows[I]["BL_DESCRIPTION"]);
                        }
                        else
                        {
                            strBlClauses = strBlClauses + "," + dsBlClauses.Tables[0].Rows[I]["BL_DESCRIPTION"];
                        }
                    }

                    if (objHBLEntry.GetBarcodeFlag("HBL") == "1" & ConfigurationSettings.AppSettings["ShowBarcode"] == "1")
                    {
                        BarCode = "*" + dsdetails.Tables[0].Rows[0]["hbl_ref_no"] + "*";
                    }
                    for (I = 0; I <= dsdetails.Tables[0].Rows.Count - 1; I++)
                    {
                        if (string.IsNullOrEmpty(dsdetails.Tables[0].Rows[I]["SEAL_NUMBER"].ToString()))
                        {
                            dsdetails.Tables[0].Rows[I]["SEAL_NUMBER"] = "";
                        }
                        else if (string.IsNullOrEmpty(dsdetails.Tables[0].Rows[I]["CONTAINER_NUMBER"].ToString()))
                        {
                            dsdetails.Tables[0].Rows[I]["CONTAINER_NUMBER"] = "";
                        }

                        if (I == 0)
                        {
                            ContainerDet = dsdetails.Tables[0].Rows[I]["CONTAINER_NUMBER"].ToString() + 
                                 dsdetails.Tables[0].Rows[I]["CONTAINER_TYPE_MST_ID"].ToString() + 
                                 dsdetails.Tables[0].Rows[I]["SEAL_NUMBER"].ToString() + 
                                dsdetails.Tables[0].Rows[I]["PACK_COUNT"].ToString() + 
                                dsdetails.Tables[0].Rows[I]["GROSS_WEIGHT"].ToString() + 
                                dsdetails.Tables[0].Rows[I]["TAREWEIGHT"].ToString() + 
                                dsdetails.Tables[0].Rows[I]["Volume"];
                        }
                        else
                        {
                            ContainerDet = ContainerDet + "," + dsdetails.Tables[0].Rows[I]["CONTAINER_NUMBER"].ToString() + dsdetails.Tables[0].Rows[I]["CONTAINER_TYPE_MST_ID"] + dsdetails.Tables[0].Rows[I]["SEAL_NUMBER"] + dsdetails.Tables[0].Rows[I]["PACK_COUNT"] + dsdetails.Tables[0].Rows[I]["GROSS_WEIGHT"] + dsdetails.Tables[0].Rows[I]["TAREWEIGHT"] + dsdetails.Tables[0].Rows[I]["Volume"];
                        }
                    }

                    rptdoc.OpenSubreport("rptConDetails").SetDataSource(dsConDet);
                    rptdoc.OpenSubreport("rptHBLMain").SetDataSource(dsmain);
                    rptdoc.OpenSubreport("rptHBLDelAddr").SetDataSource(dsDelAddr);
                    rptdoc.OpenSubreport("rptDescExtra").SetDataSource(dsExtra);
                    rptdoc.OpenSubreport("BLClause").SetDataSource(dsBlClauses);
                    rptdoc.OpenSubreport("CntrDetailsAnnexure").SetDataSource(dsConDet);

                    DataSet dsfright = new DataSet();
                    DataSet dsamt = new DataSet();
                    long frtamt = 0;
                    long othamt = 0;
                    string jobrefno = objHBLEntry.fetchjob(Convert.ToString(JOBPK));
                    dsfright = objHBLEntry.getfrightdtls("JOBREF");
                    dsfright.ReadXmlSchema(HttpContext.Current.Server.MapPath("../Documentation") + "\\HAWB_Freight.XSD");
                    dsamt = objHBLEntry.getamt(jobrefno);
                    rptdoc.OpenSubreport("rptBLClause").SetDataSource(dsBlClausesNew);
                    rptdoc.OpenSubreport("frightdtls").SetDataSource(dsfright);

                    rptdoc.SetDataSource(dsdetails);

                    rptdoc.SetParameterValue("credit", ObjClsHBLprinting.FetchCredit(Convert.ToString(HBLPk)));
                    rptdoc.SetParameterValue("Packs", NoOfPackages.Trim());
                    rptdoc.SetParameterValue("ContDetails", ContainerDet);
                    if (dsmain.Tables[0].Rows.Count > 0)
                    {
                        if (string.IsNullOrEmpty(dsmain.Tables[0].Rows[0]["PLACE_ISSUE"].ToString()) == true)
                        {
                            rptdoc.SetParameterValue("LoginLoc", "");
                        }
                        else
                        {
                            rptdoc.SetParameterValue("LoginLoc", dsmain.Tables[0].Rows[0]["PLACE_ISSUE"]);
                        }
                    }
                    DataSet dsLoadt = new DataSet();
                    dsLoadt = ObjClsHBLprinting.FetchLoadDate(Convert.ToString(HBLPk));
                    if (dsmain.Tables[0].Rows.Count > 0)
                    {
                        if (string.IsNullOrEmpty(dsmain.Tables[0].Rows[0]["VSLVOY"].ToString()) == true)
                        {
                            rptdoc.SetParameterValue("vslVoy", "");
                        }
                        else
                        {
                            rptdoc.SetParameterValue("vslVoy", dsmain.Tables[0].Rows[0]["VSLVOY"]);
                        }

                        if (string.IsNullOrEmpty(dsmain.Tables[0].Rows[0]["POL"].ToString()) == true)
                        {
                            rptdoc.SetParameterValue("POL", "");
                        }
                        else
                        {
                            rptdoc.SetParameterValue("POL", dsmain.Tables[0].Rows[0]["POL"]);
                        }

                        if (string.IsNullOrEmpty(dsmain.Tables[0].Rows[0]["HBLDATE"].ToString()) == true)
                        {
                            rptdoc.SetParameterValue("LoadDt", "");
                        }
                        else
                        {
                            rptdoc.SetParameterValue("LoadDt",Convert.ToDateTime(dsmain.Tables[0].Rows[0]["HBLDATE"]).ToString(dateFormat));
                        }
                    }
                    rptdoc.SetParameterValue("ApplyBarcode", GetBarcodeFlag("HBL"));
                    //'surya23Nov06
                    rptdoc.SetParameterValue("LoginUser", dsLoc.Tables[0].Rows[0]["ADDRESS_LINE1"]);
                    rptdoc.SetParameterValue("HBLDate", dsmain.Tables[0].Rows[0]["HBLDATE"]);
                    rptdoc.SetParameterValue("Barcode", BarCode);
                    rptdoc.SetParameterValue("status", dsmain.Tables[0].Rows[0]["status"]);

                    if (string.IsNullOrEmpty(dsmain.Tables[0].Rows[0]["POD"].ToString()) == true)
                    {
                        rptdoc.SetParameterValue("POD", "");
                    }
                    else
                    {
                        rptdoc.SetParameterValue("POD", dsmain.Tables[0].Rows[0]["POD"]);
                    }
                    rptdoc.SetParameterValue("HBLORIGINALPRINTS", dsmain.Tables[0].Rows[0]["HBL_ORIGINAL_PRINTS"]);
                    rptdoc.SetParameterValue("FreightAmt", dsmain.Tables[0].Rows[0]["FRGHTAMT"]);
                    rptdoc.SetParameterValue("OtherFreightAmt", dsmain.Tables[0].Rows[0]["OTHRFRGHTAMT"]);

                    //adding by thiyagarajan on 17/2/09:report parameteraization 
                    dshbl = HBLDts(HBLPk);
                    if (dshbl.Tables.Count > 0)
                    {
                        if (dshbl.Tables[0].Rows.Count > 0)
                        {
                            hblvalue = 1;
                            rptdoc.SetParameterValue("FreightReceivable", getDefault(dshbl.Tables[0].Rows[0]["Ptype"], ""));
                            rptdoc.SetParameterValue("CargoType", getDefault(dshbl.Tables[0].Rows[0]["Cargo"], ""));
                            rptdoc.SetParameterValue("Movecode", getDefault(dshbl.Tables[0].Rows[0]["CARGO_MOVE"], ""));
                            rptdoc.SetParameterValue("WayBill", dshbl.Tables[0].Rows[0]["CARGO_MOVE"]);
                        }
                    }
                    if (hblvalue == 0)
                    {
                        rptdoc.SetParameterValue("FreightReceivable", "");
                        rptdoc.SetParameterValue("CargoType", "");
                        rptdoc.SetParameterValue("Movecode", "");
                        rptdoc.SetParameterValue("WayBill", "");
                    }

                    if (dsfright.Tables.Count == 0)
                    {
                        frtamt = 0;
                        othamt = 0;
                    }
                    else
                    {
                        frtamt = Convert.ToInt32(getDefault(dsamt.Tables[0].Rows[0][0], 0));
                        othamt = Convert.ToInt32(getDefault(dsamt.Tables[0].Rows[1][0], 0));
                    }
                    rptdoc.SetParameterValue("fright", dsfright.Tables[0].Rows.Count);
                    rptdoc.SetParameterValue("frtamt", Convert.ToString(frtamt.ToString("2")));
                    rptdoc.SetParameterValue("othamt", Convert.ToString(othamt.ToString("2")));
                    rptdoc.SetParameterValue("totamt", Convert.ToString(frtamt + othamt.ToString("2")));
                    rptdoc.SetParameterValue("Currency", currID);
                    rptdoc.SetParameterValue("credit", "");
                    rptdoc.SetParameterValue("ShowAnnexure", "");

                    getReportControls(rptdoc, "QFOR3033");
                }
                else
                {
                    strMsg = "HBL,";
                    rptdoc = null;
                }
                return rptdoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public object HBLPRINT_BIAFA(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath, string strMsg, string PrePrinted = "")
        {
            cls_HBL_List ObjClsHBLprinting = new cls_HBL_List();
            WorkFlow objEmp = new WorkFlow();
            ReportDocument rptdoc = new ReportDocument();
            DataSet dsmain = null;
            DataSet dsdetails = null;
            DataSet dsPackage = null;
            DataSet dsDelAddr = null;
            DataSet dsConDet = new DataSet();
            DataSet dshbl = new DataSet();
            DataSet dsExtra = null;
            DataSet dsLoc = null;
            DataSet dsBlClauses = null;
            DataSet dsBlClausesNew = null;
            string NoOfPackages = "0";
            string ContainerDet = "";
            string strBlClauses = "";
            int I = 0;
            int j = 0;
            string BarCode = "";
            string HBKReturnValue = null;
            Int32 hblvalue = 0;
            string PODPK = null;
            string CommPK = null;
            string FormFlag = null;
            cls_HBL_Entry objHBLEntry = new cls_HBL_Entry();
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            Int32 HBLPk = default(Int32);
            clsQuotationReport objQuotReport = new clsQuotationReport();
            string @add = "";


            CommonFeatures objdef = new CommonFeatures();
            string strHBLTYPE = "No";
            try
            {
                HBKReturnValue = objEmp.ExecuteScaler("SELECT j.HBL_HAWB_FK HBL_EXP_TBL_FK FROM JOB_CARD_TRN j WHERE j.job_card_trn_pk = " + JOBPK);
                HBLPk = (!string.IsNullOrEmpty(getDefault(HBKReturnValue, "").ToString()) ? Convert.ToInt32(HBKReturnValue) : 0);
                if (HBLPk > 0)
                {
                    dsdetails = ObjClsHBLprinting.FetchHBLDetails(Convert.ToString(HBLPk));
                    if (objHBLEntry.GetBarcodeFlag("HBL") == "1" & ConfigurationSettings.AppSettings["ShowBarcode"] == "1")
                    {
                        BarCode = "*" + dsdetails.Tables[0].Rows[I]["hbl_ref_no"] + "*";
                    }

                    dsmain = ObjClsHBLprinting.FetchMainHBL(Convert.ToString(HBLPk),"" , Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                    if (dsmain.Tables[0].Rows.Count > 0)
                    {
                        if (string.IsNullOrEmpty(dsmain.Tables[0].Rows[0]["HBLDATE"].ToString()))
                        {
                            strMsg = "Actual departure date is not available for HBL Printing,";
                            rptdoc = null;
                            return rptdoc;
                        }
                    }

                    dsmain = null;
                    string From = "";
                    if (strHBLTYPE == "Yes")
                    {
                        dsmain = ObjClsHBLprinting.FetchMainHBL(Convert.ToString(HBLPk), From, Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                    }
                    else if (strHBLTYPE == "No")
                    {
                        dsmain = ObjClsHBLprinting.FetchHBLBIFA(Convert.ToString(HBLPk));
                    }

                    dsmain = ObjClsHBLprinting.FetchMainHBL(Convert.ToString(HBLPk), "", Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                    if (dsmain.Tables[0].Rows.Count > 0)
                    {
                        if (string.IsNullOrEmpty(dsmain.Tables[0].Rows[0]["HBLDATE"].ToString()))
                        {
                            strMsg = "Actual departure date is not available for HBL Printing,";
                            rptdoc = null;
                            return rptdoc;
                        }
                    }

                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["SHIPPERADD"], " ")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["SHIPPERADD"] ;
                    }

                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["shadd2"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["shadd2"] ;
                    }

                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["shadd3"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["shadd3"] ;
                    }
                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["shcity"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["shcity"] ;
                    }

                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["shzip"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["shzip"];
                    }

                    dsmain.Tables[0].Rows[0]["SHIPPERADD"] = @add;

                    @add = "";

                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["CONSIGNEEADD"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["CONSIGNEEADD"] ;
                    }

                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["ADM_ADDRESS_2"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["ADM_ADDRESS_2"] ;
                    }

                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["ADM_ADDRESS_3"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["ADM_ADDRESS_3"] ;
                    }
                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["adm_city"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["adm_city"] ;
                    }

                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["adm_zip_code"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["adm_zip_code"];
                    }

                    dsmain.Tables[0].Rows[0]["CONSIGNEEADD"] = @add;

                    @add = "";

                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["NOTIFYADD"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["NOTIFYADD"] ;
                    }

                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["nadd2"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["nadd2"] ;
                    }

                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["nadd3"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["nadd3"] ;
                    }
                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["ncity"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["ncity"] ;
                    }

                    if (Convert.ToString(getDefault(dsmain.Tables[0].Rows[0]["nzip"], "")).Trim().Length > 0)
                    {
                        @add += dsmain.Tables[0].Rows[0]["nzip"];
                    }
                    dsmain.Tables[0].Rows[0]["NOTIFYADD"] = @add;
                    dsPackage = ObjClsHBLprinting.FetchPackages(Convert.ToString(HBLPk));
                    for (I = 0; I <= dsPackage.Tables[0].Rows.Count - 1; I++)
                    {
                        if (I == 0)
                        {
                            NoOfPackages = Convert.ToString(dsPackage.Tables[0].Rows[I]["pack"]);
                        }
                        else
                        {
                            NoOfPackages = NoOfPackages + "," + dsPackage.Tables[0].Rows[I]["pack"];
                        }
                    }

                    if (NoOfPackages.Length <= 0)
                    {
                        NoOfPackages = "";
                    }

                    dsBlClauses = ObjClsHBLprinting.FetchBlClauses(Convert.ToString(HBLPk));
                    dsBlClauses.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\BlClausesForHBL.XSD");
                    for (I = 0; I <= dsBlClauses.Tables[0].Rows.Count - 1; I++)
                    {
                        if (I == 0)
                        {
                            strBlClauses = dsBlClauses.Tables[0].Rows[I]["BL_DESCRIPTION"].ToString();
                        }
                        else
                        {
                            strBlClauses = strBlClauses + "," + dsBlClauses.Tables[0].Rows[I]["BL_DESCRIPTION"];
                        }
                    }
                    if (strHBLTYPE == "Yes")
                    {
                        if (PrePrinted == "PrePrinted")
                        {
                            rptdoc.Load(HttpContext.Current.Server.MapPath("../07Reports") + "\\rptHBLPrinting_FIATA_New.rpt");
                        }
                        else
                        {
                            rptdoc.Load(HttpContext.Current.Server.MapPath("../07Reports") + "\\rptHBLPrinting_FIATA.rpt");
                        }
                    }
                    else if (strHBLTYPE == "No")
                    {
                        if (PrePrinted == "PrePrinted")
                        {
                            rptdoc.Load(HttpContext.Current.Server.MapPath("../07Reports") + "\\rptHBLPrinting_New.rpt");
                        }
                        else
                        {
                            rptdoc.Load(HttpContext.Current.Server.MapPath("../07Reports") + "\\rptHBLPrinting.rpt");
                        }
                    }

                    rptdoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                    dsmain.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\HBLMAIN.XSD");

                    dsBlClausesNew = objClsBlClause.FetchBlClausesForHBL("", 4, 1, 1, ((PODPK == null) ? "0" : PODPK), ((CommPK == null) ? "0" : CommPK), HBLPk, "", ((FormFlag == null) ? "" : FormFlag));

                    dsBlClausesNew.ReadXmlSchema(HttpContext.Current.Server.MapPath("../Documentation") + "\\PrintBLClause.XSD");
                    DataSet dsfright = new DataSet();
                    dsfright = objHBLEntry.getfrightdtls("JOBREF");
                    dsfright.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\frightdtls.XSD");
                    if (strHBLTYPE == "Yes")
                    {
                        dsdetails = ObjClsHBLprinting.FetchHBLDetails(Convert.ToString(HBLPk));
                        dsConDet = ObjClsHBLprinting.Get_ConDet(Convert.ToString(HBLPk));
                        dsConDet.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\CONDET.XSD");
                        //End
                    }
                    else if (strHBLTYPE == "No")
                    {
                        dsdetails = ObjClsHBLprinting.FetchHBLBIFADetails(Convert.ToString(HBLPk));
                    }

                    dsdetails.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\HBLDET.XSD");

                    for (I = 0; I <= dsdetails.Tables[0].Rows.Count - 1; I++)
                    {
                        if (string.IsNullOrEmpty(dsdetails.Tables[0].Rows[I]["SEAL_NUMBER"].ToString()))
                        {
                            dsdetails.Tables[0].Rows[I]["SEAL_NUMBER"] = "";
                        }
                        else if (string.IsNullOrEmpty(dsdetails.Tables[0].Rows[I]["CONTAINER_NUMBER"].ToString()))
                        {
                            dsdetails.Tables[0].Rows[I]["CONTAINER_NUMBER"] = "";
                        }

                        if (I == 0)
                        {
                            ContainerDet = dsdetails.Tables[0].Rows[I]["CONTAINER_NUMBER"].ToString() + dsdetails.Tables[0].Rows[I]["CONTAINER_TYPE_MST_ID"] + dsdetails.Tables[0].Rows[I]["SEAL_NUMBER"] + dsdetails.Tables[0].Rows[I]["PACK_COUNT"] + dsdetails.Tables[0].Rows[I]["GROSS_WEIGHT"] + dsdetails.Tables[0].Rows[I]["TAREWEIGHT"] + dsdetails.Tables[0].Rows[I]["Volume"];
                        }
                        else
                        {
                            ContainerDet = ContainerDet + "," + dsdetails.Tables[0].Rows[I]["CONTAINER_NUMBER"].ToString() + dsdetails.Tables[0].Rows[I]["CONTAINER_TYPE_MST_ID"] + dsdetails.Tables[0].Rows[I]["SEAL_NUMBER"] + dsdetails.Tables[0].Rows[I]["PACK_COUNT"] + dsdetails.Tables[0].Rows[I]["GROSS_WEIGHT"] + dsdetails.Tables[0].Rows[I]["TAREWEIGHT"] + dsdetails.Tables[0].Rows[I]["Volume"];
                        }
                    }
                    dsDelAddr = ObjClsHBLprinting.FetchDelAddress(Convert.ToString(HBLPk));

                    dsDelAddr.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\HBLDelAddr.xsd");

                    dsExtra = ObjClsHBLprinting.FetchDescExtra(Convert.ToString(HBLPk));
                    dsExtra.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\HBLDescExtra.xsd");

                    dsLoc = ObjClsTransportNote.FetchLocation(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                    dsLoc.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\TN_Location.xsd");


                    rptdoc.OpenSubreport("rptHBLMain").SetDataSource(dsmain);
                    rptdoc.OpenSubreport("rptHBLDelAddr").SetDataSource(dsDelAddr);
                    if (strHBLTYPE == "Yes")
                    {
                        rptdoc.OpenSubreport("rptConDetails").SetDataSource(dsConDet);
                    }
                    rptdoc.OpenSubreport("rptDescExtra").SetDataSource(dsExtra);
                    rptdoc.OpenSubreport("BLClause").SetDataSource(dsBlClauses);
                    rptdoc.OpenSubreport("rptBLClause").SetDataSource(dsBlClausesNew);

                    rptdoc.OpenSubreport("frightdtls").SetDataSource(dsfright);

                    string jobrefno = objHBLEntry.fetchjob(Convert.ToString(JOBPK));




                    rptdoc.SetDataSource(dsdetails);
                    rptdoc.SetParameterValue("fright", dsfright.Tables[0].Rows.Count);
                    rptdoc.SetParameterValue("credit", ObjClsHBLprinting.FetchCredit(Convert.ToString(HBLPk)));
                    rptdoc.SetParameterValue(0, NoOfPackages.Trim());
                    rptdoc.SetParameterValue(1, ContainerDet);
                    if (dsmain.Tables[0].Rows.Count > 0)
                    {
                        if (string.IsNullOrEmpty(dsmain.Tables[0].Rows[0]["PLACE_ISSUE"].ToString()) == true)
                        {
                            rptdoc.SetParameterValue(2, "");
                        }
                        else
                        {
                            rptdoc.SetParameterValue(2, dsmain.Tables[0].Rows[0]["PLACE_ISSUE"]);
                        }
                    }
                    DataSet dsLoadt = new DataSet();
                    dsLoadt = ObjClsHBLprinting.FetchLoadDate(Convert.ToString(HBLPk));
                    if (dsmain.Tables[0].Rows.Count > 0)
                    {
                        if (string.IsNullOrEmpty(dsmain.Tables[0].Rows[0]["VSLVOY"].ToString()) == true)
                        {
                            rptdoc.SetParameterValue(3, "");
                        }
                        else
                        {
                            rptdoc.SetParameterValue(3, dsmain.Tables[0].Rows[0]["VSLVOY"]);
                        }

                        if (string.IsNullOrEmpty(dsmain.Tables[0].Rows[0]["POL"].ToString()) == true)
                        {
                            rptdoc.SetParameterValue(6, "");
                        }
                        else
                        {
                            rptdoc.SetParameterValue(6, dsmain.Tables[0].Rows[0]["POL"]);
                        }

                        if (strHBLTYPE == "Yes")
                        {
                            if (string.IsNullOrEmpty(dsmain.Tables[0].Rows[0]["POD"].ToString()) == true)
                            {
                                rptdoc.SetParameterValue(12, "");
                            }
                            else
                            {
                                rptdoc.SetParameterValue(12, dsmain.Tables[0].Rows[0]["POD"]);
                            }
                        }
                        if (string.IsNullOrEmpty(dsmain.Tables[0].Rows[0]["HBLDATE"].ToString()) == true)
                        {
                            rptdoc.SetParameterValue(7, "");
                        }
                        else
                        {
                            rptdoc.SetParameterValue(7, Convert.ToDateTime(dsmain.Tables[0].Rows[0]["HBLDATE"]).ToString(dateFormat));
                        }
                    }

                    rptdoc.SetParameterValue(8, ObjClsHBLprinting.GetBarcodeFlag("HBL"));
                    rptdoc.SetParameterValue(9, BarCode);
                    rptdoc.SetParameterValue(10, dsmain.Tables[0].Rows[0]["status"]);

                    rptdoc.SetParameterValue(4, dsLoc.Tables[0].Rows[0]["ADDRESS_LINE1"]);
                    if (string.IsNullOrEmpty(dsmain.Tables[0].Rows[0]["HBLDATE"].ToString()) == true)
                    {
                        rptdoc.SetParameterValue(5, "");
                    }
                    else
                    {
                        rptdoc.SetParameterValue(5, Convert.ToDateTime(dsmain.Tables[0].Rows[0]["HBLDATE"]).ToString(dateFormat));
                    }
                    rptdoc.SetParameterValue("credit", "");
                    if (strHBLTYPE == "Yes")
                    {
                        getReportControls(rptdoc, "QFOR3033");
                    }
                    else if (strHBLTYPE == "No")
                    {
                        getReportControls(rptdoc, "QFOR3033", 1);
                    }

                    return rptdoc;
                }
                else
                {
                    strMsg = "BIAFA not generated";
                    rptdoc = null;
                    return rptdoc;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private DataSet HBLDts(Int32 HBLPk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            try
            {
                Strsql = "   SELECT decode(HBL.PYMT_TYPE,1,'PrePaid',2,'Collect') Ptype,decode(BKG.CARGO_TYPE,1,'FCL',2,'LCL')Cargo,HBL.CARGO_MOVE,HBL.IS_TO_ORDER ";
                Strsql += "  FROM HBL_EXP_TBL HBL,JOB_CARD_TRN JOB,BOOKING_MST_TBL BKG WHERE HBL.HBL_EXP_TBL_PK=" + HBLPk;
                Strsql += "  AND JOB.HBL_HAWB_FK=HBL.HBL_EXP_TBL_PK AND JOB.BOOKING_MST_FK=BKG.BOOKING_MST_PK  ";
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "MBL"
        public object MBL(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath, string User_Name, string From = "", string PrePrinted = "")
        {
            object functionReturnValue = null;
            ReportDocument repDoc = new ReportDocument();
            DataSet MainDS = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet CntDS = new DataSet();
            DataSet dsBlClauses = null;
            string cntno = null;
            string cnttype = null;
            string SealNo = null;
            int MBLPK = 0;
            int i = 0;
            string MBLReturnValue = null;
            string barcode = "";
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            cls_MBL_Entry objMBLEntry = new cls_MBL_Entry();
            WorkFlow objEmp = new WorkFlow();
            try
            {
                if (From == "MJC")
                {
                    MBLReturnValue = objEmp.ExecuteScaler("SELECT MJ.MBL_FK FROM MASTER_JC_SEA_EXP_TBL MJ WHERE MJ.MASTER_JC_SEA_EXP_PK = " + JOBPK);
                }
                else
                {
                    MBLReturnValue = objEmp.ExecuteScaler("SELECT j.mbl_mawb_fk FROM job_card_trn j WHERE j.job_card_trn_pk = " + JOBPK);
                }

                MBLPK = (!string.IsNullOrEmpty(MBLReturnValue) ? Convert.ToInt32(MBLReturnValue) : 0);
                objEmp = null;

                if (MBLPK > 0)
                {
                    AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                    if (PrePrinted == "PrePrinted")
                    {
                        repDoc.Load(mapPath + "\\rptMBLInstruction_New.rpt");
                    }
                    else
                    {
                        repDoc.Load(mapPath + "\\rptMBLInstruction.rpt");
                    }
                    repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                    MainDS = objMBLEntry.FetchMblInstructionData(MBLPK);
                    if (MainDS.Tables[0].Rows.Count <= 0)
                    {
                        return functionReturnValue;
                    }
                    CntDS = objMBLEntry.FetchMblCntDetailsData(MBLPK);
                    for (i = 0; i <= CntDS.Tables[0].Rows.Count - 1; i++)
                    {
                        if (!string.IsNullOrEmpty(CntDS.Tables[0].Rows[i]["CNTNUMBER"].ToString()))
                        {
                            cntno = cntno + CntDS.Tables[0].Rows[i]["CNTNUMBER"] + "/";

                        }
                        if (!string.IsNullOrEmpty(CntDS.Tables[0].Rows[i]["SEALNUMBER"].ToString()))
                        {
                            SealNo = SealNo + CntDS.Tables[0].Rows[i]["SEALNUMBER"] + "/";
                        }
                        if (!string.IsNullOrEmpty(CntDS.Tables[0].Rows[i]["CNTYPE"].ToString()))
                        {
                            cnttype = cnttype + CntDS.Tables[0].Rows[i]["CNTYPE"] + "/";
                        }
                    }
                    CntDS = objMBLEntry.FetchMblCntDetailsData(MBLPK);
                    CntDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../Documentation") + "\\Container_Details.xsd");
                    repDoc.OpenSubreport("MBLCnt").SetDataSource(CntDS);
                    dsBlClauses = objMBLEntry.FetchBlClauses(MBLPK);
                    dsBlClauses.ReadXmlSchema(HttpContext.Current.Server.MapPath("../Documentation") + "\\BLClause.xsd");
                    DataSet dsLogLoc_Address = new DataSet();
                    dsLogLoc_Address = objMBLEntry.FetchLogAddressDtl(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                    dsLogLoc_Address.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\LogLoc_Address.xsd");

                    AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\TN_Location.xsd");
                    repDoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);
                    repDoc.OpenSubreport("LogLoc_Address").SetDataSource(dsLogLoc_Address);

                    dsBlClauses.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\BlClausesForHBL.XSD");
                    repDoc.OpenSubreport("rptBLClause").SetDataSource(dsBlClauses);

                    AddressDS.ReadXmlSchema(rptPath + "\\TN_Location.xsd");
                    repDoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);
                    MainDS.ReadXmlSchema(mapPath + "\\MBLInstruction.xsd");
                    repDoc.SetDataSource(MainDS);

                    if ((cntno == null))
                    {
                        cntno = "";
                    }
                    else if ((cntno.LastIndexOf("/") != -1))
                    {
                        cntno = cntno.Remove(cntno.LastIndexOf("/"), 1);
                    }
                    if ((SealNo == null))
                    {
                        SealNo = "";
                    }
                    else if ((SealNo.LastIndexOf("/") != -1))
                    {
                        SealNo = SealNo.Remove(SealNo.LastIndexOf("/"), 1);
                    }
                    if (ConfigurationSettings.AppSettings["ShowBarcode"] == "1")
                    {
                        if (!string.IsNullOrEmpty(MainDS.Tables[0].Rows[0]["MBLNO"].ToString()))
                        {
                            barcode = "*" + Convert.ToString(MainDS.Tables[0].Rows[0]["MBLNO"]) + "*";
                        }
                    }

                    repDoc.SetParameterValue(0, User_Name);
                    repDoc.SetParameterValue(1, cntno);
                    repDoc.SetParameterValue(2, SealNo);
                    repDoc.SetParameterValue(3, cnttype);
                    repDoc.SetParameterValue(4, barcode.ToUpper());

                    repDoc.SetParameterValue(0, HttpContext.Current.Session["USER_NAME"]);
                    repDoc.SetParameterValue(1, cntno);
                    repDoc.SetParameterValue(2, SealNo);
                    repDoc.SetParameterValue(3, cnttype);
                    repDoc.SetParameterValue(4, barcode.ToUpper());
                    repDoc.SetParameterValue("BLval", (dsBlClauses.Tables[0].Rows.Count > 0 ? "Y" : "N"));
                    getReportControls(repDoc, "QFOR3052");
                }
                else
                {
                    repDoc = null;
                }
                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }
        #endregion

        #region "HAWBNew"
        public object HAWBNew(long HAWBPk, long logged_in_loc_fk, string mapPath, string rptPath, string PODPK = "0", string Fromform = "", string Standard = "", string Rated = "", int NrofCopies = 0, string PLACE_ISSUE = "")
        {
            ReportDocument rptDoc = new ReportDocument();
            DataSet dsHAWBMain = null;
            DataSet dsHAWBMarks = null;
            DataSet dsHAWBFreight = null;
            DataSet AddressDS = new DataSet();
            DataSet DSPalatte = new DataSet();
            DataSet dsClause = new DataSet();
            string strRemarks = null;
            clsHAWBentry objHAWBentry = new clsHAWBentry();
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            WorkFlow objEmp = new WorkFlow();
            string HAWBReturnValue = null;
            string barcode = "";
            string hawb_ref_no = "";
            string Palette = null;
            string CommPK = null;
            string FormFlag = null;
            int RowCnt = 0;
            CommPK = "0";
            FormFlag = "Air";
            if (Fromform == "PrintManager")
            {
                HAWBReturnValue = objEmp.ExecuteScaler("SELECT j.HBL_HAWB_FK FROM JOB_CARD_TRN j WHERE j.JOB_CARD_TRN_PK = " + HAWBPk);
                HAWBPk = (!string.IsNullOrEmpty(HAWBReturnValue) ? Convert.ToInt64(HAWBReturnValue) : 0);
            }
            //'Gangadhar, --This HAWB Print function  is calling  for whole Aplication commonly, 
            //' It is calling from HAWB, Print Manager, Track N Trace and Multiple Tracking
            //' if u want to cahnge any Parameters in Repot, have to Work on Both the reports Below 
            if (HAWBPk > 0)
            {
                if (Standard == "Standard")
                {
                    rptDoc.Load(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\rptHAWBStandard.rpt");
                }
                else
                {
                    rptDoc.Load(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\rptHAWBPreprinted.rpt");
                }
                dsHAWBMain = (DataSet)objHAWBentry.HAWB_MainPrint(Convert.ToString(HAWBPk), Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                dsHAWBMain.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\HAWB_Standard.XSD");

                //'Logo
                rptDoc.OpenSubreport("rptsubinvoiceimage").SetDataSource(ds_image());

                AddressDS = ObjClsTransportNote.FetchLocation(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\TN_Location.xsd");
                rptDoc.OpenSubreport("rptAddressDetails").SetDataSource(AddressDS);

                if (objHAWBentry.GetBarcodeFlag("HAWB EXPORTS") == "1" & ConfigurationManager.AppSettings["ShowBarcode"] == "1")
                {
                    barcode = "*" + Convert.ToString(dsHAWBMain.Tables[0].Rows[0]["HAWB_REF_NO"]) + "*";
                }

                dsHAWBFreight = (DataSet)objHAWBentry.HAWB_FREIGHT(Convert.ToString(HAWBPk));
                dsHAWBFreight.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\HAWB_Freight.XSD");
                DSPalatte = (DataSet)objHAWBentry.HAWB_PALETTE(Convert.ToString(HAWBPk));
                Palette = "";
                foreach (DataTable tb in DSPalatte.Tables)
                {
                    if (tb.Rows.Count > 0)
                    {
                        for (RowCnt = 0; RowCnt <= tb.Rows.Count - 1; RowCnt++)
                        {
                            if (string.IsNullOrEmpty(Palette))
                            {
                                Palette = (string.IsNullOrEmpty(tb.Rows[RowCnt]["PALETTE_SIZE"].ToString()) ? "" : tb.Rows[RowCnt]["PALETTE_SIZE"].ToString());
                            }
                            else
                            {
                                Palette += ", " + tb.Rows[RowCnt]["PALETTE_SIZE"];
                            }
                        }
                    }
                }


                if (Fromform != "HAWB")
                {
                    for (RowCnt = 0; RowCnt <= dsHAWBMain.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        if (RowCnt == 0)
                        {
                            if (!string.IsNullOrEmpty(dsHAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POL_FK"].ToString()) & (!string.IsNullOrEmpty(dsHAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POD_FK"].ToString())))
                            {
                                PODPK += dsHAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POL_FK"] + "," + dsHAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POD_FK"];
                            }
                        }
                        else
                        {
                            if (!string.IsNullOrEmpty(dsHAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POL_FK"].ToString()))
                            {
                                PODPK += dsHAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POL_FK"];
                            }
                            if (!string.IsNullOrEmpty(dsHAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POD_FK"].ToString()))
                            {
                                PODPK += dsHAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POD_FK"];
                            }
                        }
                    }
                }
                dsClause = objClsBlClause.FetchBlClausesForHBL("", 4, 1, 1, ((PODPK == null) ? "0" : PODPK), ((CommPK == null) ? "0" : CommPK), HAWBPk, DateTime.Now.Date.ToString(), ((FormFlag == null) ? "" : FormFlag));
                dsClause.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\PrintBLClause.XSD");
                rptDoc.OpenSubreport("rptBLClause").SetDataSource(dsClause);
                rptDoc.OpenSubreport("rptHAWBFreight").SetDataSource(dsHAWBFreight);
                rptDoc.SetDataSource(dsHAWBMain);
                rptDoc.SetParameterValue(0, barcode);
                rptDoc.SetParameterValue("Palette", Palette);
                rptDoc.SetParameterValue("Rated", Rated);
                rptDoc.SetParameterValue("NoOfBl", NrofCopies);
                rptDoc.SetParameterValue("LocId", PLACE_ISSUE);
                objrep.getReportControls(rptDoc, "QFOR3039");
                return rptDoc;
            }
            else
            {
                rptDoc = null;
            }
            return rptDoc = null;
        }
        #endregion

        #region "MAWBNew"
        public object MAWBNew(long MAWBPk, long logged_in_loc_fk, string mapPath, string rptPath, string PODPK = "0", string Fromform = "", string Standard = "", string Rated = "", int NrofCopies = 0)
        {
            ReportDocument rptDoc = new ReportDocument();
            DataSet dsMAWBMain = null;
            DataSet dsMAWBMarks = null;
            DataSet dsMAWBFreight = null;
            DataSet AddressDS = new DataSet();
            DataSet DSPalatte = new DataSet();
            DataSet dsClause = new DataSet();
            string strRemarks = null;
            cls_MAWBEntry objMAWBentry = new cls_MAWBEntry();
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            WorkFlow objEmp = new WorkFlow();
            string MAWBReturnValue = null;
            string barcode = "";
            string hawb_ref_no = "";
            string CommPK = null;
            string FormFlag = null;
            string Palette = null;
            int RowCnt = 0;
            CommPK = "0";
            FormFlag = "Air";
            if (Fromform == "PrintManager")
            {
                MAWBReturnValue = objEmp.ExecuteScaler("SELECT j.MBL_MAWB_FK FROM JOB_CARD_TRN j WHERE j.JOB_CARD_TRN_PK = " + MAWBPk);
                MAWBPk = (!string.IsNullOrEmpty(MAWBReturnValue) ? Convert.ToInt64(MAWBReturnValue) : 0);
            }
            //'Gangadhar, --This MAWB Print function  is calling  for whole Aplication commonly, 
            //' It is calling from HAWB, Print Manager, Track N Trace and Multiple Tracking
            //' if u want to cahnge any Parameters in Repot, has to Work on Both the reports Below 
            if (MAWBPk > 0)
            {
                if (Standard == "Standard")
                {
                    rptDoc.Load(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\rptMAWBStandard.rpt");
                }
                else if (Standard == "UPSFormat")
                {
                    rptDoc.Load(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\rptMAWBStandardNew.rpt");
                }
                else
                {
                    rptDoc.Load(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\rptMAWBPrePrintedNew.rpt");
                }
                dsMAWBMain = (DataSet)objMAWBentry.MAWB_MainPrint(Convert.ToString(MAWBPk), Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                dsMAWBMain.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\MAWB_Standard.XSD");

                //'Logo
                rptDoc.OpenSubreport("rptsubinvoiceimage").SetDataSource(ds_image());

                AddressDS = ObjClsTransportNote.FetchLocation(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\TN_Location.xsd");
                rptDoc.OpenSubreport("rptAddressDetails").SetDataSource(AddressDS);

                /// For Barcode
                if (objMAWBentry.GetBarcodeFlag("HAWB EXPORTS") == "1" & ConfigurationSettings.AppSettings["ShowBarcode"] == "1")
                {
                    if (string.IsNullOrEmpty(dsMAWBMain.Tables[0].Rows[0]["MAWB_REF_NO"].ToString()))
                    {
                        barcode = " ";
                    }
                    else
                    {
                        barcode = "*" + dsMAWBMain.Tables[0].Rows[0]["MAWB_REF_NO"] + "*";
                    }

                }
                dsMAWBFreight = (DataSet)objMAWBentry.MAWB_FREIGHT(Convert.ToString(MAWBPk));
                dsMAWBFreight.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\MAWB_Freight.XSD");
                DSPalatte = (DataSet)objMAWBentry.MAWB_PALETTE(Convert.ToString(MAWBPk));
                Palette = "";
                foreach (DataTable tb in DSPalatte.Tables)
                {
                    if (tb.Rows.Count > 0)
                    {
                        for (RowCnt = 0; RowCnt <= tb.Rows.Count - 1; RowCnt++)
                        {
                            if (string.IsNullOrEmpty(Palette))
                            {
                                Palette = (string.IsNullOrEmpty(tb.Rows[RowCnt]["PALETTE_SIZE"].ToString()) ? "" : tb.Rows[RowCnt]["PALETTE_SIZE"].ToString());
                            }
                            else
                            {
                                Palette += ", " + tb.Rows[RowCnt]["PALETTE_SIZE"];
                            }
                        }
                    }
                }
                if (Fromform != "MAWB")
                {
                    for (RowCnt = 0; RowCnt <= dsMAWBMain.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        if (RowCnt == 0)
                        {
                            if (!string.IsNullOrEmpty(dsMAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POL_FK"].ToString()) & (!string.IsNullOrEmpty(dsMAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POD_FK"].ToString())))
                            {
                                PODPK += dsMAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POL_FK"] + "," + dsMAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POD_FK"];
                            }
                        }
                        else
                        {
                            if (!string.IsNullOrEmpty(dsMAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POL_FK"].ToString()))
                            {
                                PODPK += dsMAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POL_FK"];
                            }
                            if (!string.IsNullOrEmpty(dsMAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POD_FK"].ToString()))
                            {
                                PODPK += dsMAWBMain.Tables[0].Rows[RowCnt]["PORT_MST_POD_FK"];
                            }
                        }
                    }
                }
                dsClause = objClsBlClause.FetchBlClausesForHBL("", 5, 1, 1, ((PODPK == null) ? "0" : PODPK), ((CommPK == null) ? "0" : CommPK), MAWBPk, DateTime.Now.Date.ToString(), ((FormFlag == null) ? "" : FormFlag));
                dsClause.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\PrintBLClause.XSD");
                rptDoc.OpenSubreport("rptBLClause").SetDataSource(dsClause);
                rptDoc.OpenSubreport("rptHAWBFreight").SetDataSource(dsMAWBFreight);
                rptDoc.SetDataSource(dsMAWBMain);


                rptDoc.SetParameterValue(0, barcode);
                rptDoc.SetParameterValue("Palette", Palette);
                rptDoc.SetParameterValue("Rated", Rated);
                rptDoc.SetParameterValue("NoOfBl", NrofCopies);
                rptDoc.SetParameterValue("LocId", (string.IsNullOrEmpty(AddressDS.Tables[0].Rows[0]["LOCATION_NAME"].ToString()) ? "" : AddressDS.Tables[0].Rows[0]["LOCATION_NAME"]));
                objrep.getReportControls(rptDoc, "QFOR3060", 2);
                return rptDoc;
            }
            else
            {
                rptDoc = null;
            }
            return rptDoc = null;
        }
        #endregion

        #region "HAWB"
        public object HAWB(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath, string User_Name, string HAWBRate = "0")
        {
            ReportDocument rptDoc = new ReportDocument();
            DataSet dsHAWBMain = null;
            DataSet dsHAWBFreight = null;
            Int32 HAWBPk = default(Int32);
            string strRemarks = null;
            clsHAWBentry objHAWBentry = new clsHAWBentry();
            WorkFlow objEmp = new WorkFlow();
            string HAWBReturnValue = null;
            //Dim repDocUsr As New ReportDocument
            //Dim Str As String       'Added By AREEF
            //Dim im As New DataSet
            string barcode = "";
            string hawb_ref_no = "";

            try
            {
                //Str = ""
                //Str = Session("ImageFile")
                //Str = HttpContext.Current.Server.MapPath("..\..") & "\Supports\" & Str
                //im = ImageTable(Str)
                //Str = "rptsubinvoiceimage.rpt"
                //im.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\07Reports") & "\ckim.xsd")  'End of Areef Statements
                HAWBReturnValue = objEmp.ExecuteScaler("SELECT j.hbl_hawb_fk FROM job_card_trn j WHERE j.job_card_trn_pk = " + JOBPK);
                HAWBPk = (!string.IsNullOrEmpty(HAWBReturnValue) ? Convert.ToInt32(HAWBReturnValue) : 0);

                objEmp = null;
                if (HAWBPk > 0)
                {
                    rptDoc.Load(mapPath + "\\rptHAWB1.rpt");
                    //Modified By Manoharan on 19Sep2007
                    //rptDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image())
                    //repDocUsr = rptDoc.OpenSubreport(Str)
                    //repDocUsr.Database.Tables(0).SetDataSource(im)

                    dsHAWBMain = objHAWBentry.HAWB_Print(Convert.ToString(HAWBPk), logged_in_loc_fk, HAWBRate);
                    if (objHAWBentry.GetBarcodeFlag("HAWB EXPORTS") == "1" & ConfigurationManager.AppSettings["ShowBarcode"] == "1")
                    {
                        barcode = "*" + Convert.ToString(dsHAWBMain.Tables[0].Rows[0]["HAWB_REF"]) + "*";
                    }
                    dsHAWBMain.ReadXmlSchema(mapPath + "\\HAWB_MAIN_RPT.XSD");

                    strRemarks = dsHAWBMain.Tables[0].Rows[0]["JOBCARD_REF_NO"].ToString();
                    strRemarks = objHAWBentry.Job_card_Remarks(strRemarks);
                    dsHAWBFreight = objHAWBentry.HAWB_FRT(Convert.ToString(HAWBPk), HAWBRate);
                    dsHAWBFreight.ReadXmlSchema(mapPath + "\\HAWB_SUB_RPT.XSD");
                    rptDoc.SetDataSource(dsHAWBMain);
                    rptDoc.SetParameterValue(0, User_Name);
                    rptDoc.SetParameterValue(1, DateTime.Now.Date);
                    rptDoc.SetParameterValue(2, logged_in_loc_fk);
                    rptDoc.SetParameterValue(3, strRemarks);
                    rptDoc.SetParameterValue(4, barcode);
                    //'surya23Nov2006
                    rptDoc.OpenSubreport("rptHAWB_SUB.rpt").SetDataSource(dsHAWBFreight);

                }
                else
                {
                    rptDoc = null;
                }

                return rptDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        #endregion

        #region "MAWB"
        public object MAWB(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath, string User_Name)
        {
            ReportDocument repDoc = new ReportDocument();
            DataSet dsMAWBMain = null;
            DataSet dsMAWBFlight = null;
            DataSet dsMAWBTp = null;
            string tpport1 = "";
            string tpairline1 = "";
            string tpport2 = "";
            string tpairline2 = "";
            Int32 MAWBPk = default(Int32);
            Int32 I = default(Int32);
            string flightno = null;
            string MAWBReturnValue = null;
            cls_MAWBEntry objMAWBentry = new cls_MAWBEntry();
            WorkFlow objEmp = new WorkFlow();
            string BARCODE = "";
            //Dim repDocUsr As New ReportDocument
            //Dim Str As String       'Added By AREEF
            //Dim im As New DataSet


            try
            {
                //Str = ""
                //Str = Session("ImageFile")
                //Str = HttpContext.Current.Server.MapPath("..\..") & "\Supports\" & Str
                //im = ImageTable(Str)
                //Str = "rptsubinvoiceimage.rpt"
                //im.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\07Reports") & "\ckim.xsd")  'End of Areef Statements
                MAWBReturnValue = objEmp.ExecuteScaler("SELECT j.mbl_MAWB_fk FROM job_card_trn j WHERE j.job_card_trn_pk = " + JOBPK);
                MAWBPk = (!string.IsNullOrEmpty(MAWBReturnValue) ? Convert.ToInt32(MAWBReturnValue) : 0);
                objEmp = null;
                if (MAWBPk > 0)
                {
                    DataSet dsMAWBFreight = new DataSet();
                    string strMAWBNo = null;
                    string[] strArray = null;
                    repDoc.Load(mapPath + "\\rptMAWB_MAIN.rpt");
                    //Modified By Manoharan on 19Sep2007
                    //commented by surya prasad as there is no subreport in .rpt file
                    //repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image())
                    //end

                    //repDocUsr = repDoc.OpenSubreport(Str)

                    //repDocUsr.Database.Tables(0).SetDataSource(im)
                    dsMAWBMain = objMAWBentry.MAWB_Print(MAWBPk, logged_in_loc_fk);
                    dsMAWBMain.ReadXmlSchema(mapPath + "\\MAWB_MAIN.XSD");

                    strMAWBNo = dsMAWBMain.Tables[0].Rows[0]["MAWB_REF"].ToString();
                    if (ConfigurationSettings.AppSettings["ShowBarcode"] == "1")
                    {
                        BARCODE = "*" + strMAWBNo.ToString() + "*";
                    }
                    strMAWBNo = strMAWBNo.Replace(" ", ",");
                    while (string.Compare(strMAWBNo, ",,") > 0)
                    {
                        strMAWBNo = strMAWBNo.Replace(",,", ",");
                    }
                    strArray = strMAWBNo.Split(',');
                    if (strArray.Length >= 3)
                    {
                        strMAWBNo = strArray[0] + "-" + strArray[2];
                    }
                    dsMAWBFreight = objMAWBentry.MAWB_FRT(MAWBPk);
                    dsMAWBFreight.ReadXmlSchema(mapPath + "\\MAWB_SUB.XSD");
                    repDoc.SetDataSource(dsMAWBMain);
                    repDoc.SetParameterValue(0, User_Name);
                    repDoc.SetParameterValue(1, DateTime.Now.Date);
                    repDoc.SetParameterValue(2, logged_in_loc_fk);
                    repDoc.SetParameterValue(3, strMAWBNo);
                    repDoc.SetParameterValue(4, BARCODE);
                    //'surya23Nov2006
                    repDoc.OpenSubreport("rptMAWB_SUB.rpt").SetDataSource(dsMAWBFreight);
                }
                else
                {
                    repDoc = null;
                }
                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }


        #endregion

        #region "Insurance Certificate"
        public object COI(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            ReportDocument rptDoc = new ReportDocument();
            cls_JobCardView ObjClsCertificateOfIns = new cls_JobCardView();
            DataSet dsCISMain = null;
            rptDoc.Load(rptPath + "\\rptCISExpSea.rpt");
            rptDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
            dsCISMain = ObjClsCertificateOfIns.FetchCISExpSea(Convert.ToInt32(JOBPK));
            dsCISMain.ReadXmlSchema(rptPath + "\\CISMAIN.xsd");
            rptDoc.SetDataSource(dsCISMain);
            return rptDoc;
        }
        #endregion

        #region "Acknowledgement Exp Sea"
        public object AcknowledgementExpSea(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            ReportDocument repDoc = new ReportDocument();
            DataSet MainRptDS = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet CntDS = new DataSet();
            int Type = 0;
            int i = 0;
            DataSet comm = new DataSet();
            DataSet GoodsDS = new DataSet();
            string CONTAINER = "";
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            cls_JobCardSearch objJobCardSea = new cls_JobCardSearch();
            Cls_BookingEntry objcomm = new Cls_BookingEntry();
            cls_MBL_Entry objMBLEntry = new cls_MBL_Entry();
            Int32 BookingPk = default(Int32);
            Int32 Bkg_Status = default(Int32);
            string PODPK = null;
            string CommPK = null;
            string FormFlag = null;
            string BkgDt = null;
            CommPK = "0";
            FormFlag = "Sea";
            try
            {
                AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                repDoc.Load(HttpContext.Current.Server.MapPath("../Documentation") + "\\Acknowledgement1.rpt");
                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                MainRptDS = objJobCardSea.FetchSeaAcknowledgement(Convert.ToString(JOBPK));
                CntDS = objJobCardSea.FetchSeaContainers(Convert.ToString(JOBPK));
                BookingPk = Convert.ToInt32(MainRptDS.Tables[0].Rows[0]["BKGPK"]);
                BkgDt = Convert.ToString(MainRptDS.Tables[0].Rows[0]["BKGDATE"]);
                DataSet dsstatus = new DataSet();
                dsstatus = FetchBookingStatus(Convert.ToString(BookingPk));
                Bkg_Status = Convert.ToInt32(dsstatus.Tables[0].Rows[0]["STATUS"]);
                comm = (DataSet)objcomm.FetchComm(BookingPk, Bkg_Status);

                for (i = 0; i <= CntDS.Tables[0].Rows.Count - 1; i++)
                {
                    if (!string.IsNullOrEmpty(CntDS.Tables[0].Rows[i]["CONTAINER"].ToString()))
                    {
                        CONTAINER = CONTAINER + CntDS.Tables[0].Rows[i]["CONTAINER"] + ",";
                    }
                }
                if ((CONTAINER.LastIndexOf(",") != -1))
                {
                    CONTAINER = CONTAINER.Remove(CONTAINER.LastIndexOf(","), 1);
                }
                MainRptDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../Documentation") + "\\Main_Acknowledgement.xsd");
                comm.ReadXmlSchema(HttpContext.Current.Server.MapPath("../06BookingExports") + "\\comm.xsd");

                DataSet dsLogLoc_Address = new DataSet();
                dsLogLoc_Address = objMBLEntry.FetchLogAddressDtl(Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                dsLogLoc_Address.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\LogLoc_Address.xsd");
                repDoc.OpenSubreport("LogLoc_Address").SetDataSource(dsLogLoc_Address);

                GoodsDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../06BookingExports") + "\\Goods.xsd");
                repDoc.OpenSubreport("goods").SetDataSource(GoodsDS);
                AddressDS.ReadXmlSchema(rptPath + "\\TN_Location.xsd");
                repDoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);
                repDoc.OpenSubreport("comm").SetDataSource(comm);

                DataSet GridDS = null;
                GridDS = objClsBlClause.FetchBlClausesForHBL("", 3, 1, 1, ((PODPK == null) ? "0" : PODPK), "0", BookingPk, BkgDt, FormFlag);
                GridDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\PrintBLClause.xsd");
                repDoc.OpenSubreport("rptBLClause").SetDataSource(GridDS);
                repDoc.SetDataSource(MainRptDS);

                repDoc.SetParameterValue("BusiType", 2);
                repDoc.SetParameterValue("Containers", CONTAINER);
                repDoc.SetParameterValue("AckType", 1);
                repDoc.SetParameterValue("UserName", HttpContext.Current.Session["USER_NAME"]);
                getReportControls(repDoc, "QFOR3050");
                return repDoc;
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        public object AcknowledgementSeaAir(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath, int BizType)
        {
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            cls_TrackAndTrace objTrTc = new cls_TrackAndTrace();
            CommonFeatures objrep = new CommonFeatures();
            ReportDocument repDoc = new ReportDocument();
            DataSet BkgDS = new DataSet();
            DataSet AddressDS = new DataSet();
            string Corpotate = null;
            string County = null;
            DataSet dsCorporate = null;
            Cls_BookingEntry objBkg = new Cls_BookingEntry();
            try
            {
                AddressDS = ObjClsTransportNote.FetchLocation(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                repDoc.Load(HttpContext.Current.Server.MapPath("../Documentation") + "\\BkgAcknowledge.rpt");

                BkgDS = FetchJCRptDetails(JOBPK);
                BkgDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../Documentation") + "\\BkgAcknowledge.xsd");

                AddressDS = ObjClsTransportNote.FetchLocation(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\TN_Location.xsd");

                repDoc.OpenSubreport("rptAddressDetails").SetDataSource(AddressDS);
                repDoc.OpenSubreport("rptsubinvoiceimage").SetDataSource(ds_image());

                repDoc.SetDataSource(BkgDS);
                dsCorporate = objBkg.GetCorporateDtls();
                Corpotate = dsCorporate.Tables[0].Rows[0]["CORPORATE_NAME"].ToString();
                County = dsCorporate.Tables[0].Rows[0]["COUNTRY_NAME"].ToString();
                repDoc.SetParameterValue("BizType", BizType);
                repDoc.SetParameterValue("UserName", HttpContext.Current.Session["USER_NAME"]);
                repDoc.SetParameterValue("Designation", getDefault(HttpContext.Current.Session["DESIGNATION"], ""));
                repDoc.SetParameterValue("Corporate_Name", Corpotate);
                repDoc.SetParameterValue("Country_Name", County);

                objrep.getReportControls(repDoc, "QFOR4448", 1);
                return repDoc;
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        #endregion

        #region "Acknowledgement Exp Air"
        public object AcknowledgementExpAir(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            ReportDocument repDoc = new ReportDocument();
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            DataSet MainDS = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet CntDS = new DataSet();
            int i = 0;
            clsJobCardAirExport objJobCardAir = new clsJobCardAirExport();
            string CONTAINER = null;
            DataSet GoodsDS = new DataSet();
            DataSet BLClauseDS = new DataSet();
            DataSet LogLoc = new DataSet();
            DataSet CommDS = new DataSet();

            try
            {
                AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                repDoc.Load(mapPath + "\\Acknowledgement1.rpt");
                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                MainDS = objJobCardAir.FetchAirAcknowledgement(Convert.ToString(JOBPK));
                MainDS.Tables[0].Columns.Add("Containers", typeof(string));
                CntDS = objJobCardAir.FetchSeaContainers(Convert.ToString(JOBPK));
                for (i = 0; i <= CntDS.Tables[0].Rows.Count - 1; i++)
                {
                    if (!string.IsNullOrEmpty(CntDS.Tables[0].Rows[i]["CONTAINER"].ToString()))
                    {
                        CONTAINER = CONTAINER + CntDS.Tables[0].Rows[i]["CONTAINER"] + ",";
                    }
                }
                if ((CONTAINER != null))
                {
                    if ((CONTAINER.LastIndexOf(",") != -1))
                    {
                        CONTAINER = CONTAINER.Remove(CONTAINER.LastIndexOf(","), 1);
                    }
                }
                MainDS.ReadXmlSchema(mapPath + "\\Main_Acknowledgement.xsd");

                AddressDS.ReadXmlSchema(rptPath + "\\TN_Location.xsd");
                repDoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);
                GoodsDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../06BookingExports") + "\\Goods.xsd");
                repDoc.OpenSubreport("goods").SetDataSource(GoodsDS);

                BLClauseDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../Documentation") + "\\PrintBLClause.xsd");
                repDoc.OpenSubreport("rptBLClause").SetDataSource(BLClauseDS);

                LogLoc.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\LogLoc_Address.xsd");
                repDoc.OpenSubreport("LogLoc_Address").SetDataSource(LogLoc);

                CommDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../06BookingExports") + "\\comm.xsd");
                repDoc.OpenSubreport("comm").SetDataSource(CommDS);

                repDoc.SetDataSource(MainDS);
                repDoc.SetParameterValue("BusiType", 1);
                repDoc.SetParameterValue("Containers", (CONTAINER == null ? "" : CONTAINER));
                repDoc.SetParameterValue("AckType", 2);
                repDoc.SetParameterValue("UserName", HttpContext.Current.Session["USER_NAME"]);
                getReportControls(repDoc, "QFOR3050");
                return repDoc;
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        #endregion
        //CoverLetterforSplitAir
        #region "Cover Letter for Split Sea"
        public object CoverLetterforSplitSea(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            ReportDocument Rep = new ReportDocument();
            DataTable dt = null;
            DataSet dsJob_BL = new DataSet();
            DataSet dsJob_Cont = new DataSet();
            DataSet dsLoc = new DataSet();
            Cls_SplitBL objBL = new Cls_SplitBL();
            string container = "";
            int R = 0;
            try
            {
                Rep.Load(mapPath + "\\rptSplitCargo.rpt");
                //Rep.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image())
                //dsLoc = (New Cls_Transport_Note).FetchLocation(logged_in_loc_fk)
                //dsLoc.ReadXmlSchema(mapPath & "\TN_Location.xsd")
                dsJob_BL = objBL.Job_Card_And_BL_Dtls(JOBPK);
                if (dsJob_BL.Tables[0].Rows.Count > 0)
                {
                    dsJob_BL.ReadXmlSchema(mapPath + "\\SplitCargo.xsd");
                    dsJob_Cont = objBL.Job_Card_Cont_Dtls(JOBPK);
                    dsJob_Cont.ReadXmlSchema(mapPath + "\\ContainerDetails.xsd");
                    for (R = 0; R <= dsJob_Cont.Tables[0].Rows.Count - 1; R++)
                    {
                        if (!string.IsNullOrEmpty(dsJob_Cont.Tables[0].Rows[R]["CONTAINER"].ToString()))
                        {
                            container = container + dsJob_Cont.Tables[0].Rows[R]["CONTAINER"] + ",";
                        }
                    }

                    if ((container.LastIndexOf(",") != -1))
                    {
                        container = container.Remove(container.LastIndexOf(","), 1);
                    }
                    double Weight = 0.0;
                    for (R = 0; R <= dsJob_Cont.Tables[0].Rows.Count - 1; R++)
                    {
                        if (!string.IsNullOrEmpty(dsJob_Cont.Tables[0].Rows[R]["VOLUME"].ToString()))
                        {
                            Weight = Weight + Convert.ToDouble(dsJob_Cont.Tables[0].Rows[R]["VOLUME"].ToString());
                        }
                    }
                    double Volume = 0;
                    for (R = 0; R <= dsJob_Cont.Tables[0].Rows.Count - 1; R++)
                    {
                        if (!string.IsNullOrEmpty(dsJob_Cont.Tables[0].Rows[R]["WEIGHT"].ToString()))
                        {
                            Volume = Volume + Convert.ToDouble(dsJob_Cont.Tables[0].Rows[R]["WEIGHT"].ToString());
                        }
                    }
                    int Pieces = 0;
                    for (R = 0; R <= dsJob_Cont.Tables[0].Rows.Count - 1; R++)
                    {
                        if (!string.IsNullOrEmpty(dsJob_Cont.Tables[0].Rows[R]["PIECES"].ToString()))
                        {
                            Pieces = Convert.ToInt32(Pieces + Convert.ToDouble(dsJob_Cont.Tables[0].Rows[R]["PIECES"].ToString()));
                        }
                    }
                    //Rep.OpenSubreport("rptLocation").SetDataSource(dsLoc)
                    //Rep.OpenSubreport("Consignor")
                    //Rep.OpenSubreport("Agent")
                    Rep.OpenSubreport("rptContainerDetails.rpt").SetDataSource(dsJob_Cont);
                    Rep.SetDataSource(dsJob_BL);

                    //Rep.SetParameterValue(0, container)
                    //Rep.SetParameterValue(1, Volume)
                    //Rep.SetParameterValue(2, Weight)
                    //Rep.SetParameterValue(3, Pieces)
                    //getReportControls(Rep, "QFOR4026")
                }
                else
                {
                    Rep = null;
                }
                return Rep;
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        #endregion

        #region "FMCBL"
        public object FMCBL(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            ReportDocument repDoc = new ReportDocument();
            DataSet dsMain = new DataSet();
            DataSet dsDetails = new DataSet();
            DataSet dsLoc = new DataSet();
            DataSet dsFreight = new DataSet();
            string ContainerNos = null;
            WorkFlow objEmp = new WorkFlow();
            Int32 HBLPk = default(Int32);
            string HBKReturnValue = null;
            Cls_FMCBL objClsFMC = new Cls_FMCBL();
            int I = 0;
            string strSql = null;
            //Dim repDocUsr As New ReportDocument
            //Dim Str As String       'Added By AREEF
            //Dim im As New DataSet
            try
            {
                //Str = ""
                //Str = Session("ImageFile")
                //Str = HttpContext.Current.Server.MapPath("..\..") & "\Supports\" & Str
                //im = ImageTable(Str)
                //Str = "rptsubinvoiceimage.rpt"
                //im.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\07Reports") & "\ckim.xsd")  'End of Areef Statements
                strSql = "SELECT J.HBL_HAWB_FK " + " FROM JOB_CARD_TRN J, PORT_MST_TBL PORT, BOOKING_MST_TBL BS " + " WHERE J.JOB_CARD_TRN_PK =" + JOBPK + " AND bs.PORT_MST_POD_FK = PORT.PORT_MST_PK " + " AND bs.booking_mst_pk=j.booking_mst_fk" + " AND PORT.PORT_ID LIKE 'US%'";
                HBKReturnValue = objEmp.ExecuteScaler(strSql);
                HBLPk = (!string.IsNullOrEmpty(HBKReturnValue) ? Convert.ToInt32(HBKReturnValue) : 0);
                if (HBLPk > 0)
                {
                    repDoc.Load(rptPath + "\\rptFMCBL.rpt");
                    //Modified By Manoharan on 19Sep2007
                    repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                    //repDocUsr = repDoc.OpenSubreport(Str)
                    //repDocUsr.Database.Tables(0).SetDataSource(im)
                    dsLoc = (new Cls_Transport_Note()).FetchLocation(logged_in_loc_fk);

                    dsMain = objClsFMC.FetchFMCMain(Convert.ToString(HBLPk));
                    dsMain.ReadXmlSchema(rptPath + "\\FMCMAIN.XSD");
                    dsDetails = objClsFMC.FetchFMCDetails(Convert.ToString(HBLPk));
                    dsDetails.ReadXmlSchema(rptPath + "\\FMCDETAILS.XSD");
                    dsFreight = objClsFMC.FetchFMCFreight(Convert.ToString(HBLPk));
                    dsFreight.ReadXmlSchema(rptPath + "\\FMCFREIGHT.XSD");

                    for (I = 0; I <= dsDetails.Tables[0].Rows.Count - 1; I++)
                    {
                        if (!string.IsNullOrEmpty(dsDetails.Tables[0].Rows[I]["CONTAINER_NUMBER"].ToString()))
                        {
                            ContainerNos += dsDetails.Tables[0].Rows[I]["CONTAINER_NUMBER"] + ",";
                            //Else
                            //    ContainerNos &= dsDetails.Tables(0).Rows(i).Item("CONTAINER_NUMBER") & ","
                        }
                    }
                    if (string.IsNullOrEmpty(ContainerNos))
                    {
                        ContainerNos = "";
                    }
                    else if (ContainerNos.LastIndexOf(",") != -1)
                    {
                        ContainerNos = ContainerNos.Remove(ContainerNos.LastIndexOf(","), 1);
                    }

                    repDoc.SetDataSource(dsDetails);
                    repDoc.SetParameterValue(0, ContainerNos);
                    if (dsLoc.Tables[0].Rows.Count > 0)
                    {
                        if (string.IsNullOrEmpty(dsLoc.Tables[0].Rows[0]["LOCATION_NAME"].ToString()) == true)
                        {
                            repDoc.SetParameterValue(1, "");
                        }
                        else
                        {
                            repDoc.SetParameterValue(1, dsLoc.Tables[0].Rows[0]["LOCATION_NAME"]);
                        }
                        if (string.IsNullOrEmpty(dsLoc.Tables[0].Rows[0]["ADDRESS_LINE1"].ToString()) == true)
                        {
                            repDoc.SetParameterValue(2, "");
                        }
                        else
                        {
                            repDoc.SetParameterValue(2, dsLoc.Tables[0].Rows[0]["ADDRESS_LINE1"]);
                        }
                    }
                    if (dsMain.Tables[0].Rows.Count > 0)
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables[0].Rows[0]["CORPORATE_NAME"].ToString()) == true)
                        {
                            repDoc.SetParameterValue(3, "");
                        }
                        else
                        {
                            repDoc.SetParameterValue(3, dsLoc.Tables[0].Rows[0]["CORPORATE_NAME"]);
                        }
                    }
                    repDoc.SetParameterValue(4, objEmp.MyUserName);

                    repDoc.OpenSubreport("FMCMain").SetDataSource(dsMain);
                    repDoc.OpenSubreport("FMCFreight").SetDataSource(dsFreight);

                    //adding by thiygarajan on 16/2/09
                    getReportControls(repDoc, "QFOR3058");
                }
                else
                {
                    repDoc = null;
                }
                return repDoc;
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        #endregion

        #region "export file cover sheet"
        public object EXPFileCsheet(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            ReportDocument repDoc = new ReportDocument();
            DataSet MainRptDS = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet FrtEleDS = new DataSet();
            DataSet DimenDS = new DataSet();
            int i = 0;
            int j = 0;
            int Type = 0;
            string HAWBReturnValue = null;
            string Dimensions = null;
            clsMAWBListing objMawb = new clsMAWBListing();
            Int32 HAWBPk = default(Int32);
            WorkFlow objEmp = new WorkFlow();
            try
            {
                HAWBReturnValue = objEmp.ExecuteScaler("SELECT j.hbl_hawb_fk FROM job_card_trn j WHERE j.job_card_trn_pk = " + JOBPK);
                HAWBPk = (!string.IsNullOrEmpty(HAWBReturnValue) ? Convert.ToInt32(HAWBReturnValue) : 0);
                objEmp = null;
                if (HAWBPk > 0)
                {
                    Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
                    repDoc.Load(rptPath + "\\FileCoverSheet.rpt");
                    repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                    AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                    MainRptDS = objMawb.FetchFileCoverReportDetails(Convert.ToString(HAWBPk));
                    FrtEleDS = objMawb.FetchFrtElementDetails(Convert.ToString(HAWBPk));
                    DimenDS = objMawb.FetchDimensionsDetails(Convert.ToString(HAWBPk));
                    repDoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);
                    MainRptDS.Tables[0].Columns.Add("Dimensions", typeof(string));
                    Array PkArr = null;
                    string strDimensions = "";

                    Int16 RowCnt = default(Int16);
                    Int16 pkCount = default(Int16);

                    for (RowCnt = 0; RowCnt <= MainRptDS.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        Int16 Dimension = default(Int16);
                        for (Dimension = 0; Dimension <= DimenDS.Tables[0].Rows.Count - 1; Dimension++)
                        {
                            strDimensions += Convert.ToString(getDefault(DimenDS.Tables[0].Rows[Dimension]["PALETTE_SIZE"], "")).Trim() + ",";
                        }
                        MainRptDS.Tables[0].Rows[RowCnt]["Dimensions"] = strDimensions.TrimEnd(',');
                        strDimensions = "";
                        break; // TODO: might not be correct. Was : Exit For
                    }
                    repDoc.SetDataSource(MainRptDS);
                    getReportControls(repDoc, "QFOR3057");
                }
                else
                {
                    repDoc = null;
                }
                return repDoc;
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        #endregion

        #region "Cargo manifest"
        public object CargoManifestSea(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath, Int16 BusinessType, string From = "", int Process = 1, string JOBTrnPK = "0")
        {
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            ReportDocument repDoc = new ReportDocument();
            DataSet MainRptDS = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet CntDS = new DataSet();
            DataSet ContDts = new DataSet();
            DataSet HBLCOUNT = new DataSet();
            DataSet DimenDS = new DataSet();
            int i = 0;
            int Type = 0;
            Int32 MBLPK = default(Int32);
            string MBLReturnValue = null;
            WorkFlow objEmp = new WorkFlow();
            Cls_SeaCargoManifest objCargo = new Cls_SeaCargoManifest();
            string Headers = null;
            DataSet dsHdr = new DataSet();
            string[] PKHDr = null;
            int j = 0;
            DataSet dshaz = new DataSet();
            DataSet dsreefer = new DataSet();
            DataSet subrptDs = new DataSet();
            string commgrp = null;
            DataSet AgntDS = new DataSet();
            string ProcessType = null;
            try
            {
                commgrp = "";
                AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                if (Process == 1)
                {
                    ProcessType = "EXPORT";
                }
                else
                {
                    ProcessType = "IMPORT";
                }
                if (BusinessType == 2)
                {
                    if (From == "MJC")
                    {
                        string CntrNr = null;
                        string SealNr = null;
                        int RwCnt = 0;
                        MBLReturnValue = objEmp.ExecuteScaler("SELECT j.mbl_mawb_fk FROM job_card_trn j WHERE j.job_card_trn_pk = " + JOBPK);
                        MBLPK = Convert.ToInt32(!string.IsNullOrEmpty(MBLReturnValue) ? Convert.ToInt32(MBLReturnValue) : 0);
                        objEmp = null;

                        repDoc.Load(rptPath + "\\MJCSeaCargoManifest.rpt");
                        MainRptDS = objCargo.FetchSeacargoReportDetailsMJC(Convert.ToString(MBLPK), Convert.ToString(JOBPK), 0, 2, ProcessType);
                        MainRptDS.ReadXmlSchema(rptPath + "\\MJCSeaCargoManifest.xsd");

                        CntDS = objCargo.FetchDetailSeacargoReport(Convert.ToString(JOBPK));
                        CntDS.ReadXmlSchema(rptPath + "\\MJCCntDetails.xsd");

                        AgntDS = objCargo.FetchAgntDtls(Convert.ToString(JOBPK));
                        AgntDS.ReadXmlSchema(rptPath + "\\AgentDtls.xsd");

                        ContDts = objCargo.FetchContainerDetaails(Convert.ToString(JOBPK));
                        if (CntDS.Tables[0].Rows.Count > 0)
                        {
                            if (Convert.ToInt32(MainRptDS.Tables[0].Rows[0]["CARGO_TYPE"]) == 1)
                            {
                                for (RwCnt = 0; RwCnt <= ContDts.Tables[0].Rows.Count - 1; RwCnt++)
                                {
                                    CntrNr = CntrNr + ContDts.Tables[0].Rows[RwCnt]["CONTAINERS"] + ",";
                                    SealNr = SealNr + ContDts.Tables[0].Rows[RwCnt]["SEALNUMBER"] + ",";
                                }
                            }
                            else
                            {
                                CntrNr = (string.IsNullOrEmpty(ContDts.Tables[0].Rows[0]["CONTAINERS"].ToString()) ? "" : ContDts.Tables[0].Rows[0]["CONTAINERS"].ToString());
                                SealNr = (string.IsNullOrEmpty(ContDts.Tables[0].Rows[0]["SEALNUMBER"].ToString()) ? "" : ContDts.Tables[0].Rows[0]["SEALNUMBER"].ToString());
                            }
                        }
                        repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                        repDoc.OpenSubreport("MJCCntDetails").SetDataSource(CntDS);
                        repDoc.OpenSubreport("AgentDtls").SetDataSource(AgntDS);
                        repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);
                        repDoc.SetDataSource(MainRptDS);
                        repDoc.SetParameterValue("CntrNr", ((CntrNr == null) ? "" : CntrNr));
                        repDoc.SetParameterValue("SealNr", ((SealNr == null) ? "" : SealNr));
                        //getReportControls(repDoc, "QFOR3064")
                    }
                    else
                    {
                        MBLReturnValue = objEmp.ExecuteScaler("SELECT j.mbl_mawb_fk FROM job_card_trn j WHERE j.job_card_trn_pk = " + JOBPK);
                        MBLPK = (!string.IsNullOrEmpty(MBLReturnValue) ? Convert.ToInt32(MBLReturnValue) : 0);
                        objEmp = null;
                        MainRptDS = objCargo.FetchSeacargoReportDetails(Convert.ToString(MBLPK), Convert.ToString(JOBPK),0 , BusinessType, ProcessType);
                        CntDS.Tables.Add();
                        CntDS.Tables[0].Columns.Add("MBPK", typeof(decimal));
                        CntDS.Tables[0].Columns.Add("CNTTYPE", typeof(string));
                        CntDS.Tables[0].Columns.Add("COUNT", typeof(decimal));
                        DataRow Mdr = null;
                        DataRow Cdr = null;
                        bool Flag = false;
                        foreach (DataRow Mdr_loopVariable in MainRptDS.Tables[0].Rows)
                        {
                            Mdr = Mdr_loopVariable;
                            Flag = false;
                            if (!string.IsNullOrEmpty(Mdr["CONTAINERTYPE"].ToString()))
                            {

                                foreach (DataRow Cdr_loopVariable in CntDS.Tables[0].Rows)
                                {
                                    Cdr = Cdr_loopVariable;
                                    if (Cdr["Cnttype"] == Mdr["CONTAINERTYPE"] & Cdr["MBPK"] == Mdr["MBLPK"])
                                    {
                                        Cdr["Count"] += "1";
                                        Flag = true;
                                        break; // TODO: might not be correct. Was : Exit For
                                    }
                                }
                                if (Flag == false)
                                {
                                    Cdr = CntDS.Tables[0].NewRow();
                                    Cdr["MBPK"] = Mdr["MBLPK"];
                                    Cdr["Cnttype"] = Mdr["CONTAINERTYPE"];
                                    Cdr["Count"] = 1;
                                    CntDS.Tables[0].Rows.Add(Cdr);

                                }
                            }

                        }
                        CntDS.AcceptChanges();
                        repDoc.Load(rptPath + "\\SeaCargoManifest.rpt");
                        MainRptDS.ReadXmlSchema(rptPath + "\\SeaCargoManifest.xsd");
                        CntDS.ReadXmlSchema(rptPath + "\\CntDetails.xsd");
                        repDoc.OpenSubreport("CntDetails").SetDataSource(CntDS);
                        dshaz = objCargo.FetchHazardousDetails(2, Convert.ToString(JOBPK), ProcessType);

                        dshaz.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\Hazardous.xsd");
                        repDoc.OpenSubreport("HazDetails").SetDataSource(dshaz);
                        dsreefer = objCargo.FetchReeferDetails(2, Convert.ToString(JOBPK), ProcessType);
                        dsreefer.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\reefer.xsd");
                        repDoc.OpenSubreport("ReeferDetails").SetDataSource(dsreefer);
                        subrptDs = objCargo.FetchFreightDetails(Convert.ToString(JOBPK), ProcessType);
                        subrptDs.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\FreightCargoManifest.xsd");
                        repDoc.OpenSubreport("FreightDetails").SetDataSource(subrptDs);
                        repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);
                        commgrp = objCargo.FetchCommGrp(Convert.ToString(JOBPK), "2", Process);
                        repDoc.SetDataSource(MainRptDS);

                        repDoc.SetParameterValue("commgrp", commgrp);
                        repDoc.SetParameterValue(0, "CargoManifest");
                        //repDoc.SetParameterValue(1, getDefault(AddressDS.Tables(0).Rows(0).Item("corporate_name"), ""))
                        getReportControls(repDoc, "QFOR3064");
                    }
                }
                else
                {
                    MBLReturnValue = objEmp.ExecuteScaler("SELECT j.mbl_mawb_fk FROM job_card_trn j WHERE j.job_card_trn_pk = " + JOBPK);
                    MBLPK = (!string.IsNullOrEmpty(MBLReturnValue) ? Convert.ToInt32(MBLReturnValue) : 0);
                    objEmp = null;
                    repDoc.Load(rptPath + "\\rptSeaCargoManifest.rpt");
                    MainRptDS = objCargo.FetchAircargoReportDetails(Convert.ToString(MBLPK), Convert.ToString(JOBPK), 0, ProcessType);
                    MainRptDS.ReadXmlSchema(rptPath + "\\Main_SeaCargoManifest.xsd");
                    dshaz = objCargo.FetchHazardousDetails(1, Convert.ToString(JOBPK));
                    repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);

                    dshaz = objCargo.FetchHazardousDetails(1, Convert.ToString(JOBPK), ProcessType);
                    repDoc.OpenSubreport("HazDetails").SetDataSource(dshaz);
                    dsreefer = objCargo.FetchReeferDetails(1, Convert.ToString(JOBPK), ProcessType);
                    dsreefer.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\reefer.xsd");
                    repDoc.OpenSubreport("ReeferDetails").SetDataSource(dsreefer);
                    subrptDs = objCargo.FetchFreightDetails(Convert.ToString(JOBPK), ProcessType);
                    subrptDs.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\FreightCargoManifest.xsd");
                    repDoc.OpenSubreport("FreightDetails").SetDataSource(subrptDs);
                    repDoc.SetDataSource(MainRptDS);

                    repDoc.SetParameterValue(0, 1);
                    repDoc.SetParameterValue(1, getDefault(AddressDS.Tables[0].Rows[0]["corporate_name"], ""));
                    repDoc.SetParameterValue(2, "CargoManifest");
                    getReportControls(repDoc, "QFOR3064", 2);
                }

                if (!(MainRptDS.Tables[0].Rows.Count > 0))
                {
                    repDoc = null;
                }
                return repDoc;
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        #endregion

        #region "Cargo manifest For Break Bulk"
        public object BBCargoManifestSea(long JOBPK, long logged_in_loc_fk, string mapPath, string rptPath, Int16 BusinessType, int Process)
        {
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            ReportDocument repDoc = new ReportDocument();
            DataSet MainRptDS = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet CntDS = new DataSet();
            DataSet HBLCOUNT = new DataSet();
            DataSet DimenDS = new DataSet();
            int i = 0;
            int Type = 0;
            Int32 MBLPK = default(Int32);
            string MBLReturnValue = null;
            WorkFlow objEmp = new WorkFlow();
            Cls_SeaCargoManifest objCargo = new Cls_SeaCargoManifest();
            string Headers = null;
            DataSet dsHdr = new DataSet();
            string[] PKHDr = null;
            int j = 0;
            DataSet dshaz = new DataSet();
            DataSet dsreefer = new DataSet();
            DataSet subrptDs = new DataSet();
            //Snigdharani
            string commgrp = null;
            DataSet CommDS = new DataSet();
            string ProcessType = null;
            try
            {
                commgrp = "All";
                if (Process == 1)
                {
                    ProcessType = "EXPORT";
                }
                else
                {
                    ProcessType = "IMPORT";
                }
                AddressDS = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                if (BusinessType == 2)
                {
                    MBLReturnValue = objEmp.ExecuteScaler("SELECT j.mbl_mawb_fk FROM job_card_trn j WHERE j.job_card_trn_pk = " + JOBPK);
                    MBLPK = (!string.IsNullOrEmpty(MBLReturnValue) ? Convert.ToInt32(MBLReturnValue) : 0);
                    objEmp = null;
                    MainRptDS = objCargo.FetchSeacargoReportDetails(Convert.ToString(MBLPK), Convert.ToString(JOBPK),0 , 2, ProcessType);
                    CommDS = objCargo.FetchCommodityDetails(Convert.ToString(JOBPK), ProcessType);
                    CntDS.Tables.Add();
                    CntDS.Tables[0].Columns.Add("MBPK", typeof(decimal));
                    CntDS.Tables[0].Columns.Add("CNTTYPE", typeof(string));
                    CntDS.Tables[0].Columns.Add("COUNT", typeof(decimal));
                    DataRow Mdr = null;
                    DataRow Cdr = null;
                    bool Flag = false;
                    foreach (DataRow Mdr_loopVariable in MainRptDS.Tables[0].Rows)
                    {
                        Mdr = Mdr_loopVariable;
                        Flag = false;
                        if (!string.IsNullOrEmpty(Convert.ToString(Mdr["CONTAINERTYPE"])))
                        {

                            foreach (DataRow Cdr_loopVariable in CntDS.Tables[0].Rows)
                            {
                                Cdr = Cdr_loopVariable;
                                if (Cdr["Cnttype"] == Mdr["CONTAINERTYPE"] & Cdr["MBPK"] == Mdr["MBLPK"])
                                {
                                    Cdr["Count"] += "1";
                                    Flag = true;
                                    break; // TODO: might not be correct. Was : Exit For
                                }
                            }
                            if (Flag == false)
                            {
                                Cdr = CntDS.Tables[0].NewRow();
                                Cdr["MBPK"] = Mdr["MBLPK"];
                                Cdr["Cnttype"] = Mdr["CONTAINERTYPE"];
                                Cdr["Count"] = 1;
                                CntDS.Tables[0].Rows.Add(Cdr);

                            }
                        }
                    }
                    CntDS.AcceptChanges();
                    repDoc.Load(rptPath + "\\SeaCargomanifestBB.rpt");
                    MainRptDS.ReadXmlSchema(rptPath + "\\SeaCargoManifest.xsd");
                    CntDS.ReadXmlSchema(rptPath + "\\CntDetails.xsd");
                    repDoc.OpenSubreport("CntDetails").SetDataSource(CntDS);

                    repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                    repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);

                    dshaz = objCargo.FetchHazardousDetails(2, Convert.ToString(JOBPK), ProcessType);
                    dshaz.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\Hazardous.xsd");
                    repDoc.OpenSubreport("HazDetails").SetDataSource(dshaz);

                    dsreefer = objCargo.FetchReeferDetails(2, Convert.ToString(JOBPK), ProcessType);
                    dsreefer.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\reefer.xsd");
                    repDoc.OpenSubreport("ReeferDetails").SetDataSource(dsreefer);

                    subrptDs = objCargo.FetchFreightDetails(Convert.ToString(JOBPK), ProcessType);
                    subrptDs.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\FreightCargoManifest.xsd");
                    repDoc.OpenSubreport("FreightDetails").SetDataSource(subrptDs);

                    CommDS.ReadXml(HttpContext.Current.Server.MapPath("../07Reports") + "\\CommDetails.xsd");
                    repDoc.OpenSubreport("CommodityDetails").SetDataSource(CommDS);
                    repDoc.SetDataSource(MainRptDS);
                    repDoc.SetParameterValue("commgrp", commgrp);
                    repDoc.SetParameterValue("Type", "CargoManifest");
                    getReportControls(repDoc, "QFOR3064");

                }
                else
                {
                    MBLReturnValue = objEmp.ExecuteScaler("SELECT j.mbl_mawb_fk FROM job_card_trn j WHERE j.job_card_trn_pk = " + JOBPK);
                    MBLPK = (!string.IsNullOrEmpty(MBLReturnValue) ? Convert.ToInt32(MBLReturnValue) : 0);
                    objEmp = null;
                    repDoc.Load(rptPath + "\\rptSeaCargoManifest.rpt");
                    MainRptDS = objCargo.FetchAircargoReportDetails(Convert.ToString(MBLPK), Convert.ToString(JOBPK));
                    MainRptDS.ReadXmlSchema(rptPath + "\\Main_SeaCargoManifest.xsd");

                    repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                    repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);

                    dshaz = objCargo.FetchHazardousDetails(1, Convert.ToString(JOBPK));
                    dshaz.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\Hazardous.xsd");
                    repDoc.OpenSubreport("HazDetails").SetDataSource(dshaz);


                    dsreefer = objCargo.FetchReeferDetails(1, Convert.ToString(JOBPK));
                    dsreefer.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\reefer.xsd");
                    repDoc.OpenSubreport("ReeferDetails").SetDataSource(dsreefer);

                    repDoc.SetParameterValue(0, 1);
                    repDoc.SetParameterValue(1, getDefault(AddressDS.Tables[0].Rows[0]["corporate_name"], ""));
                    repDoc.SetParameterValue(2, "CargoManifest");

                    subrptDs = objCargo.FetchFreightDetails(Convert.ToString(JOBPK));
                    subrptDs.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\FreightCargoManifest.xsd");
                    repDoc.OpenSubreport("FreightDetails").SetDataSource(subrptDs);
                    repDoc.SetDataSource(MainRptDS);
                    getReportControls(repDoc, "QFOR3064", 2);
                }

                if (!(MainRptDS.Tables[0].Rows.Count > 0))
                {
                    repDoc = null;
                }
                return repDoc;
            }
            catch (Exception ex)
            {
                return null;
            }
        }


        #endregion

        #region "Get Invoice PK"
        //0 - means no invoice exists.
        //-1 - means more then one invoice extists
        //pk value of invoice

        //invice Type: 1-Invoice to customer sea exp.
        //invice Type: 2-Invoice to customer air exp.
        //invice Type: 3-Invoice to customer sea imp.
        //invice Type: 4-Invoice to customer air imp.

        //invice Type: 5-Invoice to CB Agent sea exp.
        //invice Type: 6-Invoice to CB Agent air exp.
        //invice Type: 7-Invoice to CB Agent sea imp.
        //invice Type: 8-Invoice to CB Agent air imp.

        //invice Type: 9-Invoice to DP Agent sea exp.
        //invice Type: 10-Invoice to DP Agent air exp.
        //invice Type: 11-Invoice to DP Agent sea imp.
        //invice Type: 12-Invoice to DP Agent air imp.
        public ArrayList GetInvoicePK(string jobCardPK, Int16 invoiceType = 1, string InvNr = "")
        {

            System.Text.StringBuilder SQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            OracleDataReader oraReader = null;
            int invoiceCount = 0;
            long invoicePK = 0;
            ArrayList InvPks = new ArrayList();

            switch (invoiceType)
            {
                case 1:
                    //SQL.Append(vbCrLf & "select i.inv_cust_sea_exp_pk from inv_cust_sea_exp_tbl i where i.job_card_sea_exp_fk = " & jobCardPK)
                    SQL.Append( "select distinct c.consol_invoice_fk from consol_invoice_trn_tbl c where c.job_card_fk = " + jobCardPK);

                    break;
                case 2:
                    //SQL.Append(vbCrLf & "select i.inv_cust_air_exp_pk from inv_cust_air_exp_tbl i where i.job_card_air_exp_fk = " & jobCardPK)
                    SQL.Append( "select distinct c.consol_invoice_fk from consol_invoice_trn_tbl c where c.job_card_fk = " + jobCardPK);

                    break;
                case 3:
                    //SQL.Append(vbCrLf & "select i.inv_cust_sea_imp_pk from inv_cust_sea_imp_tbl i where i.job_card_sea_imp_fk = " & jobCardPK)
                    SQL.Append( "select distinct c.consol_invoice_fk from consol_invoice_trn_tbl c where c.job_card_fk = " + jobCardPK);

                    break;
                case 4:
                    //SQL.Append(vbCrLf & "select i.inv_cust_air_imp_pk from inv_cust_air_imp_tbl i where i.job_card_air_imp_fk = " & jobCardPK)
                    SQL.Append( "select distinct c.consol_invoice_fk from consol_invoice_trn_tbl c where c.job_card_fk = " + jobCardPK);

                    break;
                case 5:
                    SQL.Append( "select i.inv_agent_pk from inv_agent_tbl i where i.CB_DP_LOAD_AGENT=1 AND  i.job_card_fk = " + jobCardPK);
                    if (!string.IsNullOrEmpty(InvNr))
                    {
                        SQL.Append( "  AND I.INVOICE_REF_NO='" + InvNr + "'");
                    }
                    break;
                case 6:
                    SQL.Append( "select i.inv_agent_pk from inv_agent_tbl i where i.CB_DP_LOAD_AGENT=1 AND  i.job_card_fk = " + jobCardPK);
                    if (!string.IsNullOrEmpty(InvNr))
                    {
                        SQL.Append( "  AND I.INVOICE_REF_NO='" + InvNr + "'");
                    }
                    break;
                case 7:
                    SQL.Append( "select i.inv_agent_pk from inv_agent_tbl i where i.CB_DP_LOAD_AGENT =1 AND  i.job_card_fk = " + jobCardPK);
                    if (!string.IsNullOrEmpty(InvNr))
                    {
                        SQL.Append( "  AND I.INVOICE_REF_NO='" + InvNr + "'");
                    }
                    break;
                case 8:
                    SQL.Append( "select i.inv_agent_pk from inv_agent_tbl i where i.CB_DP_LOAD_AGENT=1 AND  i.job_card_fk = " + jobCardPK);
                    if (!string.IsNullOrEmpty(InvNr))
                    {
                        SQL.Append( "  AND I.INVOICE_REF_NO='" + InvNr + "'");
                    }
                    break;
                case 9:
                    SQL.Append( "select i.inv_agent_pk from inv_agent_tbl i where  i.CB_DP_LOAD_AGENT=2 AND i.job_card_fk = " + jobCardPK);
                    if (!string.IsNullOrEmpty(InvNr))
                    {
                        SQL.Append( "  AND I.INVOICE_REF_NO='" + InvNr + "'");
                    }

                    break;
                case 10:
                    SQL.Append( "select i.inv_agent_pk from inv_agent_tbl i where  i.CB_DP_LOAD_AGENT = 2 AND i.job_card_fk = " + jobCardPK);
                    if (!string.IsNullOrEmpty(InvNr))
                    {
                        SQL.Append( "  AND I.INVOICE_REF_NO='" + InvNr + "'");
                    }
                    break;
                case 11:
                    SQL.Append( "select i.inv_agent_pk from inv_agent_tbl i where  i.CB_DP_LOAD_AGENT=2 AND i.job_card_fk = " + jobCardPK);
                    if (!string.IsNullOrEmpty(InvNr))
                    {
                        SQL.Append( "  AND I.INVOICE_REF_NO='" + InvNr + "'");
                    }
                    break;
                case 12:
                    SQL.Append( "select i.inv_agent_pk from inv_agent_tbl i where  i.CB_DP_LOAD_AGENT=2 AND i.job_card_fk = " + jobCardPK);
                    if (!string.IsNullOrEmpty(InvNr))
                    {
                        SQL.Append( "  AND I.INVOICE_REF_NO='" + InvNr + "'");
                    }
                    break;
            }


            oraReader = objWF.GetDataReader(SQL.ToString());

            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], DBNull.Value)))
                {
                    InvPks.Add(oraReader[0]);
                }
            }

            return InvPks;
            oraReader.Close();
            try
            {
                return InvPks;
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

        #region "invoice to customer"
        public object INVToCustomer(long INVPK, long logged_in_loc_fk, string mapPath, string rptPath, Int16 CargoType = 0)
        {
            CrystalDecisions.CrystalReports.Engine.ReportDocument rptDoc = new CrystalDecisions.CrystalReports.Engine.ReportDocument();
            clsConsolidatedInvoices objConsInv = new clsConsolidatedInvoices();
            cls_BlClauseForHblMbl objClsBlClause = new cls_BlClauseForHblMbl();
            CommonFeatures objrep = new CommonFeatures();
            DataSet DsInvDetails = new DataSet();
            DataSet FetchModiUser = new DataSet();
            DataSet DSFetchCurContDtl = new DataSet();
            DataSet DSContType = new DataSet();
            DataSet DsCrInvDetailsMain = new DataSet();
            DataSet DsCrInvDetailsSub = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet DSBankDetails = new DataSet();
            DataSet dsClause = new DataSet();
            DataSet ContactDS = new DataSet();
            DataSet DsCurr = new DataSet();
            DataSet DsCrInvMain = new DataSet();
            DataSet GridDS = new DataSet();
            DataSet MainDS = new DataSet();
            int nInvPK = Convert.ToInt32(INVPK);
            string CONTAINER = "";
            string strConsigneeLbl = "";
            string Barcode = "";
            int invPkPrint = 0;
            Int32 nParentRowCnt = default(Int32);
            string[] PK = null;
            Int32 j = default(Int32);
            Int32 i = default(Int32);
            string filename = null;
            DataSet dsdtvat = new DataSet();
            DataSet dsamt = new DataSet();
            DataSet dsinv = new DataSet();
            Int32 chk = 0;
            string Refno = null;
            string customer = null;
            string uniqrefno = null;
            Int32 biztype = default(Int32);
            Int32 Process = default(Int32);
            Int32 Jobpk = default(Int32);
            Int32 isAIF = default(Int32);
            try
            {
                dsinv = GetInvDtls(Convert.ToInt32(INVPK));
                if (dsinv.Tables.Count > 0)
                {
                    if (dsinv.Tables[0].Rows.Count > 0)
                    {
                        if (INVPK == 0)
                        {
                            //invPkPrint = ViewState["InvPk"];
                        }
                        else
                        {
                            invPkPrint = Convert.ToInt32(INVPK);
                        }
                        Jobpk = Convert.ToInt32(dsinv.Tables[0].Rows[0][0]);
                        //jobpk
                        Refno = Convert.ToString(dsinv.Tables[0].Rows[0][1]);
                        //invoice refno
                        customer = Convert.ToString(dsinv.Tables[0].Rows[0][2]);
                        //cust. name
                        biztype = Convert.ToInt32(dsinv.Tables[0].Rows[0][3]);
                        //jobpk
                        Process = Convert.ToInt32(dsinv.Tables[0].Rows[0][4]);
                        //process
                        uniqrefno = Convert.ToString(getDefault(dsinv.Tables[0].Rows[0][5], ""));
                        //process
                        isAIF = Convert.ToInt32(dsinv.Tables[0].Rows[0][6]);
                        //AIF
                        rptDoc.Load(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\RptConsolInvDetail.rpt");
                        rptDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

                        DsInvDetails = objConsInv.INV_DETAIL_PRINT(Convert.ToString(invPkPrint), biztype, Process);

                        DsCrInvDetailsMain = objConsInv.CONSOL_INV_DETAIL_MAIN_PRINT(Convert.ToString(invPkPrint), biztype, Process);
                        DsCrInvDetailsMain.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Details.xsd");
                        if (isAIF == 0)
                        {
                            DsCrInvDetailsSub = objConsInv.CONSOL_INV_DETAIL_SUB_MAIN_PRINT(Convert.ToString(invPkPrint), biztype, Process);
                        }
                        else
                        {
                            DsCrInvDetailsSub = objConsInv.CONSOL_INV_DETAIL_AIF_PRINT(Convert.ToString(invPkPrint), biztype, Process);
                        }

                        DsCrInvDetailsSub.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Sub_Details.xsd");

                        AddressDS = objConsInv.FetchLocation(Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                        AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\CR_Location.xsd");
                        //CR_LOC_DETAILS
                        //'Added by gangadhar
                        DSBankDetails = objConsInv.BankDetails(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                        DSBankDetails.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\BankDetails.xsd");

                        ContactDS = objConsInv.FetchLocationNew(Convert.ToInt32(HttpContext.Current.Session["USER_PK"]));
                        ContactDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\ContactAddress.xsd");

                        DsCurr = objConsInv.CONSOL_INV_DETAIL_CURR_MAIN_PRINT(Convert.ToString(invPkPrint), biztype, Process);
                        DsCurr.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Curr_Details.xsd");

                        DsCrInvMain = objConsInv.CONSOL_INV_CUST_PRINT(invPkPrint, biztype, Process);
                        DsCrInvMain.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Custumer.xsd");

                        FetchModiUser = objConsInv.FetchInvModifiedUserDetails(invPkPrint);
                        FetchModiUser.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\FetchModiUser.XSD");

                        DSFetchCurContDtl = objConsInv.FetchCurContSummary(invPkPrint);
                        DSFetchCurContDtl.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\CurSummary.XSD");

                        DSContType = objConsInv.FetchContCount(invPkPrint);
                        DSContType.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\ContCount.XSD");

                        GridDS = objClsBlClause.FetchBlClausesForHBL("", 8, 1, 1, "0", Convert.ToString(invPkPrint));
                        GridDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\PrintBLClause.XSD");

                        rptDoc.OpenSubreport("rptBLClause").SetDataSource(GridDS);
                        rptDoc.OpenSubreport("Cr_Location").SetDataSource(AddressDS);
                        rptDoc.OpenSubreport("RptCustDetails").SetDataSource(DsCrInvMain);
                        rptDoc.OpenSubreport("RptSubDetails").SetDataSource(DsCrInvDetailsSub);
                        rptDoc.OpenSubreport("RptBankDetails").SetDataSource(DSBankDetails);
                        rptDoc.OpenSubreport("RptContactAddress").SetDataSource(ContactDS);
                        rptDoc.OpenSubreport("RptCurrency").SetDataSource(DsCurr);
                        rptDoc.OpenSubreport("FetchModiUser").SetDataSource(FetchModiUser);
                        rptDoc.OpenSubreport("CurSummary").SetDataSource(DSFetchCurContDtl);
                        rptDoc.OpenSubreport("ContCount").SetDataSource(DSContType);
                        DataSet DSCargoType = null;
                        DSCargoType = objConsInv.FetchCargoType(invPkPrint);
                        int CargoType1 = Convert.ToInt32(DSCargoType.Tables[0].Rows[0]["CARGO_TYPE"]);
                        rptDoc.SetDataSource(DsCrInvDetailsMain);

                        string ShowBarCode = ConfigurationSettings.AppSettings["ShowBarcode"];
                        if (ShowBarCode == "1" & objConsInv.GetBarcodeFlag("CONSOLIDATED INVOICE") == "1")
                        {
                            Barcode = "*" + FetchBarcodes(Refno, customer) + "*";
                        }

                        objrep.getReportControls(rptDoc, "QFOR4078", 5);
                        if (DsInvDetails.Tables[0].Rows.Count > 0)
                        {
                            rptDoc.SetParameterValue(3, (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"]));
                            rptDoc.SetParameterValue(4, (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["CURRENCY_ID"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["CURRENCY_ID"]));
                            rptDoc.SetParameterValue(5, (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["TAX_AMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["TAX_AMT"]));
                            rptDoc.SetParameterValue(6, (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["DICSOUNT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["DICSOUNT"]));
                            rptDoc.SetParameterValue(7, "");
                            rptDoc.SetParameterValue(8, (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["NET_INV_AMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["NET_INV_AMT"]));
                            rptDoc.SetParameterValue(9, (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_DATE"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_DATE"]));
                            rptDoc.SetParameterValue(10, Barcode);
                            rptDoc.SetParameterValue(28, biztype);
                            rptDoc.SetParameterValue(29, Process);
                            rptDoc.SetParameterValue(64, (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_DUE_DATE"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_DUE_DATE"]));
                            rptDoc.SetParameterValue("UniqueRefNr", Refno);
                            rptDoc.SetParameterValue("CargoType", CargoType1);
                            //rptDoc.SetParameterValue("Remarks", IIf(IsDBNull(DsInvDetails.Tables(0).Rows(0).Item("REMARKS")), "", DsInvDetails.Tables(0).Rows(0).Item("REMARKS")))
                        }
                        if (DsCrInvDetailsMain.Tables[0].Rows.Count > 0)
                        {
                            rptDoc.SetParameterValue("Vessel", (string.IsNullOrEmpty(DsCrInvDetailsMain.Tables[0].Rows[0]["VSL_VOY"].ToString()) ? "TBA" : DsCrInvDetailsMain.Tables[0].Rows[0]["VSL_VOY"]));
                        }
                        else
                        {
                            rptDoc.SetParameterValue("Vessel", "TBA");
                        }
                        rptDoc.SetParameterValue("InvAmt", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["TOTAMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["TOTAMT"]));
                        rptDoc.SetParameterValue("PrtHdr", true);
                        rptDoc.SetParameterValue("Remarks", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["REMARKS"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["REMARKS"]));
                        string duedate = null;
                        duedate = GetPaymentdue(Convert.ToInt32(INVPK), biztype, Process);
                        duedate = (!string.IsNullOrEmpty(getDefault(duedate, "").ToString()) ? duedate : "Immediate");
                        //rptDoc.SetParameterValue("Paydue", duedate)
                        string payduedt = null;
                        payduedt = objConsInv.GetInvCrday(Convert.ToString(invPkPrint), duedate, biztype, Process);
                        rptDoc.SetParameterValue("Paydue", (!string.IsNullOrEmpty(getDefault(payduedt, "").ToString()) ? payduedt : "Immediate"));
                        rptDoc.SetParameterValue("PS_AMT", "0.00");
                    }
                }

                //Dim DsInvoiceDet As New DataSet
                //Dim UniqueRefNo As String
                //UniqueRefNo = "Invoice" & "-" & "-" & DsInvDetails.Tables(0).Rows(0).Item("INVOICE_REF_NO") & "-" & Format(Date.Now, "ddMMyyyyHHmmss").ToString()

                objrep.getReportControls(rptDoc, "QFOR4078", 1);
                return rptDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "invoice to customer"
        private DataSet GetInvDtls(Int32 invpk)
        {
            System.Text.StringBuilder SQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                SQL.Append(" SELECT DISTINCT TRN.JOB_CARD_FK, MAS.INVOICE_REF_NO REFNO,CUST.CUSTOMER_NAME ,MAS.BUSINESS_TYPE,MAS.PROCESS_TYPE ,MAS.INV_UNIQUE_REF_NR,MAS.AIF FROM CONSOL_INVOICE_TBL MAS,CONSOL_INVOICE_TRN_TBL TRN,CUSTOMER_MST_TBL CUST WHERE 1=1 ");
                SQL.Append(" AND MAS.CONSOL_INVOICE_PK=" + invpk + " AND MAS.CONSOL_INVOICE_PK=TRN.CONSOL_INVOICE_FK  ");
                SQL.Append(" AND CUST.CUSTOMER_MST_PK=MAS.CUSTOMER_MST_FK ");
                return objWF.GetDataSet(SQL.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //This block adding by thiyagrajan on 18/2/09
        public void ReportParameters(Int32 Jobpk, Int32 biztype, Int32 process, CrystalDecisions.CrystalReports.Engine.ReportDocument repdoc, Int32 CargoType = 0)
        {
            DataSet dsparameter = new DataSet();
            Int32 i = default(Int32);
            clsConsolidatedInvoices objConsInv = new clsConsolidatedInvoices();
            try
            {
                if (biztype == 2)
                {
                    dsparameter = objConsInv.FetchJobCardSeaDetails(Convert.ToString(Jobpk), process);
                }
                else
                {
                    dsparameter = objConsInv.FetchJobCardAirDetails(Convert.ToString(Jobpk), process);
                }

                if (dsparameter.Tables[0].Rows.Count > 0)
                {
                    repdoc.SetParameterValue(8, getDefault(dsparameter.Tables[0].Rows[0]["CONSIGNAME"], ""));
                    repdoc.SetParameterValue(9, getDefault(dsparameter.Tables[0].Rows[0]["CONSIGADD1"], ""));
                    repdoc.SetParameterValue(10, getDefault(dsparameter.Tables[0].Rows[0]["CONSIGADD2"], ""));
                    repdoc.SetParameterValue(11, getDefault(dsparameter.Tables[0].Rows[0]["CONSIGADD3"], ""));
                    repdoc.SetParameterValue(12, getDefault(dsparameter.Tables[0].Rows[0]["CONSIGZIP"], ""));
                    repdoc.SetParameterValue(13, getDefault(dsparameter.Tables[0].Rows[0]["CONSIGCITY"], ""));
                    repdoc.SetParameterValue(14, getDefault(dsparameter.Tables[0].Rows[0]["CONSCOUNTRY"], ""));
                    repdoc.SetParameterValue(15, getDefault(dsparameter.Tables[0].Rows[0]["POLNAME"], ""));
                    repdoc.SetParameterValue(16, getDefault(dsparameter.Tables[0].Rows[0]["PODNAME"], ""));
                    repdoc.SetParameterValue(17, getDefault(dsparameter.Tables[0].Rows[0]["VES_VOY"], ""));
                    repdoc.SetParameterValue(18, getDefault(dsparameter.Tables[0].Rows[0]["DEL_PLACE_NAME"], ""));
                    repdoc.SetParameterValue(19, getDefault(dsparameter.Tables[0].Rows[0]["BLREFNO"], ""));

                    repdoc.SetParameterValue(20, getDefault(dsparameter.Tables[0].Rows[0]["TERMS"], ""));
                    repdoc.SetParameterValue(21, getDefault(dsparameter.Tables[0].Rows[0]["PYMT_TYPE"], ""));
                    repdoc.SetParameterValue(22, getDefault(dsparameter.Tables[0].Rows[0]["INSURANCE"], ""));
                    repdoc.SetParameterValue(23, getDefault(dsparameter.Tables[0].Rows[0]["MARKS"], ""));

                    repdoc.SetParameterValue(24, getDefault(dsparameter.Tables[0].Rows[0]["GOODS_DESCRIPTION"], ""));

                    // repdoc.SetParameterValue(25, getDefault(dsparameter.Tables(0).Rows(0).Item("VOLUME"), ""))
                    repdoc.SetParameterValue(25, getDefault(dsparameter.Tables[0].Rows[0]["VOLUME"], 0));
                    if (CargoType == 4)
                    {
                        repdoc.SetParameterValue(26, getDefault(dsparameter.Tables[0].Rows[0]["CHARWT"], 0));
                    }
                    else
                    {
                        //repdoc.SetParameterValue(26, getDefault(dsparameter.Tables(0).Rows(0).Item("GROSSWEIGHT"), ""))
                        repdoc.SetParameterValue(26, getDefault(dsparameter.Tables[0].Rows[0]["GROSSWEIGHT"], 0));
                    }
                    repdoc.SetParameterValue(27, getDefault(dsparameter.Tables[0].Rows[0]["CHARWT"], 0));
                    repdoc.SetParameterValue(28, getDefault("SGD", ""));
                }
                else
                {
                    for (i = 8; i <= 28; i += 1)
                    {
                        if (i >= 25)
                        {
                            repdoc.SetParameterValue(i, 0.0);
                        }
                        else
                        {
                            repdoc.SetParameterValue(i, "");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //This block adding by thiyagrajan on 18/2/09
        public string FetchBarcodes(string refno, string customer)
        {
            int strReturnPk = 0;
            DataSet DsReturn = null;
            string strBarcode = null;
            Int16 i = default(Int16);
            clsConsolidatedInvoices objConsInv = new clsConsolidatedInvoices();
            try
            {
                strReturnPk = objConsInv.FetchBarCodeManagerPk(2, 1);
                DsReturn = objConsInv.FetchBarCodeField(strReturnPk);
                strBarcode = "";


                if (DsReturn.Tables[0].Rows.Count > 0)
                {
                    for (i = 0; i <= DsReturn.Tables[0].Rows.Count - 1; i++)
                    {
                        var _with7 = DsReturn.Tables[0].Rows[i];
                        if (removeDBNull(_with7["field_name"]) == "Consolidate Invoice Nr")
                        {
                            if (!string.IsNullOrEmpty(strBarcode))
                            {
                                strBarcode = strBarcode + "~" + refno;
                            }
                            else
                            {
                                strBarcode = strBarcode + refno;
                            }

                        }
                        else if (removeDBNull(_with7["field_name"]) == "Jobcard Nr")
                        {
                            if (!string.IsNullOrEmpty(strBarcode))
                            {
                                strBarcode = strBarcode + "~" + customer;
                            }
                            else
                            {
                                strBarcode = strBarcode + customer;
                            }
                        }
                    }
                }
                return strBarcode;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public object INVCustExpAir(long INVPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            clsInvoiceListAir ObjClsITCprinting = new clsInvoiceListAir();
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            ReportDocument rptdoc = new ReportDocument();
            DataSet dsImain = new DataSet();
            DataSet dsLoc = new DataSet();
            DataSet CntDS = new DataSet();
            int I = 0;
            Int32 nInvPK = Convert.ToInt32(INVPK);
            string CONTAINER = "";
            string invreturn = null;
            System.DateTime Invoicedate = default(System.DateTime);
            string Barcode = "";
            //Dim repDocUsr As New ReportDocument
            //Dim Str As String       'Added By AREEF
            //Dim im As New DataSet

            try
            {
                //Str = ""
                //Str = Session("ImageFile")
                //Str = HttpContext.Current.Server.MapPath("..\..") & "\Supports\" & Str
                //im = ImageTable(Str)
                //Str = "rptsubinvoiceimage.rpt"
                //im.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\07Reports") & "\ckim.xsd")  'End of Areef Statements
                if (nInvPK > 0)
                {
                    rptdoc.Load(rptPath + "\\rptITCexp.rpt");
                    //Modified By Manoharan on 19Sep2007
                    rptdoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                    //repDocUsr = rptdoc.OpenSubreport(Str)
                    //repDocUsr.Database.Tables(0).SetDataSource(im)
                    dsImain = ObjClsITCprinting.FetchInvToCustExpAirMain(nInvPK);
                    dsImain.ReadXmlSchema(rptPath + "\\ITCEXPSEAMAIN.XSD");
                    CntDS = ObjClsITCprinting.FetchContainerDetails(nInvPK);
                    for (I = 0; I <= CntDS.Tables[0].Rows.Count - 1; I++)
                    {
                        if (!string.IsNullOrEmpty(CntDS.Tables[0].Rows[I]["CONTAINER"].ToString()))
                        {
                            CONTAINER = CONTAINER + CntDS.Tables[0].Rows[I]["CONTAINER"] + ",";
                        }
                    }
                    if ((CONTAINER.LastIndexOf(",") != -1))
                    {
                        CONTAINER = CONTAINER.Remove(CONTAINER.LastIndexOf(","), 1);
                    }
                    if (ObjClsITCprinting.GetBarcodeFlag("AGENT INVOICE SEA EXPORT") == "1" & ConfigurationSettings.AppSettings["ShowBarcode"] == "1")
                    {
                        Barcode = "*" + Convert.ToString(dsImain.Tables[0].Rows[0]["INVREFNO"]) + "*";
                    }
                    dsLoc = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                    dsLoc.ReadXmlSchema(rptPath + "\\TN_Location.xsd");

                    rptdoc.OpenSubreport("AddressDetails").SetDataSource(dsLoc);
                    rptdoc.OpenSubreport("AddressDetails1").SetDataSource(dsLoc);
                    if (!string.IsNullOrEmpty(dsImain.Tables[0].Rows[0]["INVOICE_DATE"].ToString()))
                    {
                        Invoicedate = DateAndTime.DateAdd(DateInterval.Day, Convert.ToInt32(getDefault(dsImain.Tables[0].Rows[0]["PAYMENTDAYS"], 0)), Convert.ToDateTime(dsImain.Tables[0].Rows[0]["INVOICE_DATE"]));
                    }

                    rptdoc.SetParameterValue(2, Invoicedate);
                    rptdoc.SetParameterValue(3, Barcode);
                    //'surya23Nov06
                    rptdoc.SetParameterValue(0, CONTAINER);
                    rptdoc.SetParameterValue(1, 1);
                    rptdoc.SetDataSource(dsImain);
                }
                else
                {
                    rptdoc = null;
                }
                return rptdoc;
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        public object INVCustImpSea(long INVPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            clsInvoiceListAir ObjClsITCprinting = new clsInvoiceListAir();
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            ReportDocument rptdoc = new ReportDocument();
            DataSet dsImain = new DataSet();
            DataSet dsLoc = new DataSet();
            DataSet CntDS = new DataSet();
            int I = 0;
            Int32 nInvPK = Convert.ToInt32(INVPK);
            string CONTAINER = "";
            string invreturn = null;
            System.DateTime Invoicedate = default(System.DateTime);
            string Barcode = "";
            //Dim repDocUsr As New ReportDocument
            //Dim Str As String       'Added By AREEF
            //Dim im As New DataSet

            try
            {
                //Str = ""
                //Str = Session("ImageFile")
                //Str = HttpContext.Current.Server.MapPath("..\..") & "\Supports\" & Str
                //im = ImageTable(Str)
                //Str = "rptsubinvoiceimage.rpt"
                //im.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\07Reports") & "\ckim.xsd")  'End of Areef Statements
                if (nInvPK > 0)
                {
                    rptdoc.Load(rptPath + "\\rptITCexp.rpt");
                    //Modified By Manoharan on 19Sep2007
                    rptdoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                    //repDocUsr = rptdoc.OpenSubreport(Str)
                    //repDocUsr.Database.Tables(0).SetDataSource(im)
                    dsImain = ObjClsITCprinting.FetchInvToCustSeaImpMain(nInvPK);
                    CntDS = ObjClsITCprinting.FetchSeaImpContainerDetails(nInvPK);
                    for (I = 0; I <= CntDS.Tables[0].Rows.Count - 1; I++)
                    {
                        if (!string.IsNullOrEmpty(Convert.ToString(CntDS.Tables[0].Rows[I]["CONTAINER"])))
                        {
                            CONTAINER = CONTAINER + CntDS.Tables[0].Rows[I]["CONTAINER"] + ",";
                        }
                    }
                    if ((CONTAINER.LastIndexOf(",") != -1))
                    {
                        CONTAINER = CONTAINER.Remove(CONTAINER.LastIndexOf(","), 1);
                    }
                    if (ObjClsITCprinting.GetBarcodeFlag("AGENT INVOICE SEA EXPORT") == "1" & ConfigurationSettings.AppSettings["ShowBarcode"] == "1")
                    {
                        Barcode = "*" + Convert.ToString(dsImain.Tables[0].Rows[0]["INVREFNO"]) + "*";
                    }
                    dsLoc = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                    dsLoc.ReadXmlSchema(rptPath + "\\TN_Location.xsd");

                    rptdoc.OpenSubreport("AddressDetails").SetDataSource(dsLoc);
                    rptdoc.OpenSubreport("AddressDetails1").SetDataSource(dsLoc);
                    if (!string.IsNullOrEmpty(Convert.ToString(dsImain.Tables[0].Rows[0]["INVOICE_DATE"])))
                    {
                        Invoicedate = DateAndTime.DateAdd(DateInterval.Day, Convert.ToDouble(getDefault(dsImain.Tables[0].Rows[0]["PAYMENTDAYS"], 0)), Convert.ToDateTime(dsImain.Tables[0].Rows[0]["INVOICE_DATE"]));
                    }

                    rptdoc.SetParameterValue(2, Invoicedate);
                    rptdoc.SetParameterValue(3, Barcode);
                    //'surya23Nov06
                    rptdoc.SetParameterValue(0, CONTAINER);
                    rptdoc.SetParameterValue(1, 2);
                    rptdoc.SetDataSource(dsImain);
                }
                else
                {
                    rptdoc = null;
                }
                return rptdoc;
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        public object INVCustImpAir(long INVPK, long logged_in_loc_fk, string mapPath, string rptPath)
        {
            clsInvoiceListAir ObjClsITCprinting = new clsInvoiceListAir();
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            ReportDocument rptdoc = new ReportDocument();
            DataSet dsImain = new DataSet();
            DataSet dsLoc = new DataSet();
            DataSet CntDS = new DataSet();
            int I = 0;
            Int32 nInvPK = Convert.ToInt32(INVPK);
            string CONTAINER = "";
            string invreturn = null;
            System.DateTime Invoicedate = default(System.DateTime);
            string Barcode = "";
            //add by latha for logon failed
            //Dim repDocUsr As New ReportDocument
            //Dim Str As String       'Added By AREEF
            //Dim im As New DataSet
            try
            {
                //'add by latha for logon failed
                //Str = ""
                //Str = Session("ImageFile")
                //Str = HttpContext.Current.Server.MapPath("..\..") & "\Supports\" & Str
                //im = ImageTable(Str)
                //Str = "rptsubinvoiceimage.rpt"
                //im.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\07Reports") & "\ckim.xsd")  'End of Areef Statements
                if (nInvPK > 0)
                {
                    rptdoc.Load(rptPath + "\\rptITCexp.rpt");
                    //Modified By Manoharan on 19Sep2007
                    rptdoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                    //add by latha for logon failed
                    //repDocUsr = rptdoc.OpenSubreport(Str)
                    //repDocUsr.Database.Tables(0).SetDataSource(im)

                    dsImain = ObjClsITCprinting.FetchInvToCustImpMain(nInvPK);
                    dsImain.ReadXmlSchema(rptPath + "\\ITCIMPMAIN.XSD");
                    CntDS = ObjClsITCprinting.FetchAirImpContainerDetails(nInvPK);
                    for (I = 0; I <= CntDS.Tables[0].Rows.Count - 1; I++)
                    {
                        if (!string.IsNullOrEmpty(Convert.ToString(CntDS.Tables[0].Rows[I]["CONTAINER"])))
                        {
                            CONTAINER = CONTAINER + CntDS.Tables[0].Rows[I]["CONTAINER"] + ",";
                        }
                    }
                    if (ObjClsITCprinting.GetBarcodeFlag("AGENT INVOICE SEA EXPORT") == "1" & ConfigurationSettings.AppSettings["ShowBarcode"] == "1")
                    {
                        Barcode = "*" + Convert.ToString(dsImain.Tables[0].Rows[0]["INVREFNO"]) + "*";
                    }
                    if ((CONTAINER.LastIndexOf(",") != -1))
                    {
                        CONTAINER = CONTAINER.Remove(CONTAINER.LastIndexOf(","), 1);
                    }
                    dsLoc = ObjClsTransportNote.FetchLocation(logged_in_loc_fk);
                    dsLoc.ReadXmlSchema(rptPath + "\\TN_Location.xsd");

                    rptdoc.OpenSubreport("AddressDetails").SetDataSource(dsLoc);
                    rptdoc.OpenSubreport("AddressDetails1").SetDataSource(dsLoc);
                    if (!string.IsNullOrEmpty(Convert.ToString(dsImain.Tables[0].Rows[0]["INVOICE_DATE"])))
                    {
                        Invoicedate = DateAndTime.DateAdd(DateInterval.Day, Convert.ToDouble(getDefault(dsImain.Tables[0].Rows[0]["PAYMENTDAYS"], 0)), Convert.ToDateTime(dsImain.Tables[0].Rows[0]["INVOICE_DATE"]));
                    }
                    rptdoc.SetParameterValue(2, Invoicedate);
                    rptdoc.SetParameterValue(3, Barcode);
                    //'surya23Nov06
                    rptdoc.SetParameterValue(0, CONTAINER);
                    rptdoc.SetParameterValue(1, 2);
                    rptdoc.SetDataSource(dsImain);
                }
                else
                {
                    rptdoc = null;
                }
                return rptdoc;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        #endregion

        //adding by thiyagarajan on 4/12/08 for introducing "Payment Due" facility in report
        #region "GetPaymentdue Invoice Report"
        public string GetPaymentdue(Int32 invpk, Int32 biztype, Int32 process)
        {
            string strSQL = null;
            WorkFlow objWK = new WorkFlow();
            DataSet dsinv = new DataSet();
            try
            {
                strSQL = " select b.credit_days,to_CHAR(inv.invoice_date,'dd/mm/yyyy')invdt  , (case when b.credit_days>0 then ";
                strSQL += " to_CHAR(inv.invoice_date+b.credit_days,'dd/mm/yyyy') end )crdate ";
                strSQL += " from job_card_trn j,booking_mst_tbl b,INV_AGENT_TBL  inv ";
                strSQL += " where inv.Inv_Agent_Pk= " + invpk;
                strSQL += " and inv.Job_Card_Fk=j.job_card_trn_pk and j.booking_mst_fk=b.booking_mst_pk(+) ";
                if (biztype == 1 | biztype == 2)
                {
                    strSQL += " and j.BUSINESS_TYPE = " + biztype;
                }
                if (process == 1 | process == 2)
                {
                    strSQL += " and j.process_type = " + process;
                }
                dsinv = objWK.GetDataSet(strSQL);
                if (dsinv.Tables[0].Rows.Count > 0)
                {
                    return Convert.ToString(getDefault(dsinv.Tables[0].Rows[0][2], ""));
                }
                else
                {
                    return "";
                }
            }
            catch (Exception ex)
            {
                return "";
            }
        }
        //end
        #endregion


        ///modified  by gangadhar for detail report instead of summery report  PTS ID: Sep-012"
        #region "invoice to CB Agent SeaExport detail Report"
        public object INVCAGExpSea(long INVPK, long logged_in_loc_fk, string mapPath, string rptPath, string Agent, string PODPK = "", string InvDate = "", int BizType = 2, int ProcessType = 1)
        {
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            clsConsolidatedInvoices objConsInv = new clsConsolidatedInvoices();
            clsInvoiceAgentSeaEntry objInvoiceEntry = new clsInvoiceAgentSeaEntry();
            cls_BlClauseForHblMbl objClsBlClause = new cls_BlClauseForHblMbl();
            CommonFeatures objrep = new CommonFeatures();
            ReportDocument rptDoc = new ReportDocument();
            DataSet DsInvDetails = new DataSet();
            DataSet DsCrInvDetailsMain = new DataSet();
            DataSet DsCrInvDetailsSub = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet DSBankDetails = new DataSet();
            DataSet dsClause = new DataSet();
            DataSet ContactDS = new DataSet();
            DataSet DsCurr = new DataSet();
            DataSet DsCrInvMain = new DataSet();
            DataSet GridDS = new DataSet();
            DataSet MainDS = new DataSet();
            Int16 i = default(Int16);
            Int32 nInvPK = Convert.ToInt32(INVPK);
            string CONTAINER = "";
            string strConsigneeLbl = "";
            string Barcode = "";
            int invPkPrint = 0;
            Int32 nParentRowCnt = default(Int32);
            DataSet FetchModiUser = null;
            DataSet DSFetchCurContDtl = null;
            DataSet DSContType = null;
            try
            {
                if (INVPK == 0)
                {
                    //invPkPrint = ViewState["InvPk"];
                }
                else
                {
                    invPkPrint = Convert.ToInt32(INVPK);
                }

                rptDoc.Load(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\RptConsolInvDetail.rpt");


                DsInvDetails = (DataSet)objInvoiceEntry.INV_DETAIL_PRINT(invPkPrint);
                if (BizType == 2 & ProcessType == 2)
                {
                    DsCrInvDetailsMain = (DataSet)objInvoiceEntry.CONSOL_INV_DETAIL_MAIN_PRINTSEAIMP(invPkPrint, Convert.ToString(HttpContext.Current.Session["USER_NAME"]), BizType, ProcessType);
                    //EXP/IMP SEA pass process type and bussiness type
                }
                else
                {
                    DsCrInvDetailsMain = (DataSet)objInvoiceEntry.CONSOL_INV_DETAIL_MAIN_PRINT(invPkPrint, Convert.ToString(HttpContext.Current.Session["USER_NAME"]), BizType, ProcessType);
                    //EXP/IMP SEA pass process type and bussiness type
                }


                DsCrInvDetailsMain.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Details.xsd");

                DsCrInvDetailsSub = (DataSet)objInvoiceEntry.CONSOL_INV_DETAIL_SUB_MAIN_PRINT(invPkPrint, BizType, ProcessType);
                DsCrInvDetailsSub.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Sub_Details.xsd");

                AddressDS = objConsInv.FetchLocation(Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\CR_Location.xsd");

                DSBankDetails = objConsInv.BankDetailsforAgent(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                DSBankDetails.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\BankDetails.xsd");

                ContactDS = objConsInv.FetchLocationNew(Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                ContactDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\ContactAddress.xsd");

                DsCurr = objConsInv.CONSOL_INV_DETAIL_CURR_MAIN_PRINT(Convert.ToString(invPkPrint), BizType, ProcessType);
                //Package Change
                DsCurr.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Curr_Details.xsd");

                DsCrInvMain = (DataSet)objInvoiceEntry.CONSOL_INV_CUST_PRINT(invPkPrint);
                DsCrInvMain.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Custumer.xsd");

                ///'''''''''''''''''''''''''''''
                FetchModiUser = objConsInv.FetchInvModifiedUserDetailsInvoice(invPkPrint);
                FetchModiUser.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\FetchModiUser.XSD");

                DSFetchCurContDtl = objConsInv.FetchCurContSummaryInvoiceToAgent(invPkPrint);
                DSFetchCurContDtl.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\CurSummary.XSD");

                DSContType = objConsInv.FetchContCountInvoiceToAgent(invPkPrint);
                DSContType.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\ContCount.XSD");

                ///'''''''''''''''''''''''''''''''''''''''''''''''''''''''

                string Flag = null;
                int ClauseTypeFlag = 0;
                if ((Convert.ToString(ProcessType) == "1" & BizType == 1 & Agent == "CB"))
                {
                    ClauseTypeFlag = 9;
                    Flag = "AirCBExp";
                }
                else if ((ProcessType == 1 & BizType == 1 & Agent == "DP"))
                {
                    ClauseTypeFlag = 11;
                    Flag = "AirDPExp";
                }
                else if ((ProcessType == 1 & BizType == 2 & Agent == "CB"))
                {
                    ClauseTypeFlag = 9;
                    Flag = "SeaCBExp";
                }
                else if ((ProcessType == 1 & BizType == 2 & Agent == "DP"))
                {
                    ClauseTypeFlag = 11;
                    Flag = "SeaDPExp";
                }
                else if ((ProcessType == 2 & BizType == 2 & Agent == "CB"))
                {
                    ClauseTypeFlag = 9;
                    Flag = "SeaCBImp";
                }
                else if ((ProcessType == 2 & BizType == 2 & Agent == "LA"))
                {
                    ClauseTypeFlag = 10;
                    Flag = "SeaLAImp";
                }
                else if ((ProcessType == 2 & BizType == 1 & Agent == "CB"))
                {
                    ClauseTypeFlag = 9;
                    Flag = "AirCBImp";
                }
                else
                {
                    ClauseTypeFlag = 10;
                    Flag = "AirLAImp";
                }

                GridDS = objClsBlClause.FetchBlClausesForHBL("", ClauseTypeFlag, 1, 1, ((PODPK == null) ? "0" : PODPK), "0", invPkPrint, InvDate, Flag);
                GridDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\PrintBLClause.XSD");

                if (objInvoiceEntry.GetBarcodeFlag("AGENT INVOICE SEA EXPORT") == "1" & ConfigurationSettings.AppSettings["ShowBarcode"] == "1")
                {
                    Barcode = "*" + (string.IsNullOrEmpty(Convert.ToString(DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"])) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"]) + "*";
                }

                rptDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                rptDoc.OpenSubreport("Cr_Location").SetDataSource(AddressDS);
                rptDoc.OpenSubreport("RptCustDetails").SetDataSource(DsCrInvMain);
                rptDoc.OpenSubreport("RptSubDetails").SetDataSource(DsCrInvDetailsSub);
                rptDoc.OpenSubreport("RptBankDetails").SetDataSource(DSBankDetails);
                rptDoc.OpenSubreport("RptContactAddress").SetDataSource(ContactDS);
                rptDoc.OpenSubreport("RptCurrency").SetDataSource(DsCurr);
                rptDoc.OpenSubreport("rptBLClause").SetDataSource(GridDS);
                rptDoc.OpenSubreport("FetchModiUser").SetDataSource(FetchModiUser);
                rptDoc.OpenSubreport("CurSummary").SetDataSource(DSFetchCurContDtl);
                rptDoc.OpenSubreport("ContCount").SetDataSource(DSContType);

                DataSet DSCargoType = null;
                DSCargoType = objConsInv.FetchCargoTypeIVG(invPkPrint);
                int CargoType = Convert.ToInt32(DSCargoType.Tables[0].Rows[0]["CARGO_TYPE"]);

                rptDoc.SetDataSource(DsCrInvDetailsMain);

                if (DsInvDetails.Tables[0].Rows.Count > 0)
                {
                    //'rptDoc.SetParameterValue(3, InvoiceNo)
                    //'rptDoc.SetParameterValue(4, Currency)
                    //'rptDoc.SetParameterValue(5, Invtax)
                    //'rptDoc.SetParameterValue(6, Discount)
                    //'rptDoc.SetParameterValue(8, NetAmt)
                    //'rptDoc.SetParameterValue(9, InvDate)
                    //'rptDoc.SetParameterValue("InvAmt", InvAmt)
                    //'rptDoc.SetParameterValue("Remarks", Remarks)

                    rptDoc.SetParameterValue("InvoceNr", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"]));
                    rptDoc.SetParameterValue("BaseCurrency", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["CURRENCY_ID"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["CURRENCY_ID"]));
                    rptDoc.SetParameterValue("tax", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["TAX_AMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["TAX_AMT"]));
                    rptDoc.SetParameterValue("Discount1", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["DICSOUNT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["DICSOUNT"]));
                    rptDoc.SetParameterValue("Net Invoice Amt", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["NET_INV_AMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["NET_INV_AMT"]));
                    rptDoc.SetParameterValue("Date1", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_DATE"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_DATE"]));
                    rptDoc.SetParameterValue("Due_Date", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_DUE_DATE"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_DUE_DATE"]));
                    rptDoc.SetParameterValue("InvAmt", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["TOTAMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["TOTAMT"]));
                    rptDoc.SetParameterValue("Remarks", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["REMARKS"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["REMARKS"]));
                }
                rptDoc.SetParameterValue("Invoice type", "");
                rptDoc.SetParameterValue("BizType", BizType);
                rptDoc.SetParameterValue("Process", ProcessType);
                rptDoc.SetParameterValue("ApplyBarcode", Barcode);
                rptDoc.SetParameterValue("UniqueRefNr", "");
                rptDoc.SetParameterValue("CargoType", CargoType);
                rptDoc.SetParameterValue("PrtHdr", true);
                string duedate = null;
                duedate = GetPaymentdue(Convert.ToInt32(INVPK), 2, 1);
                duedate = (!string.IsNullOrEmpty(getDefault(duedate, "").ToString()) ? duedate : "Immediate");
                rptDoc.SetParameterValue("Paydue", duedate);
                if (DsCrInvDetailsMain.Tables[0].Rows.Count > 0)
                {
                    rptDoc.SetParameterValue("Vessel", (string.IsNullOrEmpty(DsCrInvDetailsMain.Tables[0].Rows[0]["VSL_VOY"].ToString()) ? "TBA" : DsCrInvDetailsMain.Tables[0].Rows[0]["VSL_VOY"]));
                }
                else
                {
                    rptDoc.SetParameterValue("Vessel", "TBA");
                }
                rptDoc.SetParameterValue("PS_AMT", "0.00");
                rptDoc.SetParameterValue("DISCOUNT_AMT", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["DICSOUNT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["DICSOUNT"]));
                objrep.getReportControls(rptDoc, "QFOR4078", 5);
                objrep.getReportControls(rptDoc, "QFOR4078", 1);
                return rptDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Invoice to ExpAir for Detail Report"
        public object INVCAGExpAir(long INVPK, long logged_in_loc_fk, string mapPath, string rptPath, string agentType, string PODPK = "", string InvDate = "")
        {
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            clsConsolidatedInvoices objConsInv = new clsConsolidatedInvoices();
            clsInvoiceAgentSeaEntry objInvoiceEntry = new clsInvoiceAgentSeaEntry();
            clsInvoiceAgentEntryAir objInvoiceEXPAir = new clsInvoiceAgentEntryAir();
            cls_BlClauseForHblMbl objClsBlClause = new cls_BlClauseForHblMbl();
            CommonFeatures objrep = new CommonFeatures();
            ReportDocument rptDoc = new ReportDocument();
            DataSet DsInvDetails = new DataSet();
            DataSet DsCrInvDetailsMain = new DataSet();
            DataSet DsCrInvDetailsSub = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet DSBankDetails = new DataSet();
            DataSet dsClause = new DataSet();
            DataSet ContactDS = new DataSet();
            DataSet DsCurr = new DataSet();
            DataSet DsCrInvMain = new DataSet();
            DataSet GridDS = new DataSet();
            DataSet MainDS = new DataSet();
            Int16 i = default(Int16);
            Int32 nInvPK = Convert.ToInt32(INVPK);
            string CONTAINER = "";
            string strConsigneeLbl = "";
            string Barcode = "";
            int invPkPrint = 0;
            Int32 nParentRowCnt = default(Int32);
            DataSet FetchModiUser = null;
            DataSet DSFetchCurContDtl = null;
            DataSet DSContType = null;
            try
            {
                if (INVPK == 0)
                {
                    //invPkPrint = ViewState["InvPk"];
                }
                else
                {
                    invPkPrint = Convert.ToInt32(INVPK);
                }

                rptDoc.Load(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\RptConsolInvDetail.rpt");
                rptDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

                DsInvDetails = (DataSet)objInvoiceEXPAir.INV_DETAIL_PRINT(invPkPrint);

                DsCrInvDetailsMain = objInvoiceEXPAir.CONSOL_INV_DETAIL_MAIN_PRINT(invPkPrint, Convert.ToString(HttpContext.Current.Session["USER_NAME"]));
                DsCrInvDetailsMain.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Details.xsd");

                DsCrInvDetailsSub = objInvoiceEXPAir.CONSOL_INV_DETAIL_SUB_MAIN_PRINT(invPkPrint);
                DsCrInvDetailsSub.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Sub_Details.xsd");

                AddressDS = objConsInv.FetchLocation(Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\CR_Location.xsd");

                DSBankDetails = objConsInv.BankDetails(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                DSBankDetails.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\BankDetails.xsd");

                ContactDS = objConsInv.FetchLocation(Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                ContactDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\ContactAddress.xsd");

                DsCurr = objConsInv.CONSOL_INV_DETAIL_CURR_MAIN_PRINT(Convert.ToString(invPkPrint), 2, 1);
                DsCurr.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Curr_Details.xsd");

                DsCrInvMain = objInvoiceEXPAir.CONSOL_INV_CUST_PRINT(invPkPrint);
                DsCrInvMain.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Custumer.xsd");
                ///'''''''''''''''''''''''''''''

                FetchModiUser = objConsInv.FetchInvModifiedUserDetails(invPkPrint);
                FetchModiUser.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\FetchModiUser.XSD");

                DSFetchCurContDtl = objConsInv.FetchCurContSummary(invPkPrint);
                DSFetchCurContDtl.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\CurSummary.XSD");

                DSContType = objConsInv.FetchContCount(invPkPrint);
                DSContType.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\ContCount.XSD");

                ///'''''''''''''''''''''''''''''''''''''''''''''''''''''''
                //GridDS = objClsBlClause.FetchBlClausesForHBL("", 9, 1, 1, "0", invPkPrint)
                string Flag = null;
                int ClauseTypeFlag = 0;
                if (agentType == "CB" | agentType == "1")
                {
                    ClauseTypeFlag = 9;
                    Flag = "AirCBExp";
                }
                else
                {
                    ClauseTypeFlag = 11;
                    Flag = "AirDPExp";
                }

                GridDS = objClsBlClause.FetchBlClausesForHBL("", ClauseTypeFlag, 1, 1, ((PODPK == null) ? "0" : PODPK), "0", invPkPrint, InvDate, Flag);
                GridDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\PrintBLClause.XSD");

                if (objInvoiceEntry.GetBarcodeFlag("AGENT INVOICE AIR EXPORT") == "1" & ConfigurationSettings.AppSettings["ShowBarcode"] == "1")
                {
                    Barcode = "*" + (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"]) + "*";
                }


                rptDoc.OpenSubreport("Cr_Location").SetDataSource(AddressDS);
                rptDoc.OpenSubreport("RptCustDetails").SetDataSource(DsCrInvMain);
                rptDoc.OpenSubreport("RptSubDetails").SetDataSource(DsCrInvDetailsSub);
                rptDoc.OpenSubreport("RptBankDetails").SetDataSource(DSBankDetails);
                rptDoc.OpenSubreport("RptContactAddress").SetDataSource(ContactDS);
                rptDoc.OpenSubreport("RptCurrency").SetDataSource(DsCurr);
                rptDoc.OpenSubreport("rptBLClause").SetDataSource(GridDS);
                rptDoc.OpenSubreport("rptBLClause").SetDataSource(GridDS);
                rptDoc.OpenSubreport("FetchModiUser").SetDataSource(FetchModiUser);
                rptDoc.OpenSubreport("CurSummary").SetDataSource(DSFetchCurContDtl);
                rptDoc.OpenSubreport("ContCount").SetDataSource(DSContType);
                rptDoc.SetDataSource(DsCrInvDetailsMain);


                if (DsInvDetails.Tables[0].Rows.Count > 0)
                {
                    rptDoc.SetParameterValue("InvoceNr", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"]));
                    rptDoc.SetParameterValue("BaseCurrency", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["CURRENCY_ID"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["CURRENCY_ID"]));
                    rptDoc.SetParameterValue("tax", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["TAX_AMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["TAX_AMT"]));
                    rptDoc.SetParameterValue("Discount1", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["DICSOUNT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["DICSOUNT"]));
                    rptDoc.SetParameterValue(8, (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["NET_INV_AMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["NET_INV_AMT"]));
                    rptDoc.SetParameterValue("Date1", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_DATE"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_DATE"]));
                    rptDoc.SetParameterValue("Due_Date", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_DUE_DATE"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_DUE_DATE"]));
                    rptDoc.SetParameterValue("InvAmt", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["TOTAMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["TOTAMT"]));
                    rptDoc.SetParameterValue("Remarks", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["REMARKS"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["REMARKS"]));
                }

                rptDoc.SetParameterValue(7, "");
                rptDoc.SetParameterValue("BizType", 1);
                rptDoc.SetParameterValue("Process", 1);
                rptDoc.SetParameterValue("ApplyBarcode", Barcode);
                rptDoc.SetParameterValue("UniqueRefNr", "");
                rptDoc.SetParameterValue("PrtHdr", true);
                string duedate = null;
                duedate = GetPaymentdue(Convert.ToInt32(INVPK), 2, 1);
                duedate = (!string.IsNullOrEmpty(Convert.ToString(getDefault(duedate, ""))) ? duedate : "Immediate");
                rptDoc.SetParameterValue("Paydue", duedate);

                if (DsCrInvDetailsMain.Tables[0].Rows.Count > 0)
                {
                    rptDoc.SetParameterValue("Vessel", (string.IsNullOrEmpty(DsCrInvDetailsMain.Tables[0].Rows[0]["VSL_VOY"].ToString()) ? "TBA" : DsCrInvDetailsMain.Tables[0].Rows[0]["VSL_VOY"]));
                }
                else
                {
                    rptDoc.SetParameterValue("Vessel", "TBA");
                }
                rptDoc.SetParameterValue("PS_AMT", "0.00");
                objrep.getReportControls(rptDoc, "QFOR4078", 5);
                objrep.getReportControls(rptDoc, "QFOR4078", 1);
                return rptDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Invoice to ImpSea for Detail Report"
        public object INVCAGImpSea(long INVPK, long logged_in_loc_fk, string mapPath, string rptPath, string agentType, string PODPK = "", string InvDate = "")
        {
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            clsConsolidatedInvoices objConsInv = new clsConsolidatedInvoices();
            clsInvoiceAgentSeaImpEntry objInvoiceImpSea = new clsInvoiceAgentSeaImpEntry();
            clsInvoiceAgentSeaEntry objInvoiceEntry = new clsInvoiceAgentSeaEntry();
            cls_BlClauseForHblMbl objClsBlClause = new cls_BlClauseForHblMbl();
            CommonFeatures objrep = new CommonFeatures();
            ReportDocument rptDoc = new ReportDocument();
            DataSet DsInvDetails = new DataSet();
            DataSet DsCrInvDetailsMain = new DataSet();
            DataSet DsCrInvDetailsSub = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet DSBankDetails = new DataSet();
            DataSet dsClause = new DataSet();
            DataSet ContactDS = new DataSet();
            DataSet DsCurr = new DataSet();
            DataSet DsCrInvMain = new DataSet();
            DataSet GridDS = new DataSet();
            DataSet MainDS = new DataSet();
            Int16 i = default(Int16);
            Int32 nInvPK = Convert.ToInt32(INVPK);
            string CONTAINER = "";
            string strConsigneeLbl = "";
            string Barcode = "";
            int invPkPrint = 0;
            Int32 nParentRowCnt = default(Int32);
            try
            {
                if (INVPK == 0)
                {
                    //invPkPrint = ViewState["InvPk"];
                }
                else
                {
                    invPkPrint = Convert.ToInt32(INVPK);
                }

                rptDoc.Load(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\RptConsolInvDetail.rpt");


                DsInvDetails = (DataSet)objInvoiceImpSea.INV_DETAIL_PRINT(invPkPrint);

                DsCrInvDetailsMain = objInvoiceImpSea.CONSOL_INV_DETAIL_MAIN_PRINT(invPkPrint, Convert.ToString(HttpContext.Current.Session["USER_NAME"]));
                DsCrInvDetailsMain.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Details.xsd");

                DsCrInvDetailsSub = objInvoiceImpSea.CONSOL_INV_DETAIL_SUB_MAIN_PRINT(invPkPrint);
                DsCrInvDetailsSub.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Sub_Details.xsd");

                AddressDS = objConsInv.FetchLocation(Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\CR_Location.xsd");

                DSBankDetails = objConsInv.BankDetails(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                DSBankDetails.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\BankDetails.xsd");

                ContactDS = objConsInv.FetchLocation(Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                ContactDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\ContactAddress.xsd");

                DsCurr = objConsInv.CONSOL_INV_DETAIL_CURR_MAIN_PRINT(Convert.ToString(invPkPrint), 2, 1);
                DsCurr.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Curr_Details.xsd");

                DsCrInvMain = objInvoiceImpSea.CONSOL_INV_CUST_PRINT(invPkPrint);
                DsCrInvMain.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Custumer.xsd");

                //GridDS = objClsBlClause.FetchBlClausesForHBL("", 9, 1, 1, "0", invPkPrint)
                string Flag = null;
                int ClauseTypeFlag = 0;
                if (agentType == "CB" | agentType == "1")
                {
                    ClauseTypeFlag = 9;
                    Flag = "SeaCBImp";
                }
                else
                {
                    ClauseTypeFlag = 10;
                    Flag = "SeaLAImp";
                }

                GridDS = objClsBlClause.FetchBlClausesForHBL("", ClauseTypeFlag, 1, 1, ((PODPK == null) ? "0" : PODPK), "0", invPkPrint, InvDate, Flag);
                GridDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\PrintBLClause.XSD");

                if (objInvoiceEntry.GetBarcodeFlag("AGENT INVOICE SEA IMPORT") == "1" & ConfigurationSettings.AppSettings["ShowBarcode"] == "1")
                {
                    Barcode = "*" + (string.IsNullOrEmpty(Convert.ToString(DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"])) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"]) + "*";
                }

                rptDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                rptDoc.OpenSubreport("Cr_Location").SetDataSource(AddressDS);
                rptDoc.OpenSubreport("RptCustDetails").SetDataSource(DsCrInvMain);
                rptDoc.OpenSubreport("RptSubDetails").SetDataSource(DsCrInvDetailsSub);
                rptDoc.OpenSubreport("RptBankDetails").SetDataSource(DSBankDetails);
                rptDoc.OpenSubreport("RptContactAddress").SetDataSource(ContactDS);
                rptDoc.OpenSubreport("RptCurrency").SetDataSource(DsCurr);
                rptDoc.OpenSubreport("rptBLClause").SetDataSource(GridDS);
                rptDoc.SetDataSource(DsCrInvDetailsMain);
                if (DsInvDetails.Tables[0].Rows.Count > 0)
                {
                    rptDoc.SetParameterValue("InvoceNr", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"]));
                    rptDoc.SetParameterValue("BaseCurrency", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["CURRENCY_ID"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["CURRENCY_ID"]));
                    rptDoc.SetParameterValue("tax", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["TAX_AMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["TAX_AMT"]));
                    rptDoc.SetParameterValue("Discount1", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["DICSOUNT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["DICSOUNT"]));
                    rptDoc.SetParameterValue(8, (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["NET_INV_AMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["NET_INV_AMT"]));
                    rptDoc.SetParameterValue("Date1", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_DATE"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_DATE"]));
                    rptDoc.SetParameterValue("Due_Date", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_DUE_DATE"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_DUE_DATE"]));
                    rptDoc.SetParameterValue("InvAmt", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["TOTAMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["TOTAMT"]));
                    rptDoc.SetParameterValue("Remarks", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["REMARKS"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["REMARKS"]));
                }

                rptDoc.SetParameterValue(7, "");
                rptDoc.SetParameterValue("BizType", 2);
                rptDoc.SetParameterValue("Process", 2);
                rptDoc.SetParameterValue("ApplyBarcode", Barcode);
                rptDoc.SetParameterValue("UniqueRefNr", "");
                rptDoc.SetParameterValue("PrtHdr", true);
                string duedate = null;
                duedate = GetPaymentdue(Convert.ToInt32(INVPK), 2, 1);
                duedate = (!string.IsNullOrEmpty(Convert.ToString(getDefault(duedate, ""))) ? duedate : "Immediate");
                rptDoc.SetParameterValue("Paydue", duedate);

                if (DsCrInvDetailsMain.Tables[0].Rows.Count > 0)
                {
                    rptDoc.SetParameterValue("Vessel", (string.IsNullOrEmpty(DsCrInvDetailsMain.Tables[0].Rows[0]["VSL_VOY"].ToString()) ? "TBA" : DsCrInvDetailsMain.Tables[0].Rows[0]["VSL_VOY"]));
                }
                else
                {
                    rptDoc.SetParameterValue("Vessel", "TBA");
                }
                rptDoc.SetParameterValue("PS_AMT", "0.00");
                objrep.getReportControls(rptDoc, "QFOR4078", 5);
                objrep.getReportControls(rptDoc, "QFOR4078", 1);
                return rptDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Invoice to ImpAir for Detail Report"
        public object INVCAGImpAir(long INVPK, long logged_in_loc_fk, string mapPath, string rptPath, string agentType, string PODPK = "", string InvDate = "")
        {
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            clsConsolidatedInvoices objConsInv = new clsConsolidatedInvoices();
            clsInvoiceAgentSeaEntry objInvoiceEntry = new clsInvoiceAgentSeaEntry();
            clsInvoiceAgentImpEntryAir objInvoiceImpAir = new clsInvoiceAgentImpEntryAir();
            cls_BlClauseForHblMbl objClsBlClause = new cls_BlClauseForHblMbl();
            CommonFeatures objrep = new CommonFeatures();
            ReportDocument rptDoc = new ReportDocument();
            DataSet DsInvDetails = new DataSet();
            DataSet DsCrInvDetailsMain = new DataSet();
            DataSet DsCrInvDetailsSub = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet DSBankDetails = new DataSet();
            DataSet dsClause = new DataSet();
            DataSet ContactDS = new DataSet();
            DataSet DsCurr = new DataSet();
            DataSet DsCrInvMain = new DataSet();
            DataSet GridDS = new DataSet();
            DataSet MainDS = new DataSet();
            Int16 i = default(Int16);
            Int32 nInvPK = Convert.ToInt32(INVPK);
            string CONTAINER = "";
            string strConsigneeLbl = "";
            string Barcode = "";
            int invPkPrint = 0;
            Int32 nParentRowCnt = default(Int32);
            try
            {
                if (INVPK == 0)
                {
                    //invPkPrint = ViewState["InvPk"];
                }
                else
                {
                    invPkPrint = Convert.ToInt32(INVPK);
                }

                rptDoc.Load(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\RptConsolInvDetail.rpt");
                rptDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

                DsInvDetails = (DataSet)objInvoiceImpAir.INV_DETAIL_PRINT(invPkPrint);

                DsCrInvDetailsMain = objInvoiceImpAir.CONSOL_INV_DETAIL_MAIN_PRINT(invPkPrint, Convert.ToString(HttpContext.Current.Session["USER_NAME"]));
                DsCrInvDetailsMain.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Details.xsd");

                DsCrInvDetailsSub = objInvoiceImpAir.CONSOL_INV_DETAIL_SUB_MAIN_PRINT(invPkPrint);
                DsCrInvDetailsSub.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Sub_Details.xsd");

                AddressDS = objConsInv.FetchLocation(Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\CR_Location.xsd");

                DSBankDetails = objConsInv.BankDetails(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                DSBankDetails.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\BankDetails.xsd");

                ContactDS = objConsInv.FetchLocation(Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                ContactDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\ContactAddress.xsd");

                DsCurr = objConsInv.CONSOL_INV_DETAIL_CURR_MAIN_PRINT(Convert.ToString(invPkPrint), 1, 2);
                DsCurr.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Curr_Details.xsd");

                DsCrInvMain = objInvoiceImpAir.CONSOL_INV_CUST_PRINT(invPkPrint);
                DsCrInvMain.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Custumer.xsd");

                //GridDS = objClsBlClause.FetchBlClausesForHBL("", 9, 1, 1, "0", invPkPrint)
                string Flag = null;
                int ClauseTypeFlag = 0;
                if (agentType == "CB" | agentType == "1")
                {
                    ClauseTypeFlag = 9;
                    Flag = "AirCBImp";
                }
                else
                {
                    ClauseTypeFlag = 10;
                    Flag = "AirLAImp";
                }
                GridDS = objClsBlClause.FetchBlClausesForHBL("", ClauseTypeFlag, 1, 1, ((PODPK == null) ? "0" : PODPK), "0", invPkPrint, InvDate, Flag);
                GridDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\PrintBLClause.XSD");

                if (objInvoiceEntry.GetBarcodeFlag("AGENT INVOICE AIR IMPORT") == "1" & ConfigurationSettings.AppSettings["ShowBarcode"] == "1")
                {
                    Barcode = "*" + (string.IsNullOrEmpty(Convert.ToString(DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"])) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"]) + "*";
                }


                rptDoc.OpenSubreport("Cr_Location").SetDataSource(AddressDS);
                rptDoc.OpenSubreport("RptCustDetails").SetDataSource(DsCrInvMain);
                rptDoc.OpenSubreport("RptSubDetails").SetDataSource(DsCrInvDetailsSub);
                rptDoc.OpenSubreport("RptBankDetails").SetDataSource(DSBankDetails);
                rptDoc.OpenSubreport("RptContactAddress").SetDataSource(ContactDS);
                rptDoc.OpenSubreport("RptCurrency").SetDataSource(DsCurr);
                rptDoc.OpenSubreport("rptBLClause").SetDataSource(GridDS);
                rptDoc.SetDataSource(DsCrInvDetailsMain);
                if (DsInvDetails.Tables[0].Rows.Count > 0)
                {
                    rptDoc.SetParameterValue("InvoceNr", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_REF_NO"]));
                    rptDoc.SetParameterValue("BaseCurrency", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["CURRENCY_ID"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["CURRENCY_ID"]));
                    rptDoc.SetParameterValue("tax", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["TAX_AMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["TAX_AMT"]));
                    rptDoc.SetParameterValue("Discount1", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["DICSOUNT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["DICSOUNT"]));
                    rptDoc.SetParameterValue(8, (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["NET_INV_AMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["NET_INV_AMT"]));
                    rptDoc.SetParameterValue("Date1", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_DATE"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_DATE"]));
                    rptDoc.SetParameterValue("Due_Date", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["INVOICE_DUE_DATE"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["INVOICE_DUE_DATE"]));
                    rptDoc.SetParameterValue("InvAmt", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["TOTAMT"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["TOTAMT"]));
                    rptDoc.SetParameterValue("Remarks", (string.IsNullOrEmpty(DsInvDetails.Tables[0].Rows[0]["REMARKS"].ToString()) ? "" : DsInvDetails.Tables[0].Rows[0]["REMARKS"]));
                }

                rptDoc.SetParameterValue(7, "");
                rptDoc.SetParameterValue("BizType", 1);
                rptDoc.SetParameterValue("Process", 2);
                rptDoc.SetParameterValue("ApplyBarcode", Barcode);
                rptDoc.SetParameterValue("UniqueRefNr", "");
                rptDoc.SetParameterValue("PrtHdr", true);

                string duedate = null;
                duedate = GetPaymentdue(Convert.ToInt32(INVPK), 1, 2);
                duedate = (!string.IsNullOrEmpty(Convert.ToString(getDefault(duedate, ""))) ? duedate : "Immediate");
                rptDoc.SetParameterValue("Paydue", duedate);

                if (DsCrInvDetailsMain.Tables[0].Rows.Count > 0)
                {
                    rptDoc.SetParameterValue("Vessel", (string.IsNullOrEmpty(DsCrInvDetailsMain.Tables[0].Rows[0]["VSL_VOY"].ToString()) ? "TBA" : DsCrInvDetailsMain.Tables[0].Rows[0]["VSL_VOY"]));
                }
                else
                {
                    rptDoc.SetParameterValue("Vessel", "TBA");
                }
                rptDoc.SetParameterValue("PS_AMT", "0.00");
                objrep.getReportControls(rptDoc, "QFOR4078", 5);
                objrep.getReportControls(rptDoc, "QFOR4078", 1);
                return rptDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Credit Note to Agent Common Query"
        public DataSet FetchCRNReportData(long strPK, short AgentFlag = 1, int BIZ = 2, int Process = 1)
        {
            WorkFlow objWK = new WorkFlow();
            //AgentFlag = 1 = CB Agent
            //AgentFlag = 2 = DP Agent 

            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT CRSTBL.CR_AGENT_PK CBPK,");
            sb.Append("       CRSTBL.CREDIT_NOTE_REF_NO CRREFNO,");
            sb.Append("       CRSTBL.CREDIT_NOTE_DATE  CRDATE,");
            sb.Append("       CRCURR.CURRENCY_ID CRCURR_ID,");
            sb.Append("       CRCURR.CURRENCY_NAME CRCURR_NAME,");
            sb.Append("       NVL(CRSTBL.CREDIT_NOTE_AMT,0) CRAMT,");
            sb.Append("       INVAGTEXP.INV_AGENT_PK INVPK,");
            sb.Append("       INVAGTEXP.INVOICE_REF_NO INVREFNO,");
            sb.Append("       INVAGTEXP.INVOICE_DATE INVDATE,");
            sb.Append("       NVL(INVAGTEXP.GROSS_INV_AMT,0)  GROSSAMOUNT,");
            sb.Append("       (SELECT SUM(NVL(INVT.TAX_AMT, 0)) ");
            sb.Append("          FROM INV_AGENT_TRN_TBL INVT ");
            sb.Append("         WHERE INVT.INV_AGENT_FK = INVAGTEXP.INV_AGENT_PK) VATAMOUNT,");
            //sb.Append("       INVAGTEXP.VAT_AMT VATAMOUNT,")
            sb.Append("       NVL(INVAGTEXP.DISCOUNT_AMT,0) DISCAMOUNT,");
            sb.Append("       INVCURR.CURRENCY_ID CURRID,");
            sb.Append("       INVCURR.CURRENCY_NAME CURRNAME,");
            sb.Append("       NVL(INVAGTEXP.NET_INV_AMT,0) AMOUNT,");
            sb.Append("       JSE.JOB_CARD_TRN_PK JOBPK,");
            sb.Append("       JSE.JOBCARD_REF_NO JOBREFNO,");
            sb.Append("       JSE.JOBCARD_DATE JOBDATE,");
            sb.Append("       AMST.AGENT_NAME AGENTNAME,");
            sb.Append("       AMST.ACCOUNT_NO AGENTREFNO,");
            sb.Append("       ADTLS.ADM_ADDRESS_1 AGENTADD1,");
            sb.Append("       ADTLS.ADM_ADDRESS_2 AGENTADD2,");
            sb.Append("       ADTLS.ADM_ADDRESS_3 AGENTADD3,");
            sb.Append("       ADTLS.ADM_CITY AGENTCITY,");
            sb.Append("       ADTLS.ADM_ZIP_CODE AGENTZIP,");
            sb.Append("       ADTLS.ADM_PHONE_NO_1 AGENTPHONE,");
            sb.Append("       ADTLS.ADM_FAX_NO AGENTFAX,");
            sb.Append("       ADTLS.ADM_EMAIL_ID AGENTEMAIL,");
            sb.Append("       AGTCOUNTRY.COUNTRY_NAME AGENTCOUNTRY,");
            sb.Append("       AMST.VAT_NO AGTVATNO,");
            sb.Append("       CRSTBL.REMARKS");
            sb.Append("       FROM CR_AGENT_TBL    CRSTBL,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CRCURR,    ");
            sb.Append("       INV_AGENT_TBL   INVAGTEXP,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   INVCURR,");
            sb.Append("       JOB_CARD_TRN    JSE,");
            sb.Append("       JOB_TRN_CONT    JSEC,");
            sb.Append("       AGENT_MST_TBL           AMST,");
            sb.Append("       AGENT_CONTACT_DTLS      ADTLS,");
            sb.Append("       COUNTRY_MST_TBL         AGTCOUNTRY ");
            sb.Append(" WHERE CRSTBL.INV_AGENT_FK = INVAGTEXP.INV_AGENT_PK");
            sb.Append("   AND INVAGTEXP.JOB_CARD_FK = JSE.JOB_CARD_TRN_PK");
            sb.Append("   AND INVCURR.CURRENCY_MST_PK(+) = INVAGTEXP.CURRENCY_MST_FK");
            sb.Append("   AND JSE.JOB_CARD_TRN_PK = JSEC.JOB_CARD_TRN_FK");
            sb.Append("   AND AMST.AGENT_MST_PK = CRSTBL.AGENT_MST_FK");
            sb.Append("   AND ADTLS.AGENT_MST_FK(+) = AMST.AGENT_MST_PK");
            sb.Append("   AND AGTCOUNTRY.COUNTRY_MST_PK(+) = ADTLS.ADM_COUNTRY_MST_FK");
            sb.Append("   AND CRSTBL.CURRENCY_MST_FK = CRCURR.CURRENCY_MST_PK");
            sb.Append("   AND CRSTBL.CR_AGENT_PK = " + strPK);
            sb.Append(" GROUP BY CRSTBL.CR_AGENT_PK,");
            sb.Append("          CRSTBL.CREDIT_NOTE_REF_NO,");
            sb.Append("          CRSTBL.CREDIT_NOTE_DATE,");
            sb.Append("          CRCURR.CURRENCY_ID,");
            sb.Append("          CRCURR.CURRENCY_NAME,");
            sb.Append("          CRSTBL.CREDIT_NOTE_AMT,");
            sb.Append("          INVAGTEXP.INV_AGENT_PK,");
            sb.Append("          INVAGTEXP.INVOICE_REF_NO,");
            sb.Append("          INVAGTEXP.INVOICE_DATE,");
            sb.Append("          INVAGTEXP.GROSS_INV_AMT,");
            sb.Append("          INVAGTEXP.VAT_AMT,");
            sb.Append("          INVAGTEXP.DISCOUNT_AMT,");
            sb.Append("          INVAGTEXP.NET_INV_AMT,");
            sb.Append("          JSE.JOB_CARD_TRN_PK,");
            sb.Append("          JSE.JOBCARD_REF_NO,");
            sb.Append("          JSE.JOBCARD_DATE,");
            sb.Append("          AMST.AGENT_NAME,");
            sb.Append("          AMST.ACCOUNT_NO,");
            sb.Append("          ADTLS.ADM_ADDRESS_1,");
            sb.Append("          ADTLS.ADM_ADDRESS_2,");
            sb.Append("          ADTLS.ADM_ADDRESS_3,");
            sb.Append("          ADTLS.ADM_CITY,");
            sb.Append("          ADTLS.ADM_ZIP_CODE,");
            sb.Append("          ADTLS.ADM_PHONE_NO_1,");
            sb.Append("          ADTLS.ADM_FAX_NO,");
            sb.Append("          ADTLS.ADM_EMAIL_ID,");
            sb.Append("          AGTCOUNTRY.COUNTRY_NAME,");
            sb.Append("          INVCURR.CURRENCY_ID,");
            sb.Append("          INVCURR.CURRENCY_NAME,");
            sb.Append("          AMST.VAT_NO,");
            sb.Append("          CRSTBL.REMARKS");
            try
            {
                string query = sb.ToString();
                if (BIZ == 1)
                {
                    query = query.Replace("SEA", "AIR");
                }
                if (Process == 2)
                {
                    query = query.Replace("EXP", "IMP");
                }
                return objWK.GetDataSet(query);
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

        ///Added By Koteshwari on 2/2/2011
        #region "Fetch CargoType"
        public DataSet FetchCargoType(string Jobpk, int Process)
        {
            WorkFlow Objwk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT");
            sb.Append("  JC.JOB_CARD_TRN_PK, NVL(JC.CARGO_TYPE,0)CARGO_TYPE");
            sb.Append("  FROM JOB_CARD_TRN JC, BOOKING_MST_TBL BKG");
            sb.Append(" WHERE JC.BOOKING_MST_FK = BKG.BOOKING_MST_PK(+) ");
            sb.Append(" AND JC.JOB_CARD_TRN_PK=" + Jobpk + " ");
            try
            {
                return Objwk.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }
        #endregion
        ///End

        public DataSet FetchBookingStatus(string BkgPk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = " SELECT BK.STATUS FROM BOOKING_MST_TBL BK ";
            Strsql +=  " WHERE  BK.BOOKING_MST_PK IN(" + BkgPk + ")";

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01)
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
                throw sqlExp;
            }
        }

        #region "Print Acrobat"
        private void PrintInAcrobat(ReportDocument rptDoc, string filename, int StrSetPrint)
        {
            string Fname = null;
            ExportOptions crExportOptions = null;
            DiskFileDestinationOptions crDiskFileDestinationOptions = null;
            rptDoc.PrintOptions.PaperSize = PaperSize.PaperA4;
            if (StrSetPrint == 1)
            {
                rptDoc.PrintOptions.PaperOrientation = PaperOrientation.Portrait;
            }
            else
            {
                rptDoc.PrintOptions.PaperOrientation = PaperOrientation.Landscape;
            }
            //Fname = HttpContext.Current.Server.MapPath(".") & "\Files_Uploaded\" & filename & ".pdf"
            Fname = HttpContext.Current.Server.MapPath("..\\..\\") + "Supports\\Files_Uploaded\\" + filename + ".pdf";
            crDiskFileDestinationOptions = new DiskFileDestinationOptions();
            crDiskFileDestinationOptions.DiskFileName = Fname;
            crExportOptions = rptDoc.ExportOptions;
            var _with8 = crExportOptions;
            _with8.DestinationOptions = crDiskFileDestinationOptions;
            _with8.ExportDestinationType = ExportDestinationType.DiskFile;
            _with8.ExportFormatType = ExportFormatType.PortableDocFormat;
            rptDoc.Export();
        }
        #endregion

        #region "CommInvoice/Package Report"
        // Private Sub CommInvoicePackage()
        public object CommInvoicePackage(string BookingPK, string CommInvNum, string rptPath, string ComDate)
        {
            ReportDocument repDoc = new ReportDocument();
            DataSet dsQuotReport = new DataSet();
            DataSet dsPackSummary = new DataSet();
            DataSet dsInvoicePackSum = new DataSet();
            clsQuotationReport objQuotReport = new clsQuotationReport();
            CommonFeatures objrep = new CommonFeatures();
            string fromDate = "";
            string toDate = "";
            int Customer = 0;
            int pol = 0;
            int pod = 0;
            string cargo = null;
            DataSet dsDescription = null;

            //If Not Me.wdcBDate.Value Is Nothing Then
            //fromDate = wdcBDate.Value
            // End If
            //get the to date
            //If Not Me.wdcToDt.Value Is Nothing Then
            //    toDate = wdcToDt.Value
            //End If

            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            DataSet AddressDS = new DataSet();
            Int32 flag = default(Int32);
            try
            {
                dsQuotReport = ObjClsTransportNote.PackingHeader(Convert.ToInt64(BookingPK));
                dsQuotReport.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\ComInvHeader.xsd");
                //Main Report 
                repDoc.Load(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\InvoicePackageDesc1.rpt");

                dsDescription = ObjClsTransportNote.ComInvoiceGoodsDesc(Convert.ToInt64(BookingPK));
                //'SubReport
                dsDescription.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\ComInvoiceSubReport1.xsd");
                repDoc.OpenSubreport("rptSubGoodsDetails").SetDataSource(dsDescription);

                ///'
                dsPackSummary = ObjClsTransportNote.PackingSummary(Convert.ToInt64(BookingPK));
                //'SubReport
                dsPackSummary.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\PackSummaryDet.xsd");
                repDoc.OpenSubreport("rptPackageSummary").SetDataSource(dsPackSummary);

                dsInvoicePackSum = ObjClsTransportNote.InvPackingSummary(Convert.ToInt64(BookingPK));
                //'SubReports
                dsInvoicePackSum.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\PackSummary.xsd");
                repDoc.OpenSubreport("rptSubCartonSummary").SetDataSource(dsInvoicePackSum);
                ///''
                AddressDS = ObjClsTransportNote.FetchLocationWithVat(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\CR_Location.xsd");

                repDoc.OpenSubreport("addressdetails.rpt").SetDataSource(AddressDS);

                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

                repDoc.SetDataSource(dsQuotReport);

                repDoc.SetParameterValue(0, CommInvNum);
                repDoc.SetParameterValue(1, ComDate);

                //repDoc.SetParameterValue(2, cargo)
                objrep.getReportControls(repDoc, "QFOR4459");

                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        public DataSet FetchJCRptDetails(long JOBPK)
        {
            WorkFlow objWK = new WorkFlow();
            try
            {
                var _with9 = objWK.MyCommand.Parameters;
                _with9.Add("JOB_CARD_TRN_FK_IN", JOBPK).Direction = ParameterDirection.Input;
                _with9.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                return objWK.GetDataSet("BOOKING_MST_PKG", "FETCH_JOBRPT_DETAILS");
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #region "Cargo/Frieght Manifest"
        public object ManifestReports(long JOB_PK, long MBL_PK, int StoreorPreviewFlag, string strDocName, int BizType = 1, int ProcessType = 1, int Commodity_Type = 1, int Cargo_Type = 0, string FormType = "", string ddlCommodity = "",
        string ViewState = "")
        {
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            Cls_SeaCargoManifest objCargo = new Cls_SeaCargoManifest();
            WorkFlow objWK = new WorkFlow();
            clsConsolidatedInvoices objConsInv = new clsConsolidatedInvoices();
            ReportDocument repDoc = new ReportDocument();
            DataSet MainRptDS = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet CntDS = new DataSet();
            DataSet dsreefer = new DataSet();
            DataSet dsODC = new DataSet();
            DataSet dshaz = new DataSet();
            int i = 0;
            int j = 0;
            int K = 0;
            string MBLPK = "0";
            string JOBPK = "0";
            Int32 Haz_Reefer = 0;
            short CommodityType = 0;
            DataSet CommDS = null;
            int BBFLAG = 0;
            string commgrp = null;
            DataSet subrptDs = new DataSet();
            string Type = null;
            string RequestForm = null;
            string Process = null;
            try
            {
                Type = FormType;
                //form name
                CommodityType = Convert.ToInt16(Commodity_Type);
                MBLPK = Convert.ToString(MBL_PK);
                JOBPK = Convert.ToString(JOB_PK);
                RequestForm = FormType;
                if (ProcessType == 1)
                {
                    Process = "EXPORT";
                }
                else
                {
                    Process = "IMPORT";
                }
                if (ViewState == "FreightManifest")
                {
                }
                if (BBFLAG == 4)
                {
                    AddressDS = ObjClsTransportNote.FetchLocation(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));

                    //'SEA     'RRR                        
                    if (BizType == 2)
                    {
                        //RRR
                        if (RequestForm == "MSTSEA")
                        {
                            MBLPK = Convert.ToString(MBL_PK);
                            MainRptDS = objCargo.FetchSeacargoReportDetails(MBLPK, JOBPK, CommodityType, 2);
                            CommDS = objCargo.FetchCommodityDetails(JOBPK);
                        }
                        else
                        {
                            MainRptDS = objCargo.FetchSeacargoReportDetails(MBLPK, JOBPK, CommodityType, 2, Process);
                            CommDS = objCargo.FetchCommodityDetails(JOBPK, Process);
                        }
                        if (Type == "FreightManifest" | ViewState == "MSTFreight")
                        {
                            //RRR
                            if (RequestForm == "MSTSEA")
                            {
                                subrptDs = objCargo.FetchFreightDetails(JOBPK);
                            }
                            else
                            {
                                subrptDs = objCargo.FetchFreightDetails(JOBPK, Process);
                            }
                        }
                        CntDS.Tables.Add();
                        CntDS.Tables[0].Columns.Add("MBPK", typeof(decimal));
                        CntDS.Tables[0].Columns.Add("CNTTYPE", typeof(string));
                        CntDS.Tables[0].Columns.Add("COUNT", typeof(decimal));
                        DataRow Mdr = null;
                        DataRow Cdr = null;
                        bool Flag = false;
                        foreach (DataRow Mdr_loopVariable in MainRptDS.Tables[0].Rows)
                        {
                            Mdr = Mdr_loopVariable;
                            Flag = false;
                            if (!string.IsNullOrEmpty(Mdr["CONTAINERTYPE"].ToString()))
                            {

                                foreach (DataRow Cdr_loopVariable in CntDS.Tables[0].Rows)
                                {
                                    Cdr = Cdr_loopVariable;
                                    if (Cdr["Cnttype"] == Mdr["CONTAINERTYPE"] & Cdr["MBPK"] == Mdr["JOBPK"])
                                    {
                                        Cdr["Count"] += "1";
                                        Flag = true;
                                        break; // TODO: might not be correct. Was : Exit For
                                    }
                                }
                                if (Flag == false)
                                {
                                    Cdr = CntDS.Tables[0].NewRow();
                                    Cdr["MBPK"] = Mdr["JOBPK"];
                                    Cdr["Cnttype"] = Mdr["CONTAINERTYPE"];
                                    Cdr["Count"] = 1;
                                    CntDS.Tables[0].Rows.Add(Cdr);
                                }
                            }
                        }
                        if (RequestForm == "MSTSEA")
                        {
                            CntDS.AcceptChanges();
                            repDoc.Load(HttpContext.Current.Server.MapPath("../07Reports") + "/SeaCargomanifestBB.rpt");
                            MainRptDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/SeaCargoManifest.xsd");
                            CntDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/CntDetails.xsd");
                            repDoc.OpenSubreport("CntDetails").SetDataSource(CntDS);
                            subrptDs.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/FreightCargoManifest.xsd");
                            repDoc.OpenSubreport("FreightDetails").SetDataSource(subrptDs);
                            CommDS.ReadXml(HttpContext.Current.Server.MapPath("../07Reports") + "/CommDetails.xsd");
                            repDoc.OpenSubreport("CommodityDetails").SetDataSource(CommDS);
                            objrep.getReportControls(repDoc, "QFOR3064");
                        }
                        else
                        {
                            CntDS.AcceptChanges();
                            repDoc.Load(HttpContext.Current.Server.MapPath("../07Reports") + "/SeaCargoManifestBB.rpt");
                            MainRptDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/SeaCargoManifest.xsd");
                            CntDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/CntDetails.xsd");
                            repDoc.OpenSubreport("CntDetails").SetDataSource(CntDS);
                            subrptDs.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/FreightCargoManifest.xsd");
                            repDoc.OpenSubreport("FreightDetails").SetDataSource(subrptDs);
                            CommDS.ReadXml(HttpContext.Current.Server.MapPath("../07Reports") + "/CommDetails.xsd");
                            repDoc.OpenSubreport("CommodityDetails").SetDataSource(CommDS);
                            objrep.getReportControls(repDoc, "QFOR3064");
                        }

                        dshaz = objCargo.FetchHazardousDetails(2, JOBPK, Process);
                        dshaz.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/Hazardous.xsd");
                        repDoc.OpenSubreport("HazDetails").SetDataSource(dshaz);

                        dsreefer = objCargo.FetchReeferDetails(2, JOBPK, Process);
                        dsreefer.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/reefer.xsd");
                        repDoc.OpenSubreport("ReeferDetails").SetDataSource(dsreefer);

                        dsODC = objCargo.FetchODCDetails(2, JOBPK, Process);
                        dsODC.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/ODC_DETAILS.xsd");
                        repDoc.OpenSubreport("ODCDETAILS").SetDataSource(dsODC);
                    }

                    if (Haz_Reefer != 1)
                    {
                        repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                    }
                    repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);
                    repDoc.SetDataSource(MainRptDS);
                    repDoc.SetParameterValue("commgrp", "All");
                    if (Type == "FreightManifest" | ViewState == "MSTFreight")
                    {
                        if (ddlCommodity == getCommodityGrp(2).ToString())
                        {
                            repDoc.SetParameterValue("Type", "HAZARDOUSFFRT");
                        }
                        else if (ddlCommodity == getCommodityGrp(3).ToString())
                        {
                            repDoc.SetParameterValue("Type", "REEFERFRT");
                        }
                        else if (ddlCommodity == getCommodityGrp(4).ToString())
                        {
                            repDoc.SetParameterValue("Type", "ODCFRT");
                        }
                        else
                        {
                            repDoc.SetParameterValue("Type", "FreightManifest");
                        }

                    }
                    else
                    {
                        repDoc.SetParameterValue("Type", "CargoManifest");
                    }

                }
                else
                {
                    AddressDS = ObjClsTransportNote.FetchLocation(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                    AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\TN_Location.xsd");

                    //'SEA     'RRR
                    if (BizType == 2)
                    {
                        //RRR
                        if (RequestForm == "MSTSEA")
                        {
                            MBLPK = Convert.ToString(MBL_PK);
                            JOBPK = Convert.ToString(JOB_PK);
                            MainRptDS = objCargo.FetchSeacargoReportDetails(MBLPK, JOBPK, CommodityType, 2, "EXPORT");
                        }
                        else
                        {
                            MainRptDS = objCargo.FetchSeacargoReportDetails(MBLPK, JOBPK, CommodityType, 2, Process);
                        }
                        if (Type == "FreightManifest" | ViewState == "MSTFreight")
                        {
                            //RRR
                            if (RequestForm == "MSTSEA")
                            {
                                JOBPK = Convert.ToString(JOB_PK);
                                subrptDs = objCargo.FetchFreightDetails(JOBPK);
                            }
                            else
                            {
                                subrptDs = objCargo.FetchFreightDetails(JOBPK, Process);
                            }
                        }
                        CntDS.Tables.Add();
                        CntDS.Tables[0].Columns.Add("MBPK", typeof(decimal));
                        CntDS.Tables[0].Columns.Add("CNTTYPE", typeof(string));
                        CntDS.Tables[0].Columns.Add("COUNT", typeof(decimal));
                        DataRow Mdr = null;
                        DataRow Cdr = null;
                        bool Flag = false;
                        foreach (DataRow Mdr_loopVariable in MainRptDS.Tables[0].Rows)
                        {
                            Mdr = Mdr_loopVariable;
                            Flag = false;
                            if (!string.IsNullOrEmpty(Mdr["CONTAINERTYPE"].ToString()))
                            {

                                foreach (DataRow Cdr_loopVariable in CntDS.Tables[0].Rows)
                                {
                                    Cdr = Cdr_loopVariable;
                                    if (Cdr["Cnttype"] == Mdr["CONTAINERTYPE"] & Cdr["MBPK"] == Mdr["JOBPK"])
                                    {
                                        Cdr["Count"] += "1";
                                        Flag = true;
                                        break; // TODO: might not be correct. Was : Exit For
                                    }
                                }
                                if (Flag == false)
                                {
                                    Cdr = CntDS.Tables[0].NewRow();
                                    Cdr["MBPK"] = Mdr["JOBPK"];
                                    Cdr["Cnttype"] = Mdr["CONTAINERTYPE"];
                                    Cdr["Count"] = 1;
                                    CntDS.Tables[0].Rows.Add(Cdr);
                                }
                            }
                        }
                        if (RequestForm == "MSTSEA")
                        {
                            CntDS.AcceptChanges();
                            repDoc.Load(HttpContext.Current.Server.MapPath("../07Reports") + "/SeaCargomanifest.rpt");
                            MainRptDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/SeaCargoManifest.xsd");
                            subrptDs.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/FreightCargoManifest.xsd");
                            CntDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/CntDetails.xsd");
                            dshaz.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/Hazardous.xsd");
                            dsreefer.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/reefer.xsd");
                            dsODC.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/ODC_DETAILS.xsd");
                            repDoc.OpenSubreport("CntDetails").SetDataSource(CntDS);
                            repDoc.OpenSubreport("FreightDetails").SetDataSource(subrptDs);
                            repDoc.OpenSubreport("HazDetails").SetDataSource(dshaz);
                            repDoc.OpenSubreport("ReeferDetails").SetDataSource(dsreefer);
                            repDoc.OpenSubreport("ODCDETAILS").SetDataSource(dsODC);
                            repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                            repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);
                            repDoc.SetDataSource(MainRptDS);
                        }
                        else
                        {
                            CntDS.AcceptChanges();
                            repDoc.Load(HttpContext.Current.Server.MapPath("../07Reports") + "/SeaCargomanifest.rpt");
                            MainRptDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/SeaCargoManifest.xsd");
                            dshaz = objCargo.FetchHazardousDetails(2, JOBPK, Process);
                            dsreefer = objCargo.FetchReeferDetails(2, JOBPK, Process);
                            dsODC = objCargo.FetchODCDetails(2, JOBPK, Process);
                            CntDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/CntDetails.xsd");
                            subrptDs.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/FreightCargoManifest.xsd");
                            dshaz.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/Hazardous.xsd");
                            dsreefer.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/reefer.xsd");
                            dsODC.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/ODC_DETAILS.xsd");
                            repDoc.OpenSubreport("HazDetails").SetDataSource(dshaz);
                            repDoc.OpenSubreport("CntDetails").SetDataSource(CntDS);
                            repDoc.OpenSubreport("FreightDetails").SetDataSource(subrptDs);
                            repDoc.OpenSubreport("ReeferDetails").SetDataSource(dsreefer);
                            repDoc.OpenSubreport("ODCDETAILS").SetDataSource(dsODC);
                            repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                            repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);
                            repDoc.SetDataSource(MainRptDS);
                        }

                        repDoc.SetParameterValue("commgrp", "All");

                        if (Type == "FreightManifest" | ViewState == "MSTFreight")
                        {
                            if (ddlCommodity == getCommodityGrp(2).ToString())
                            {
                                repDoc.SetParameterValue("Type", "HAZARDOUSFFRT");
                            }
                            else if (ddlCommodity == getCommodityGrp(3).ToString())
                            {
                                repDoc.SetParameterValue("Type", "REEFERFRT");
                            }
                            else if (ddlCommodity == getCommodityGrp(4).ToString())
                            {
                                repDoc.SetParameterValue("Type", "ODCFRT");
                            }
                            else
                            {
                                repDoc.SetParameterValue("Type", "FreightManifest");
                            }

                        }
                        else
                        {
                            repDoc.SetParameterValue("Type", "CargoManifest");
                        }

                        //'AIR
                    }
                    else
                    {
                        //<<<<<<<<<-------------Newly added by Jagadeesh for sub report on 13-Dec-06->>>>
                        if (Type == "FreightManifest" | ViewState == "MSTAIRFreight")
                        {
                            if (RequestForm == "MSTAIR")
                            {
                                subrptDs = objCargo.FetchAirFreightDetails(Convert.ToString(JOB_PK));
                            }
                            else
                            {
                                subrptDs = objCargo.FetchAirFreightDetails(JOBPK, Process);
                            }
                        }
                        //<<<<<<--------------------------------------------------------------------------
                        if (RequestForm == "MSTAIR")
                        {
                            //        MainRptDS = objCargo.FetchAircargoReportDetails(ViewState.Item("MAWBPk"), ViewState.Item("JobCardPK"), CommodityType, "EXPORT")
                            JOBPK = Convert.ToString(JOB_PK);
                        }
                        else
                        {
                            MainRptDS = objCargo.FetchAircargoReportDetails(MBLPK, JOBPK, CommodityType, Process);
                        }
                        if (RequestForm == "MSTAIR")
                        {
                            repDoc.Load(HttpContext.Current.Server.MapPath("../07Reports") + "/rptSeaCargoManifest.rpt");
                            MainRptDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/Main_SeaCargoManifest.xsd");
                            dshaz = objCargo.FetchHazardousDetails(1, JOBPK, Process);
                            dsreefer = objCargo.FetchReeferDetails(1, JOBPK, Process);
                            dsODC = objCargo.FetchODCDetails(1, JOBPK, Process);
                            dshaz.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/Hazardous.xsd");
                            dsreefer.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/reefer.xsd");
                            dsODC.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/ODC_DETAILS.xsd");
                            repDoc.OpenSubreport("HazDetails").SetDataSource(dshaz);
                            repDoc.OpenSubreport("ReeferDetails").SetDataSource(dsreefer);
                            repDoc.OpenSubreport("ODCDETAILS").SetDataSource(dsODC);
                        }
                        else
                        {
                            repDoc.Load(HttpContext.Current.Server.MapPath("../07Reports") + "/rptSeaCargoManifest.rpt");
                            MainRptDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/Main_SeaCargoManifest.xsd");
                            dshaz = objCargo.FetchHazardousDetails(1, JOBPK, Process);
                            dsreefer = objCargo.FetchReeferDetails(1, JOBPK, Process);
                            dsODC = objCargo.FetchODCDetails(1, JOBPK, Process);
                            dshaz.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/Hazardous.xsd");
                            dsreefer.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/reefer.xsd");
                            dsODC.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/ODC_DETAILS.xsd");
                            repDoc.OpenSubreport("HazDetails").SetDataSource(dshaz);
                            repDoc.OpenSubreport("ReeferDetails").SetDataSource(dsreefer);
                            repDoc.OpenSubreport("ODCDETAILS").SetDataSource(dsODC);
                        }

                        //<<<<<<<<-------------------------------------------------------------------
                        if (Type == "FreightManifest" | ViewState == "MSTAIRFreight")
                        {
                            if (RequestForm == "MSTAIR")
                            {
                                subrptDs.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/FreightCargoManifest.xsd");
                                repDoc.OpenSubreport("FreightDetails").SetDataSource(subrptDs);
                            }
                            else
                            {
                                subrptDs.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "/FreightCargoManifest.xsd");
                                repDoc.OpenSubreport("FreightDetails").SetDataSource(subrptDs);
                            }
                        }
                        if (Haz_Reefer != 1)
                        {
                            repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                        }
                        repDoc.OpenSubreport("AddressDtls").SetDataSource(AddressDS);
                        repDoc.SetDataSource(MainRptDS);
                        //<<<<<<<<-------------------------------------------------------------------
                        repDoc.SetParameterValue(0, 1);
                        repDoc.SetParameterValue(1, getDefault(AddressDS.Tables[0].Rows[0]["corporate_name"], ""));
                        if (Type == "FreightManifest" | ViewState == "MSTAIRFreight")
                        {
                            repDoc.SetParameterValue(2, "FreightManifest");
                        }
                        else
                        {
                            repDoc.SetParameterValue(2, "CargoManifest");
                        }
                    }

                }

                if (BizType == 2)
                {
                    objrep.getReportControls(repDoc, "QFOR3064");
                }
                else
                {
                    objrep.getReportControls(repDoc, "QFOR3064", 2);
                }

                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "FETCH CARGOTYPE"
        public DataSet FetchCargoType(int JOBPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append(" SELECT DISTINCT JCSE.CARGO_TYPE");
            sb.Append("  FROM JOB_CARD_TRN JCSE");
            sb.Append("  WHERE CIT.INV_AGENT_PK = " + JOBPk + "");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "Fetch Job Details"
        public DataSet FetchJobDetails(int JOBPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append(" SELECT J.MBL_MAWB_FK,J.BUSINESS_TYPE,J.PROCESS_TYPE,J.CARGO_TYPE,J.COMMODITY_GROUP_FK");
            sb.Append("  FROM JOB_CARD_TRN J ");
            sb.Append("  WHERE J.JOB_CARD_TRN_PK = " + JOBPk + " ");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "Fetch Job Details"
        public DataSet FetchCollectionDetails(int JOBPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append("SELECT DISTINCT C.COLLECTIONS_TBL_PK,");
            sb.Append("     C.PROCESS_TYPE,");
            sb.Append("     C.BUSINESS_TYPE,");
            sb.Append("     CIT.CONSOL_INVOICE_PK,C.COLLECTIONS_REF_NO");
            sb.Append(" FROM COLLECTIONS_TBL        C,");
            sb.Append("    COLLECTIONS_TRN_TBL    CT,");
            sb.Append("    CONSOL_INVOICE_TBL     CIT,");
            sb.Append("   CONSOL_INVOICE_TRN_TBL CITT,");
            sb.Append("   JOB_CARD_TRN           J");
            sb.Append("  WHERE C.COLLECTIONS_TBL_PK = CT.COLLECTIONS_TBL_FK");
            sb.Append("     AND CT.INVOICE_REF_NR = CIT.INVOICE_REF_NO");
            sb.Append("    AND CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK");
            sb.Append("   AND J.JOB_CARD_TRN_PK = CITT.JOB_CARD_FK");
            sb.Append("  AND J.JOB_CARD_TRN_PK = " + JOBPk + " ");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "Collection"
        public object CollectionPrint(int Coll_pk, int INVPk, int ProcessType, int BizType, string COllRefNr)
        {
            ReportDocument repDoc = new ReportDocument();
            DataSet dsHeader = new DataSet();
            DataSet dsCollectMode = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet DsCrInvMain = new DataSet();
            string strBarCode = null;
            CommonFeatures objrep = new CommonFeatures();
            clsConsolidatedInvoices objConsInv = new clsConsolidatedInvoices();
            clsCollection objCol = new clsCollection();
            try
            {
                repDoc.Load(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\rptCollection.rpt");
                dsHeader = objCol.FetchReportHeader(Coll_pk, Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                dsHeader.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Collection.xsd");

                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

                Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
                AddressDS = ObjClsTransportNote.FetchLocation(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\TN_Location.xsd");
                repDoc.OpenSubreport("addressdetails.rpt").SetDataSource(AddressDS);

                DsCrInvMain = objConsInv.CONSOL_INV_CUST_PRINT(INVPk, BizType, ProcessType);
                DsCrInvMain.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\Inv_Custumer.xsd");
                repDoc.OpenSubreport("RptCustDetails").SetDataSource(DsCrInvMain);

                dsCollectMode = objCol.getModDetDs(Coll_pk);
                dsCollectMode.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\subModeDetail.xsd");
                repDoc.OpenSubreport("subModeReport").SetDataSource(dsCollectMode);
                repDoc.SetDataSource(dsHeader);

                repDoc.SetParameterValue("CURR", HttpContext.Current.Session["CURRENCY_ID"]);
                if (objCol.GetBarcodeFlag("COLLECTIONS") == "1" & ConfigurationSettings.AppSettings["ShowBarcode"] == "1")
                {
                    strBarCode = "*" + COllRefNr + "*";
                }
                repDoc.SetParameterValue("ApplyBarcode", strBarCode);

                return repDoc;

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Release Note"
        public object ReleaseNoteReport(int JOBCARDPK, int BizType, string PodID, string WareHouse, long CREATED_BY_FK)
        {
            object functionReturnValue = null;
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            ReportDocument repDoc = new ReportDocument();
            CommonFeatures objrep = new CommonFeatures();
            ReportDocument repDoc1 = new ReportDocument();
            ReportDocument repDoc2 = new ReportDocument();
            DataSet MainRptDS = new DataSet();
            DataSet SubRptDS = new DataSet();
            DataSet AddressDS = new DataSet();
            DataSet SummaryDS = new DataSet();
            DataSet M_DataSet = new DataSet();
            DataSet dsLogLoc_Address = new DataSet();
            DataSet Temp2P = new DataSet();
            DataSet CommDetailsDS = new DataSet();
            DataSet CargoDS = new DataSet();
            bool @checked = false;
            string tempPodID = null;
            string tempWHouse = null;
            int i = 0;
            int r = 0;
            int Type = 0;
            string CustPk = null;
            string CreditMgnr = null;
            int IntFlag = 0;
            string M_proto = null;
            Cls_Release_Note objRelease = new Cls_Release_Note();
            cls_MBL_Entry objMBLEntry = new cls_MBL_Entry();
            Cls_Arrival_Notice objArrival = new Cls_Arrival_Notice();
            cls_JobCardSearch objJobCard = new cls_JobCardSearch();


            try
            {
                PodID = PodID.TrimEnd(',');
                WareHouse = WareHouse.TrimEnd(',');
                objRelease.CREATED_BY = Convert.ToInt64(HttpContext.Current.Session["USER_PK"]);

                if (BizType == 2)
                {
                    M_proto = objRelease.GenerateSea_Ref_No(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), "");
                }
                else
                {
                    M_proto = objRelease.GenerateAir_Ref_No(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), "");
                }
                try
                {
                    if (UpdateDetails(JOBCARDPK, M_proto, BizType, WareHouse, CREATED_BY_FK) == false)
                    {
                        return functionReturnValue;
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }

                AddressDS = ObjClsTransportNote.FetchLocation(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\TN_Location.xsd");

                if (BizType == 2)
                {
                    MainRptDS = objRelease.FetchJobCardSeaDetails(Convert.ToString(JOBCARDPK), M_proto);
                    if (IntFlag == 1)
                    {
                        CommDetailsDS = objRelease.fetchCommDetails(Convert.ToString(JOBCARDPK));
                    }

                    Temp2P = objRelease.fetchTempCustSea(Convert.ToString(JOBCARDPK));
                    Type = 2;
                }
                else
                {
                    MainRptDS = objRelease.FetchJobCardAirDetails(Convert.ToString(JOBCARDPK), M_proto);
                    Temp2P = objRelease.fetchTempCustAir(Convert.ToString(JOBCARDPK));
                    Type = 1;
                }

                if ((Temp2P != null))
                {
                    if (Temp2P.Tables[0].Rows.Count > 0)
                    {
                        for (r = 0; r <= Temp2P.Tables[0].Rows.Count - 1; r++)
                        {
                            CustPk = "";
                            for (i = 0; i <= 2; i++)
                            {
                                if (!string.IsNullOrEmpty(Convert.ToString(Temp2P.Tables[0].Rows[r][i])))
                                {
                                    CustPk += Temp2P.Tables[0].Rows[r][i] + ",";
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(CustPk))
                        {
                            CustPk = CustPk.TrimEnd(',');
                            Temp2Permanent(CustPk);
                        }
                    }
                }

                TrackAndTraceInsert(Convert.ToString(JOBCARDPK), Convert.ToInt16(BizType), PodID, WareHouse);
                if (IntFlag == 0)
                {
                    repDoc.Load(HttpContext.Current.Server.MapPath("../07Reports") + "\\rptReleaseNote.rpt");
                }
                else
                {
                    repDoc.Load(HttpContext.Current.Server.MapPath("../07Reports") + "\\rptBBReleaseNote.rpt");
                }

                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                dsLogLoc_Address = objMBLEntry.FetchLogAddressDtl(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                dsLogLoc_Address.ReadXmlSchema(HttpContext.Current.Server.MapPath("../07Reports") + "\\LogLoc_Address.xsd");
                repDoc.OpenSubreport("LogLoc_Address").SetDataSource(dsLogLoc_Address);
                repDoc.OpenSubreport("AddressDetails").SetDataSource(AddressDS);
                if (IntFlag == 1)
                {
                    repDoc.OpenSubreport("rptBBCommDetails").SetDataSource(CommDetailsDS);
                }
                repDoc.SetDataSource(MainRptDS);
                repDoc.SetParameterValue(0, getDefault(AddressDS.Tables[0].Rows[0]["corporate_name"], ""));
                repDoc.SetParameterValue(1, Type);
                repDoc.SetParameterValue("UserName", HttpContext.Current.Session["USER_NAME"]);
                repDoc.SetParameterValue("BizType", BizType);
                objrep.getReportControls(repDoc, "QFOR3046");
                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;

        }
        private void Temp2Permanent(string custPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            OracleTransaction TRAN = null;
            string strSQL = null;
            int nRecAfct = 0;
            try
            {
                ObjWk.OpenConnection();
                TRAN = ObjWk.MyConnection.BeginTransaction();
                var _with10 = objCommand;
                _with10.Connection = ObjWk.MyConnection;
                _with10.CommandType = CommandType.Text;
                _with10.Transaction = TRAN;
                strSQL = "update customer_mst_tbl set temp_party = 0 where temp_party=1 and customer_mst_pk in (" + custPk + ")";
                _with10.CommandText = strSQL;
                nRecAfct = _with10.ExecuteNonQuery();
                if (nRecAfct > 0)
                {
                    TRAN.Commit();
                }
                else
                {
                    TRAN.Rollback();
                }
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
            ObjWk.MyConnection.Close();
        }
        #region " Track N Trace Insert"
        private void TrackAndTraceInsert(string jobref, short BizType, string PodID, string WareHouse)
        {
            int nlocationfk = 0;
            Cls_Release_Note objRelease = new Cls_Release_Note();
            try
            {
                nlocationfk = Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                arrMessage = (ArrayList)objRelease.SaveTrackAndTrace(jobref, nlocationfk, BizType, Convert.ToInt64(HttpContext.Current.Session["USER_PK"]), PodID, WareHouse);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion
        #endregion

        #region "Fetch ReleaseNote Details"
        public DataSet FetchReleaseNote(int JOBPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append("SELECT DISTINCT JAI.JOB_CARD_TRN_PK AS JOBCARDPK,");
            sb.Append("       POL.PORT_ID              POLID,");
            sb.Append("       POL.PORT_NAME            POLNAME,");
            sb.Append("       POD.PORT_ID              PODID,");
            sb.Append("       POD.PORT_NAME            PODNAME,");
            sb.Append("       DEPOT.DEPOT_MST_PK       AS WAREHOUSEPK,");
            sb.Append("       DEPOT.DEPOT_NAME         AS WAREHOUSE,");
            sb.Append("       jai.business_type       AS BizType,");
            sb.Append("       JAI.CREATED_BY_FK     AS CREATED_BY_FK");
            sb.Append("  FROM JOB_CARD_TRN       JAI,");
            sb.Append("       CUSTOMER_MST_TBL   CMST,");
            sb.Append("       PORT_MST_TBL       POL,");
            sb.Append("       PORT_MST_TBL       POD,");
            sb.Append("       DOCS_PRINT_DTL_TBL DOCS,");
            sb.Append("       DEPOT_MST_TBL      DEPOT");
            sb.Append(" WHERE CMST.CUSTOMER_MST_PK(+) = JAI.CONSIGNEE_CUST_MST_FK");
            sb.Append("   AND POL.PORT_MST_PK = JAI.PORT_MST_POL_FK");
            sb.Append("   AND POD.PORT_MST_PK = JAI.PORT_MST_POD_FK");
            sb.Append("   AND DOCS.JOB_CARD_REF_FK(+) = JAI.JOB_CARD_TRN_PK");
            sb.Append("   AND DEPOT.DEPOT_MST_PK(+) = DOCS.DEPOT_MST_FK");
            sb.Append("   AND DOCS.DOC_NUMBER(+) LIKE 'RELEASE%'");
            sb.Append("  AND jai.JOB_CARD_TRN_PK = " + JOBPk + " ");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "DeliveryReport"
        public object DeliveryOrderReport(int DoDetailPks, int JobPk, int CargoType, int BizType, string FromDateClause, string DoRefDate)
        {
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            cls_DeliveryOrder objDO = new cls_DeliveryOrder();
            cls_TrackAndTrace objTrTc = new cls_TrackAndTrace();
            ReportDocument repDoc = new ReportDocument();
            DataSet AddressDS = new DataSet();
            string Company = null;
            DataSet GridDS = new DataSet();
            DataSet TotalClause = new DataSet();
            DataSet ContainerDetails = new DataSet();
            cls_BlClauseForHblMbl objClsBlClause = new cls_BlClauseForHblMbl();
            CommonFeatures objrep = new CommonFeatures();
            long Fcl_Lcl = 0;
            DataSet CommodityDetails = new DataSet();
            string PODPK = null;
            string CommPK = null;
            string FormFlag = null;
            string DoRefNumber = null;
            PODPK = "0";
            CommPK = "0";
            if (BizType == 1)
            {
                FormFlag = "Air";
            }
            else
            {
                FormFlag = "Sea";
            }

            try
            {


                if (Convert.ToString(DoDetailPks) == "0")
                {
                    // Message1.Message = "Plase Select The Records Which DO has already Raised"
                }

                if (BizType == 2)
                {
                    if (CargoType == 1)
                    {
                        if (CargoType == 1)
                        {
                            Fcl_Lcl = 1;
                        }
                        else if (CargoType == 2)
                        {
                            Fcl_Lcl = 2;
                        }
                        else
                        {
                            Fcl_Lcl = 4;
                        }
                    }
                    else
                    {
                        if (CargoType == 1)
                        {
                            Fcl_Lcl = 1;
                        }
                        else if (CargoType == 2)
                        {
                            Fcl_Lcl = 2;
                        }
                        else
                        {
                            Fcl_Lcl = 4;
                        }
                    }
                }
                else if (BizType == 1)
                {
                    if (CargoType == 1 | CargoType == 3)
                    {
                        Fcl_Lcl = 1;
                    }
                    else
                    {
                        Fcl_Lcl = 2;
                    }
                }
                if (BizType == 2)
                {
                    if (Fcl_Lcl == 4)
                    {
                        GridDS = objDO.fetchBBReport(Convert.ToString(DoDetailPks), BizType, Fcl_Lcl);
                    }
                    else
                    {
                        GridDS = objDO.fetchReport(Convert.ToString(DoDetailPks), BizType, Fcl_Lcl);
                    }
                }
                else
                {
                    GridDS = objDO.fetchReport(Convert.ToString(DoDetailPks), BizType, CargoType);
                }
                if (GridDS.Tables[0].Rows.Count > 0)
                {
                    if (Fcl_Lcl == 4)
                    {
                        GridDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\delivery_order.xsd");
                        repDoc.Load(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\rptBBDO.rpt");
                        repDoc.SetDataSource(GridDS);
                    }
                    else
                    {
                        GridDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\delivery_order.xsd");
                        repDoc.Load(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\rptDO.rpt");
                        repDoc.SetDataSource(GridDS);
                    }
                    GridDS = objClsBlClause.FetchBlClausesForHBL("", 7, 1, 1, ((PODPK == null) ? "0" : PODPK), ((CommPK == null) ? "0" : CommPK), Convert.ToInt64(DoDetailPks), Convert.ToString(Strings.Format(FromDateClause, "dd/MM/yyy")), ((FormFlag == null) ? "" : FormFlag));
                    GridDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\PrintBLClause.xsd");
                    repDoc.OpenSubreport("rptBLClause").SetDataSource(GridDS);
                    repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());
                    AddressDS = ObjClsTransportNote.FetchLocation(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                    AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\TN_Location.xsd");
                    repDoc.OpenSubreport("addressdetails.rpt").SetDataSource(AddressDS);
                    if (BizType == 2 & Fcl_Lcl == 1)
                    {
                        ContainerDetails = objDO.fetchContainers(Convert.ToString(DoDetailPks), BizType, Fcl_Lcl);
                        repDoc.OpenSubreport("Delivery_Containers_fcl").SetDataSource(ContainerDetails);
                        ContainerDetails.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\Delivery_Ord_Lcl.xsd");
                        repDoc.SetParameterValue("BizType_Fcl_Lcl_Type", 2);
                    }
                    else if (BizType == 2 & Fcl_Lcl == 2)
                    {
                        ContainerDetails = objDO.fetchContainers(Convert.ToString(DoDetailPks), BizType, Fcl_Lcl);
                        ContainerDetails.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\delivery_containers.xsd");
                        repDoc.OpenSubreport("Delivery_Ord_Lcl").SetDataSource(ContainerDetails);
                        repDoc.SetParameterValue("BizType_Fcl_Lcl_Type", 1);
                    }
                    else if (BizType == 1 & Fcl_Lcl == 1)
                    {
                        ContainerDetails = objDO.fetchContainers(Convert.ToString(DoDetailPks), BizType, Fcl_Lcl);
                        ContainerDetails.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\Delivery_Ord_Air_Kgs.xsd");
                        repDoc.OpenSubreport("Delivery_Ord_Air_Kgs").SetDataSource(ContainerDetails);
                        repDoc.SetParameterValue("BizType_Fcl_Lcl_Type", 4);
                    }
                    else if (BizType == 1 & Fcl_Lcl == 2)
                    {
                        ContainerDetails = objDO.fetchContainers(Convert.ToString(DoDetailPks), BizType, Fcl_Lcl);
                        ContainerDetails.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\Delivery_Ord_Air_Uld.xsd");
                        repDoc.OpenSubreport("Delivery_Ord_Air_Uld").SetDataSource(ContainerDetails);
                        repDoc.SetParameterValue("BizType_Fcl_Lcl_Type", 3);
                    }
                    else if (BizType == 2 & Fcl_Lcl == 4)
                    {
                        CommodityDetails = objDO.fetchCommDetails(Convert.ToString(DoDetailPks), BizType, Fcl_Lcl);
                        CommodityDetails.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Documentation") + "\\delivery_commodity.xsd");
                        repDoc.OpenSubreport("Delivery_Ord_BBC").SetDataSource(CommodityDetails);
                        repDoc.SetParameterValue("BizType_Fcl_Lcl_Type", 5);
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(AddressDS.Tables[0].Rows[0]["CORPORATE_NAME"])))
                    {
                        Company = Convert.ToString(AddressDS.Tables[0].Rows[0]["CORPORATE_NAME"]);
                    }

                    repDoc.SetParameterValue(0, Company);

                    objrep.getReportControls(repDoc, "QFOR4166");
                    DataSet DsQuoteDetails = new DataSet();
                    int StorageVal = 0;
                    string UniqueRefNo = null;
                    DsQuoteDetails = objTrTc.FetchDeliveryOrder(BizType);
                    if (DsQuoteDetails.Tables[0].Rows.Count > 0)
                    {
                        StorageVal = Convert.ToInt32(DsQuoteDetails.Tables[0].Rows[0]["APPLY_STORAGE"]);
                    }
                    return repDoc;
                }
                else
                {
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return false;
        }
        #endregion

        #region "FETCH DO DETAILS"
        public DataSet FetchDoDetails(int JOBPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append("  SELECT D.DELIVERY_ORDER_PK,D.DELIVERY_ORDER_REF_NO,D.JOB_CARD_MST_FK,D.BIZ_TYPE,D.CARGO_TYPE,D.JOB_CARD_AIR_MST_FK,D.DELIVERY_ORDER_DATE");
            sb.Append("  FROM DELIVERY_ORDER_MST_TBL D");
            sb.Append("  WHERE D.JOB_CARD_MST_FK = " + JOBPk + "");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "TransportNote"
        public object TransportNote(int TPNotePK, int BizType, int ProcessType, int CustomerPK)
        {
            object functionReturnValue = null;
            ReportDocument Rep = new ReportDocument();
            CommonFeatures objrep = new CommonFeatures();
            DataSet dsLoc = null;
            DataSet dsTran = null;
            int i = 0;
            int j = 0;
            string Containers = null;
            string BLNumber = null;
            string CustomerAdd = null;
            string CustomerName = null;
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            Cls_TransportNoteListing objTrans = new Cls_TransportNoteListing();
            string Strsql = null;
            DataSet objDs = new DataSet();
            DataSet objTruckDs = new DataSet();
            DataSet dsParam = new DataSet();
            string Truck = null;
            DataSet objCustDS = new DataSet();
            try
            {
                objTruckDs = objTrans.FetchTruck(TPNotePK, BizType);
                if (objTruckDs.Tables[0].Rows.Count == 0)
                {
                    return functionReturnValue;
                }
                for (i = 0; i <= objTruckDs.Tables[0].Rows.Count - 1; i++)
                {
                    Truck = Convert.ToString(objTruckDs.Tables[0].Rows[i]["TD_TRUCK_NUMBER"]);
                    objDs = ObjClsTransportNote.FetchContainersNew(BizType, ProcessType, TPNotePK, Truck);
                    Containers = "";
                    BLNumber = "";
                    if (objDs.Tables[0].Rows.Count > 0)
                    {
                        for (j = 0; j <= objDs.Tables[0].Rows.Count - 1; j++)
                        {
                            var _with11 = objDs.Tables[0].Rows[j];
                            if ((Containers == null) == false)
                            {
                                Containers += removeDBNull(_with11[0]).ToString() + ",";
                            }
                            else
                            {
                                Containers = removeDBNull(_with11[0]).ToString() + ",";
                            }
                        }
                    }
                    if ((Containers == null) == false)
                    {
                        Containers = Strings.Mid(Containers, 1, Strings.InStrRev(Containers, ",") - 1);
                    }

                    dsTran = ObjClsTransportNote.FetchTransporterSeaExpNew(TPNotePK, BizType, ProcessType, Truck);

                    if (dsTran.Tables[0].Rows.Count > 0)
                    {
                        for (j = 0; j <= dsTran.Tables[0].Rows.Count - 1; j++)
                        {
                            var _with12 = dsTran.Tables[0].Rows[j];
                            if ((BLNumber == null) == false)
                            {
                                BLNumber += removeDBNull(_with12[0]).ToString() + ",";
                            }
                            else
                            {
                                BLNumber = removeDBNull(_with12[0]).ToString() + ",";
                            }
                        }
                    }
                    if ((BLNumber == null) == false)
                    {
                        BLNumber = Strings.Mid(BLNumber, 1, Strings.InStrRev(BLNumber, ",") - 1);
                    }

                    Rep.Load(HttpContext.Current.Server.MapPath("..\\CustomsBrokerage") + "\\rptTransportNoteNew.rpt");
                    dsTran.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\CustomsBrokerage") + "\\TN_TransporterNew.xsd");

                    objCustDS = ObjClsTransportNote.FetchCustomerNew(CustomerPK);
                    objCustDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\CustomsBrokerage") + "\\rptCustomerDetails.xsd");
                    Rep.OpenSubreport("rptCustomerAddDetails").SetDataSource(objCustDS);

                    Rep.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

                    dsLoc = ObjClsTransportNote.FetchLocation(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                    dsLoc.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\07Reports") + "\\TN_Location.xsd");
                    Rep.OpenSubreport("rptAddressDetails").SetDataSource(dsLoc);
                    Rep.SetDataSource(dsTran);

                    Rep.SetParameterValue("Containers", (string.IsNullOrEmpty(Containers) ? "" : Containers));
                    Rep.SetParameterValue("BLNumber", (string.IsNullOrEmpty(BLNumber) ? "" : BLNumber));
                }
                return Rep;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }
        #endregion

        #region "FETCH DO DETAILS"
        public DataSet FetchTPDetails(int JOBPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append("  SELECT T.TRANSPORT_INST_SEA_PK, T.BUSINESS_TYPE, T.PROCESS_TYPE,T.TP_CUSTOMER_MST_FK");
            sb.Append("  FROM TRANSPORT_INST_SEA_TBL T");
            sb.Append("  WHERE T.TRANSPORT_INST_SEA_PK= '" + JOBPk + "'");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "CBJC Invoice PK"
        public ArrayList GetCBJCInvoicePK(string jobCardPK, int tpnFlag1 = 0)
        {
            System.Text.StringBuilder SQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            OracleDataReader oraReader = null;
            int invoiceCount = 0;
            long invoicePK = 0;
            ArrayList InvPks = new ArrayList();
            long DEMTRNPK = 0;
            try
            {
                if (tpnFlag1 == 1)
                {
                    WorkFlow objWK = new WorkFlow();
                    DEMTRNPK = Convert.ToInt32(objWK.ExecuteScaler(" SELECT DCH.DEM_CALC_HDR_PK FROM DEM_CALC_HDR DCH WHERE DCH.DOC_REF_FK = " + jobCardPK + " "));
                    jobCardPK = Convert.ToString(DEMTRNPK);
                    SQL.Append( "select distinct c.consol_invoice_fk from consol_invoice_trn_tbl c where c.job_type = 3 and c.job_card_fk = " + jobCardPK);
                }
                else if (tpnFlag1 == 4)
                {
                    SQL.Append( "select distinct c.consol_invoice_fk from consol_invoice_trn_tbl c where c.job_type = 4 and c.job_card_fk = " + jobCardPK);
                }
                else
                {
                    SQL.Append( "select distinct c.consol_invoice_fk from consol_invoice_trn_tbl c where c.job_type = 2 and c.job_card_fk = " + jobCardPK);
                }

            }
            catch (Exception ex)
            {
                throw ex;
            }
            oraReader = objWF.GetDataReader(SQL.ToString());
            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], DBNull.Value)))
                {
                    InvPks.Add(oraReader[0]);
                }
            }
            return InvPks;
            oraReader.Close();
            try
            {
                return InvPks;
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

        #region "Fetch CBJCJob Details"
        public DataSet FetchCBJCCollectionDetails(int JOBPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append("SELECT DISTINCT C.COLLECTIONS_TBL_PK,");
            sb.Append("     C.PROCESS_TYPE,");
            sb.Append("     C.BUSINESS_TYPE,");
            sb.Append("     CIT.CONSOL_INVOICE_PK,C.COLLECTIONS_REF_NO");
            sb.Append(" FROM COLLECTIONS_TBL        C,");
            sb.Append("    COLLECTIONS_TRN_TBL    CT,");
            sb.Append("    CONSOL_INVOICE_TBL     CIT,");
            sb.Append("   CONSOL_INVOICE_TRN_TBL CITT");
            sb.Append("  WHERE C.COLLECTIONS_TBL_PK = CT.COLLECTIONS_TBL_FK");
            sb.Append("     AND CT.INVOICE_REF_NR = CIT.INVOICE_REF_NO");
            sb.Append("    AND CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK AND CITT.JOB_TYPE = 2");
            sb.Append("  AND CITT.JOB_CARD_FK = " + JOBPk + " ");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "Fetch TPNJob Details"
        public DataSet FetchTPNCollectionDetails(int JOBPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append("SELECT DISTINCT C.COLLECTIONS_TBL_PK,");
            sb.Append("     C.PROCESS_TYPE,");
            sb.Append("     C.BUSINESS_TYPE,");
            sb.Append("     CIT.CONSOL_INVOICE_PK,C.COLLECTIONS_REF_NO");
            sb.Append(" FROM COLLECTIONS_TBL        C,");
            sb.Append("    COLLECTIONS_TRN_TBL    CT,");
            sb.Append("    CONSOL_INVOICE_TBL     CIT,");
            sb.Append("   CONSOL_INVOICE_TRN_TBL CITT");
            sb.Append("  WHERE C.COLLECTIONS_TBL_PK = CT.COLLECTIONS_TBL_FK");
            sb.Append("     AND CT.INVOICE_REF_NR = CIT.INVOICE_REF_NO");
            sb.Append("    AND CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK AND CITT.JOB_TYPE = 3");
            sb.Append("  AND CITT.JOB_CARD_FK = " + JOBPk + " ");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        #region "PackingList"
        public object PackageList(int Booking_PK, string ComDate, string ComRef)
        {
            ReportDocument repDoc = new ReportDocument();
            DataSet dsQuotReport = new DataSet();
            DataSet dsPackSummary = new DataSet();
            DataSet dsInvoicePackSum = new DataSet();
            clsQuotationReport objQuotReport = new clsQuotationReport();
            CommonFeatures objrep = new CommonFeatures();
            string fromDate = "";
            string toDate = "";
            int Customer = 0;
            int pol = 0;
            int pod = 0;
            string cargo = null;
            DataSet dsDescription = null;
            DataSet dsCheckLength = null;
            string FileStartName = Convert.ToString(HttpContext.Current.Session["User_ID"]);
            fromDate = ComDate;
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            DataSet AddressDS = new DataSet();
            Int32 flag = default(Int32);
            try
            {
                dsQuotReport = ObjClsTransportNote.PackingHeader(Booking_PK);
                dsQuotReport.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\ComInvHeader.xsd");
                //Main Report 
                repDoc.Load(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\InvoicePackage1.rpt");


                dsDescription = ObjClsTransportNote.PackageInvoiceGoodsDesc(Booking_PK);
                //'SubReport
                dsDescription.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\PackSubQuery.xsd");
                repDoc.OpenSubreport("rptSubGoodsDetails").SetDataSource(dsDescription);
                ///'
                dsPackSummary = ObjClsTransportNote.PackingSummary(Booking_PK);
                //'SubReport
                dsPackSummary.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\PackSummaryDet.xsd");
                repDoc.OpenSubreport("rptPackageSummary").SetDataSource(dsPackSummary);

                dsInvoicePackSum = ObjClsTransportNote.InvPackingSummary(Booking_PK);
                //'SubReports
                dsInvoicePackSum.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\PackSummary.xsd");
                repDoc.OpenSubreport("rptSubCartonSummary").SetDataSource(dsInvoicePackSum);
                ///''
                AddressDS = ObjClsTransportNote.FetchLocationWithVat(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                //'SubReports
                AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\CR_Location.xsd");

                repDoc.OpenSubreport("addressdetails.rpt").SetDataSource(AddressDS);

                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

                repDoc.SetDataSource(dsQuotReport);

                repDoc.SetParameterValue(0, "PACKLST" + ComRef);
                repDoc.SetParameterValue(1, fromDate);
                objrep.getReportControls(repDoc, "QFOR4459", 2);
                return repDoc;

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "CommInvoice"
        public object CommInvoice(int Booking_PK, string ComDate, string ComRef)
        {
            ReportDocument repDoc = new ReportDocument();
            DataSet dsQuotReport = new DataSet();
            DataSet dsPackSummary = new DataSet();
            DataSet dsInvoicePackSum = new DataSet();
            clsQuotationReport objQuotReport = new clsQuotationReport();
            CommonFeatures objrep = new CommonFeatures();
            string fromDate = "";
            string toDate = "";
            int Customer = 0;
            int pol = 0;
            int pod = 0;
            string cargo = null;
            DataSet dsDescription = null;
            string FileStartName = Convert.ToString(HttpContext.Current.Session["User_ID"]);
            fromDate = ComDate;
            Cls_Transport_Note ObjClsTransportNote = new Cls_Transport_Note();
            DataSet AddressDS = new DataSet();
            Int32 flag = default(Int32);
            try
            {
                dsQuotReport = ObjClsTransportNote.PackingHeader(Booking_PK);
                dsQuotReport.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\ComInvHeader.xsd");
                //Main Report 
                repDoc.Load(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\InvoicePackageDesc1.rpt");

                dsDescription = ObjClsTransportNote.ComInvoiceGoodsDesc(Booking_PK);
                //'SubReport
                dsDescription.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\ComInvoiceSubReport1.xsd");
                repDoc.OpenSubreport("rptSubGoodsDetails").SetDataSource(dsDescription);

                ///'
                dsPackSummary = ObjClsTransportNote.PackingSummary(Booking_PK);
                //'SubReport
                dsPackSummary.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\PackSummaryDet.xsd");
                repDoc.OpenSubreport("rptPackageSummary").SetDataSource(dsPackSummary);

                dsInvoicePackSum = ObjClsTransportNote.InvPackingSummary(Booking_PK);
                //'SubReports
                dsInvoicePackSum.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\06BookingExports") + "\\PackSummary.xsd");
                repDoc.OpenSubreport("rptSubCartonSummary").SetDataSource(dsInvoicePackSum);
                ///''
                AddressDS = ObjClsTransportNote.FetchLocationWithVat(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]));
                //'SubReports
                AddressDS.ReadXmlSchema(HttpContext.Current.Server.MapPath("..\\Recievables") + "\\CR_Location.xsd");

                repDoc.OpenSubreport("addressdetails.rpt").SetDataSource(AddressDS);

                repDoc.OpenSubreport("rptsubinvoiceimage.rpt").SetDataSource(ds_image());

                repDoc.SetDataSource(dsQuotReport);

                repDoc.SetParameterValue(0, "CI" + ComRef);
                repDoc.SetParameterValue(1, fromDate);
                repDoc.SetParameterValue("Currency", HttpContext.Current.Session["CURRENCY_ID"]);
                objrep.getReportControls(repDoc, "QFOR4459");
                return repDoc;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch TPNJob Details"
        public DataSet FetchPackingComInvoiceList(int JOBPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWK = new WorkFlow();
            sb.Append(" SELECT DISTINCT J.BOOKING_MST_FK, BTCD.COMM_INV_DT, BMT.BOOKING_REF_NO");
            sb.Append(" FROM JOB_CARD_TRN J, BOOKING_TRN_COMMINV_DTL BTCD, BOOKING_MST_TBL BMT");
            sb.Append(" WHERE J.BOOKING_MST_FK = BTCD.BOOKING_MST_FK AND BMT.BOOKING_MST_PK = J.BOOKING_MST_FK AND BTCD.BOOKING_MST_FK = BMT.BOOKING_MST_PK");
            sb.Append("  AND J.JOB_CARD_TRN_PK = " + JOBPk + " ");
            try
            {
                return objWK.GetDataSet(sb.ToString());
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
        #endregion

        public bool UpdateDetails(int jobpk, string ProtocolNo, int Busitype, string warehousepk, long CREATED_BY_FK)
        {

            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Int16 I = default(Int16);
            OracleTransaction Tran = null;
            try
            {
                ObjWk.OpenConnection();
                Tran = ObjWk.MyConnection.BeginTransaction();
                var _with13 = ObjWk.MyCommand;
                _with13.CommandText = ObjWk.MyUserName + ".RELEASE_NOTE_REP_PKG.DOCS_PRINT_DTL_TBL_INS";
                _with13.CommandType = CommandType.StoredProcedure;
                _with13.Transaction = Tran;
                _with13.Parameters.Clear();
                _with13.Parameters.Add("BUSINESS_TYPE_IN", Busitype).Direction = ParameterDirection.Input;
                _with13.Parameters.Add("MODE_IN", 2).Direction = ParameterDirection.Input;
                _with13.Parameters.Add("JOBCARD_REF_PK_IN", jobpk).Direction = ParameterDirection.Input;
                _with13.Parameters.Add("PROTOCOL_NO_IN", ProtocolNo).Direction = ParameterDirection.Input;
                _with13.Parameters.Add("DEPOT_FK_IN", warehousepk).Direction = ParameterDirection.Input;
                _with13.Parameters.Add("SPL_INS_IN", "").Direction = ParameterDirection.Input;
                _with13.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with13.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DOCS_PRINT_DTL_TBL_PK").Direction = ParameterDirection.Output;
                _with13.ExecuteNonQuery();
                Tran.Commit();
                return true;
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
                ObjWk.CloseConnection();
            }
        }
    }
}