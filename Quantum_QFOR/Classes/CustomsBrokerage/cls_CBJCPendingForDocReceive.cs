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

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_CBJCPendingForDocReceive : CommonFeatures
    {
        #region "JobCard Pending For Documents to Receive"
        public DataSet FetchJCforDocRec(Int32 BizType = 0, Int32 ProcessType = 0, Int32 JobType = 0, Int32 CustomerPk = 0, Int32 PolPk = 0, Int32 PodPk = 0, string RefNr = "", Int32 VslPk = 0, string VoyNr = "", Int32 CargoType = 0,
        Int32 PenDocs = 0, string FromDt = "", string ToDt = "", Int32 LocFk = 0, Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 expExcel = 0)
        {

            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with1 = objWK.MyCommand;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".JOBSPENDIN_DOCREC_PKG.DOC_REC_FETCH";

                objWK.MyCommand.Parameters.Clear();
                var _with2 = objWK.MyCommand.Parameters;

                _with2.Add("JOB_TYPE_IN", JobType).Direction = ParameterDirection.Input;
                _with2.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with2.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with2.Add("CUSTOMER_PK_IN", CustomerPk).Direction = ParameterDirection.Input;
                _with2.Add("POL_PK_IN", PolPk).Direction = ParameterDirection.Input;
                _with2.Add("POD_PK_IN", PodPk).Direction = ParameterDirection.Input;
                _with2.Add("REFNR_IN", (string.IsNullOrEmpty(RefNr) ? "" : RefNr)).Direction = ParameterDirection.Input;
                _with2.Add("VSL_PK_IN", VslPk).Direction = ParameterDirection.Input;
                _with2.Add("FLIGHT_IN", (string.IsNullOrEmpty(VoyNr) ? "" : VoyNr)).Direction = ParameterDirection.Input;
                _with2.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with2.Add("REF_TYPE_IN", PenDocs).Direction = ParameterDirection.Input;
                _with2.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDt) ? "" : FromDt)).Direction = ParameterDirection.Input;
                _with2.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_PK_IN", LocFk).Direction = ParameterDirection.Input;
                _with2.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with2.Add("EXCEL_IN", expExcel).Direction = ParameterDirection.Input;
                _with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("CUSTOMS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);
                TotalPage = Convert.ToInt32(objWK.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }
                return dsData;
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

        #region "JobCard Pending For Documents to Upload"
        public DataSet FetchJCforDocUpload(Int32 BizType = 0, Int32 ProcessType = 0, Int32 JobType = 0, Int32 CustomerPk = 0, Int32 PolPk = 0, Int32 PodPk = 0, string RefNr = "", Int32 VslPk = 0, string VoyNr = "", Int32 CargoType = 0,
        Int32 PenDocs = 0, string FromDt = "", string ToDt = "", Int32 LocFk = 0, Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 expExcel = 0)
        {

            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with3 = objWK.MyCommand;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".JOBSPENDIN_DOCREC_PKG.DOC_UPLOAD_FETCH";

                objWK.MyCommand.Parameters.Clear();
                var _with4 = objWK.MyCommand.Parameters;

                _with4.Add("JOB_TYPE_IN", JobType).Direction = ParameterDirection.Input;
                _with4.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with4.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with4.Add("CUSTOMER_PK_IN", CustomerPk).Direction = ParameterDirection.Input;
                _with4.Add("POL_PK_IN", PolPk).Direction = ParameterDirection.Input;
                _with4.Add("POD_PK_IN", PodPk).Direction = ParameterDirection.Input;
                _with4.Add("REFNR_IN", (string.IsNullOrEmpty(RefNr) ? "" : RefNr)).Direction = ParameterDirection.Input;
                _with4.Add("VSL_PK_IN", VslPk).Direction = ParameterDirection.Input;
                _with4.Add("FLIGHT_IN", (string.IsNullOrEmpty(VoyNr) ? "" : VoyNr)).Direction = ParameterDirection.Input;
                _with4.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with4.Add("REF_TYPE_IN", PenDocs).Direction = ParameterDirection.Input;
                _with4.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDt) ? "" : FromDt)).Direction = ParameterDirection.Input;
                _with4.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with4.Add("LOCATION_PK_IN", LocFk).Direction = ParameterDirection.Input;
                _with4.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with4.Add("EXCEL_IN", expExcel).Direction = ParameterDirection.Input;
                _with4.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with4.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with4.Add("CUSTOMS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);
                TotalPage = Convert.ToInt32(objWK.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }
                return dsData;
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

        #region "JobCard Pending For Customs Documents Verification"
        public DataSet FetchJCforDocVerify(Int32 BizType = 0, Int32 ProcessType = 0, Int32 JobType = 0, Int32 CustomerPk = 0, Int32 PolPk = 0, Int32 PodPk = 0, string RefNr = "", Int32 VslPk = 0, string VoyNr = "", Int32 CargoType = 0,
        Int32 PenDocs = 0, string FromDt = "", string ToDt = "", Int32 LocFk = 0, Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 expExcel = 0)
        {

            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with5 = objWK.MyCommand;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".JOBSPENDIN_DOCREC_PKG.DOC_VERIFY_FETCH";

                objWK.MyCommand.Parameters.Clear();
                var _with6 = objWK.MyCommand.Parameters;

                _with6.Add("JOB_TYPE_IN", JobType).Direction = ParameterDirection.Input;
                _with6.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with6.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with6.Add("CUSTOMER_PK_IN", CustomerPk).Direction = ParameterDirection.Input;
                _with6.Add("POL_PK_IN", PolPk).Direction = ParameterDirection.Input;
                _with6.Add("POD_PK_IN", PodPk).Direction = ParameterDirection.Input;
                _with6.Add("REFNR_IN", (string.IsNullOrEmpty(RefNr) ? "" : RefNr)).Direction = ParameterDirection.Input;
                _with6.Add("VSL_PK_IN", VslPk).Direction = ParameterDirection.Input;
                _with6.Add("FLIGHT_IN", (string.IsNullOrEmpty(VoyNr) ? "" : VoyNr)).Direction = ParameterDirection.Input;
                _with6.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with6.Add("REF_TYPE_IN", PenDocs).Direction = ParameterDirection.Input;
                _with6.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDt) ? "" : FromDt)).Direction = ParameterDirection.Input;
                _with6.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with6.Add("LOCATION_PK_IN", LocFk).Direction = ParameterDirection.Input;
                _with6.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with6.Add("EXCEL_IN", expExcel).Direction = ParameterDirection.Input;
                _with6.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with6.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with6.Add("CUSTOMS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);
                TotalPage = Convert.ToInt32(objWK.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }
                return dsData;
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

        #region "JobCard Pending For Documents to Assessment"
        public DataSet FetchJCforDocAssess(Int32 BizType = 0, Int32 ProcessType = 0, Int32 JobType = 0, Int32 CustomerPk = 0, Int32 PolPk = 0, Int32 PodPk = 0, string RefNr = "", Int32 VslPk = 0, string VoyNr = "", Int32 CargoType = 0,
        Int32 PenDocs = 0, string FromDt = "", string ToDt = "", Int32 LocFk = 0, Int32 flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 expExcel = 0)
        {

            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();

            try
            {
                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with7 = objWK.MyCommand;
                _with7.CommandType = CommandType.StoredProcedure;
                _with7.CommandText = objWK.MyUserName + ".JOBSPENDIN_DOCREC_PKG.DOC_ASSESS_FETCH";

                objWK.MyCommand.Parameters.Clear();
                var _with8 = objWK.MyCommand.Parameters;

                _with8.Add("JOB_TYPE_IN", JobType).Direction = ParameterDirection.Input;
                _with8.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with8.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with8.Add("CUSTOMER_PK_IN", CustomerPk).Direction = ParameterDirection.Input;
                _with8.Add("POL_PK_IN", PolPk).Direction = ParameterDirection.Input;
                _with8.Add("POD_PK_IN", PodPk).Direction = ParameterDirection.Input;
                _with8.Add("REFNR_IN", (string.IsNullOrEmpty(RefNr) ? "" : RefNr)).Direction = ParameterDirection.Input;
                _with8.Add("VSL_PK_IN", VslPk).Direction = ParameterDirection.Input;
                _with8.Add("FLIGHT_IN", (string.IsNullOrEmpty(VoyNr) ? "" : VoyNr)).Direction = ParameterDirection.Input;
                _with8.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with8.Add("REF_TYPE_IN", PenDocs).Direction = ParameterDirection.Input;
                _with8.Add("FROM_DATE_IN", (string.IsNullOrEmpty(FromDt) ? "" : FromDt)).Direction = ParameterDirection.Input;
                _with8.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with8.Add("LOCATION_PK_IN", LocFk).Direction = ParameterDirection.Input;
                _with8.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with8.Add("EXCEL_IN", expExcel).Direction = ParameterDirection.Input;
                _with8.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with8.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with8.Add("CUSTOMS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);
                TotalPage = Convert.ToInt32(objWK.MyCommand.Parameters["TOTALPAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value);
                }
                return dsData;
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
    }
}