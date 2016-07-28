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
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_CustomsTrasporterNote : CommonFeatures
    {
        #region "Fetch Listing"

        /// <summary>
        /// Fetches the customs listing.
        /// </summary>
        /// <param name="txtTransNrPk">The text trans nr pk.</param>
        /// <param name="txtTransporterPk">The text transporter pk.</param>
        /// <param name="txtJobPK">The text job pk.</param>
        /// <param name="txtCustomsPk">The text customs pk.</param>
        /// <param name="ddlTPTAllocationStatus">The DDL TPT allocation status.</param>
        /// <param name="ddlJobType">Type of the DDL job.</param>
        /// <param name="ddlStatus">The DDL status.</param>
        /// <param name="openClose">The open close.</param>
        /// <param name="ddlRefType">Type of the DDL reference.</param>
        /// <param name="txtRefNr">The text reference nr.</param>
        /// <param name="Fromdt">The fromdt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="LocFK">The loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TransNr">The trans nr.</param>
        /// <returns></returns>
        public DataSet FetchCustomsListing(int txtTransNrPk = 0, int txtTransporterPk = 0, int txtJobPK = 0, int txtCustomsPk = 0, string ddlTPTAllocationStatus = "", string ddlJobType = "", string ddlStatus = "", int openClose = 1, string ddlRefType = "", string txtRefNr = "",
        string Fromdt = "", string ToDt = "", string SearchType = "C", int BizType = 0, int ProcessType = 0, int LocFK = 0, Int32 flag = 0, Int32 TotalPage = 0, Int32 CurrentPage = 0, string TransNr = "")
        {
            WorkFlow objWK = new WorkFlow();
            OracleCommand objCommand = new OracleCommand();
            DataSet dsData = new DataSet();
            int TransPK = 0;

            try
            {
                if (!string.IsNullOrEmpty(TransNr) & txtCustomsPk == 0)
                {
                    //TransPK = objWK.ExecuteScaler("SELECT T.TRANSPORT_INST_SEA_PK FROM TRANSPORT_INST_SEA_TBL T WHERE UPPER(T.TRANS_INST_REF_NO)=UPPER('" + TransNr + "')");
                }
                else
                {
                    TransPK = txtTransNrPk;
                }

                objWK.OpenConnection();
                objWK.MyCommand.Connection = objWK.MyConnection;

                var _with1 = objWK.MyCommand;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".CUSTOMS_TRANSPORTNOTE_PKG.CUSTOMS_TRANSPORTNOTE_FETCH";

                objWK.MyCommand.Parameters.Clear();
                var _with2 = objWK.MyCommand.Parameters;

                _with2.Add("TRANS_NR_IN", TransPK).Direction = ParameterDirection.Input;
                _with2.Add("TRANS_PK_IN", txtTransporterPk).Direction = ParameterDirection.Input;
                _with2.Add("JOB_PK_IN", txtJobPK).Direction = ParameterDirection.Input;
                _with2.Add("CUSTOMS_PK_IN", txtCustomsPk).Direction = ParameterDirection.Input;
                _with2.Add("TPTALLOCATION_IN", Convert.ToInt32(ddlTPTAllocationStatus)).Direction = ParameterDirection.Input;
                _with2.Add("JOB_TYPE_IN", Convert.ToInt32(ddlJobType)).Direction = ParameterDirection.Input;
                _with2.Add("TPT_STATUS_IN", Convert.ToInt32(ddlStatus)).Direction = ParameterDirection.Input;
                _with2.Add("STATUS_IN", openClose).Direction = ParameterDirection.Input;
                _with2.Add("REF_TYPE_IN", Convert.ToInt32(ddlRefType)).Direction = ParameterDirection.Input;
                _with2.Add("REF_NR_IN", (string.IsNullOrEmpty(txtRefNr) ? "" : txtRefNr)).Direction = ParameterDirection.Input;
                _with2.Add("FROM_DATE_IN", (string.IsNullOrEmpty(Fromdt) ? "" : Fromdt)).Direction = ParameterDirection.Input;
                _with2.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with2.Add("SERACH_TYPE_IN", SearchType).Direction = ParameterDirection.Input;
                _with2.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with2.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_PK_IN", LocFK).Direction = ParameterDirection.Input;
                _with2.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("CUSTOMS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);
                //TotalPage = objWK.MyCommand.Parameters["TOTALPAGE_IN"].Value;
                //CurrentPage = objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value;
                //if (TotalPage == 0)
                //{
                //    CurrentPage = 0;
                //}
                //else
                //{
                //    CurrentPage = objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value;
                //}
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
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Fetch Listing"
    }
}