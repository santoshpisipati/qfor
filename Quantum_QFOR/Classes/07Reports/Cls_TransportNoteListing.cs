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

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_TransportNoteListing : CommonFeatures
    {
        #region "Fetch Trans Listing"

        /// <summary>
        /// Fetches the trans listing.
        /// </summary>
        /// <param name="txtTransNrPk">The text trans nr pk.</param>
        /// <param name="txtTransporterPk">The text transporter pk.</param>
        /// <param name="txtJobPK">The text job pk.</param>
        /// <param name="ddlMode">The DDL mode.</param>
        /// <param name="txtPickUpRefNr">The text pick up reference nr.</param>
        /// <param name="txtDelRefNr">The text delete reference nr.</param>
        /// <param name="Fromdt">The fromdt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <param name="LocFK">The loc fk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <returns></returns>
        public DataSet FetchTransListing(int txtTransNrPk = 0, int txtTransporterPk = 0, int txtJobPK = 0, string ddlMode = "", string txtPickUpRefNr = "", string txtDelRefNr = "", string Fromdt = "", string ToDt = "", string SearchType = "C", int BizType = 0,
        int ProcessType = 0, int LocFK = 0, Int32 flag = 0, Int32 TotalPage = 0, Int32 CurrentPage = 0)
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
                _with1.CommandText = objWK.MyUserName + ".TRANSPORT_NOTE_PKG.TRANSPORT_NOTE_FETCH";

                objWK.MyCommand.Parameters.Clear();
                var _with2 = objWK.MyCommand.Parameters;

                _with2.Add("TRANS_NR_IN", txtTransNrPk).Direction = ParameterDirection.Input;
                _with2.Add("TRANS_PK_IN", txtTransporterPk).Direction = ParameterDirection.Input;
                _with2.Add("JOB_PK_IN", txtJobPK).Direction = ParameterDirection.Input;
                _with2.Add("MODE_IN", Convert.ToInt32(ddlMode)).Direction = ParameterDirection.Input;
                _with2.Add("PICKUP_REF_IN", (string.IsNullOrEmpty(txtPickUpRefNr) ? "" : txtPickUpRefNr)).Direction = ParameterDirection.Input;
                _with2.Add("DEL_REF_IN", (string.IsNullOrEmpty(txtDelRefNr) ? "" : txtDelRefNr)).Direction = ParameterDirection.Input;
                _with2.Add("FROM_DATE_IN", (string.IsNullOrEmpty(Fromdt) ? "" : Fromdt)).Direction = ParameterDirection.Input;
                _with2.Add("TO_DATE_IN", (string.IsNullOrEmpty(ToDt) ? "" : ToDt)).Direction = ParameterDirection.Input;
                _with2.Add("SERACH_TYPE_IN", SearchType).Direction = ParameterDirection.Input;
                _with2.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with2.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_PK_IN", LocFK).Direction = ParameterDirection.Input;
                _with2.Add("POST_BACK_IN", flag).Direction = ParameterDirection.Input;
                _with2.Add("TOTALPAGE_IN", TotalPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("CURRENTPAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with2.Add("TRANS_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);
                //TotalPage = objWK.MyCommand.Parameters["TOTALPAGE_IN"].Value;
                //CurrentPage = objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value;
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    //CurrentPage = objWK.MyCommand.Parameters["CURRENTPAGE_IN"].Value;
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

        #endregion "Fetch Trans Listing"

        #region "Fetch Report Parameters"

        /// <summary>
        /// Fetches the report parameters.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ProcessType">Type of the process.</param>
        /// <returns></returns>
        public DataSet FetchReportParameters(int JobPK, int BizType, int ProcessType)
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
                _with3.CommandText = objWK.MyUserName + ".TRANSPORT_NOTE_PKG.FETCH_REPORT_PARAMETERS";

                objWK.MyCommand.Parameters.Clear();
                var _with4 = objWK.MyCommand.Parameters;

                _with4.Add("JOB_PK_IN", JobPK).Direction = ParameterDirection.Input;
                _with4.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with4.Add("PROCESS_TYPE_IN", ProcessType).Direction = ParameterDirection.Input;
                _with4.Add("JOB_HEADER_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

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

        #endregion "Fetch Report Parameters"

        #region "Fetch Truck"

        /// <summary>
        /// Fetches the truck.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchTruck(int JobPK, int BizType)
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
                _with5.CommandText = objWK.MyUserName + ".TRANSPORT_NOTE_PKG.FETCH_TRUCK_PARAMETERS";

                objWK.MyCommand.Parameters.Clear();
                var _with6 = objWK.MyCommand.Parameters;

                _with6.Add("JOB_PK_IN", JobPK).Direction = ParameterDirection.Input;
                _with6.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with6.Add("TRUCK_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                objWK.MyDataAdapter.SelectCommand = objWK.MyCommand;
                objWK.MyDataAdapter.Fill(dsData);

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

        #endregion "Fetch Truck"
    }
}