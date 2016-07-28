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
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsPLReport : CommonFeatures
    {
        /// <summary>
        /// The object vessel voyage
        /// </summary>
        private Quantum_QFOR.cls_SeaBookingEntry objVesselVoyage = new Quantum_QFOR.cls_SeaBookingEntry();

        #region " FetchPLBL "

        /// <summary>
        /// Fetches the PLBL.
        /// </summary>
        /// <param name="BL_FK">The b l_ fk.</param>
        /// <param name="JobCardFK">The job card fk.</param>
        /// <param name="LocationFK">The location fk.</param>
        /// <param name="selectedMonths">The selected months.</param>
        /// <param name="VslVoyPk">The VSL voy pk.</param>
        /// <param name="FlightNo">The flight no.</param>
        /// <param name="ShipperFK">The shipper fk.</param>
        /// <param name="ConsigneeFK">The consignee fk.</param>
        /// <param name="POLFK">The polfk.</param>
        /// <param name="PODFK">The podfk.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="Status">The status.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <param name="CommGrpFK">The comm GRP fk.</param>
        /// <param name="ExecutiveFK">The executive fk.</param>
        /// <param name="CurPK">The current pk.</param>
        /// <param name="isAdmin">The is admin.</param>
        /// <param name="txtBLID">The text blid.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="LoadFlg">The load FLG.</param>
        /// <param name="ReportFlg">The report FLG.</param>
        /// <param name="dispAll">The disp all.</param>
        /// <param name="FORM_FLG">For m_ FLG.</param>
        /// <returns></returns>
        public DataSet FetchPLBL(long BL_FK, long JobCardFK, long LocationFK, string selectedMonths, long VslVoyPk, string FlightNo, long ShipperFK, long ConsigneeFK, long POLFK, long PODFK,
        string FromDate, string ToDate, Int32 BizType, Int32 Process, Int32 CargoType, long Status, Int32 JobType, long CommGrpFK, long ExecutiveFK, Int32 CurPK,
        Int32 isAdmin, string txtBLID, Int32 CurrentPage, Int32 TotalPage, short LoadFlg, short ReportFlg, int dispAll = 0, string FORM_FLG = "PPL")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with1 = objWF.MyDataAdapter;
                _with1.SelectCommand = new OracleCommand();
                _with1.SelectCommand.Connection = objWF.MyConnection;
                if (FORM_FLG == "PPL")
                {
                    _with1.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_PROFIT_AND_LOSS_PKG.FETCH_PL_BL";
                }
                else
                {
                    _with1.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_PROFIT_AND_LOSS_PKG.FETCH_ACT_PL_BL";
                }
                _with1.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with1.SelectCommand.Parameters.Add("BOOKING_BL_PK_IN", OracleDbType.Int32).Value = getDefault(BL_FK, "");
                _with1.SelectCommand.Parameters.Add("JOB_CARD_PK_IN", OracleDbType.Int32).Value = getDefault(JobCardFK, "");
                _with1.SelectCommand.Parameters.Add("BL_NO_IN", OracleDbType.Varchar2).Value = getDefault(txtBLID, "");
                _with1.SelectCommand.Parameters.Add("LOCATION_MST_PK_IN", OracleDbType.Int32).Value = getDefault(LocationFK, "");
                _with1.SelectCommand.Parameters.Add("VESSEL_VOY_FK_IN", OracleDbType.Int32).Value = getDefault(VslVoyPk, "");
                _with1.SelectCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2).Value = getDefault(FlightNo, "");
                _with1.SelectCommand.Parameters.Add("SELECTED_MONTHS_IN", OracleDbType.Varchar2).Value = getDefault(selectedMonths, "");
                _with1.SelectCommand.Parameters.Add("SHIPPER_PK_IN", OracleDbType.Int32).Value = getDefault(ShipperFK, "");
                _with1.SelectCommand.Parameters.Add("CONSIGNEE_PK_IN", OracleDbType.Int32).Value = getDefault(ConsigneeFK, "");
                _with1.SelectCommand.Parameters.Add("POL_PK_IN", OracleDbType.Int32).Value = getDefault(POLFK, "");
                _with1.SelectCommand.Parameters.Add("POD_PK_IN", OracleDbType.Int32).Value = getDefault(PODFK, "");
                _with1.SelectCommand.Parameters.Add("FROM_DATE_IN", OracleDbType.Varchar2).Value = getDefault(FromDate, "");
                _with1.SelectCommand.Parameters.Add("TO_DATE_IN", OracleDbType.Varchar2).Value = getDefault(ToDate, "");
                _with1.SelectCommand.Parameters.Add("BIZTYPE_IN", OracleDbType.Int32).Value = BizType;
                _with1.SelectCommand.Parameters.Add("PROCESS_IN", OracleDbType.Int32).Value = Process;
                _with1.SelectCommand.Parameters.Add("CARGOTYPE_IN", OracleDbType.Int32).Value = CargoType;
                _with1.SelectCommand.Parameters.Add("STATUS_IN", OracleDbType.Int32).Value = Status;
                _with1.SelectCommand.Parameters.Add("JOBTYPE_IN", OracleDbType.Int32).Value = JobType;
                _with1.SelectCommand.Parameters.Add("COMMODITY_GROUP_PK_IN", OracleDbType.Int32).Value = getDefault(CommGrpFK, "");
                _with1.SelectCommand.Parameters.Add("EMPLOYEE_MST_PK_IN", OracleDbType.Int32).Value = getDefault(ExecutiveFK, "");
                _with1.SelectCommand.Parameters.Add("CURRENCY_MST_PK_IN", OracleDbType.Int32).Value = CurPK;
                _with1.SelectCommand.Parameters.Add("LOGGED_LOC_MST_PK_IN", OracleDbType.Int32).Value = HttpContext.Current.Session["LOGED_IN_LOC_FK"];
                _with1.SelectCommand.Parameters.Add("IS_ADMIN_IN", OracleDbType.Int32).Value = (isAdmin == 1 ? 1 : 0);
                _with1.SelectCommand.Parameters.Add("LOAD_FLAG_IN", LoadFlg).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("REPORT_FLAG_IN", ReportFlg).Direction = ParameterDirection.Input;
                _with1.SelectCommand.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with1.SelectCommand.Parameters.Add("MASTER_PAGE_SIZE_IN", OracleDbType.Int32).Value = (dispAll == 1 ? 100000 : RecordsPerPage);
                _with1.SelectCommand.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with1.SelectCommand.Parameters.Add("PL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Fill(ds);
                TotalPage = Convert.ToInt32(_with1.SelectCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(_with1.SelectCommand.Parameters["CURRENT_PAGE_IN"].Value);

                return ds;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }

        /// <summary>
        /// Fetches the PLBL GRP by.
        /// </summary>
        /// <param name="BL_FK">The b l_ fk.</param>
        /// <param name="JobCardFK">The job card fk.</param>
        /// <param name="LocationFK">The location fk.</param>
        /// <param name="selectedMonths">The selected months.</param>
        /// <param name="VslVoyPk">The VSL voy pk.</param>
        /// <param name="FlightNo">The flight no.</param>
        /// <param name="ShipperFK">The shipper fk.</param>
        /// <param name="ConsigneeFK">The consignee fk.</param>
        /// <param name="POLFK">The polfk.</param>
        /// <param name="PODFK">The podfk.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="Status">The status.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <param name="CommGrpFK">The comm GRP fk.</param>
        /// <param name="ExecutiveFK">The executive fk.</param>
        /// <param name="CurPK">The current pk.</param>
        /// <param name="isAdmin">The is admin.</param>
        /// <param name="txtBLID">The text blid.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="LoadFlg">The load FLG.</param>
        /// <param name="ReportFlg">The report FLG.</param>
        /// <param name="FORM_FLG">For m_ FLG.</param>
        /// <returns></returns>
        public DataSet FetchPLBLGrpBy(long BL_FK, long JobCardFK, long LocationFK, string selectedMonths, long VslVoyPk, string FlightNo, long ShipperFK, long ConsigneeFK, long POLFK, long PODFK,
        string FromDate, string ToDate, Int32 BizType, Int32 Process, Int32 CargoType, long Status, Int32 JobType, long CommGrpFK, long ExecutiveFK, Int32 CurPK,
        Int32 isAdmin, string txtBLID, Int32 CurrentPage, Int32 TotalPage, short LoadFlg, short ReportFlg, string FORM_FLG = "PPL")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyCommand = new OracleCommand();
                var _with2 = objWF.MyCommand;
                _with2.Parameters.Add("BOOKING_BL_PK_IN", OracleDbType.Int32).Value = getDefault(BL_FK, "");
                _with2.Parameters.Add("JOB_CARD_PK_IN", OracleDbType.Int32).Value = getDefault(JobCardFK, "");
                _with2.Parameters.Add("BL_NO_IN", OracleDbType.Varchar2).Value = getDefault(txtBLID, "");
                _with2.Parameters.Add("LOCATION_MST_PK_IN", OracleDbType.Int32).Value = getDefault(LocationFK, "");
                _with2.Parameters.Add("SELECTED_MONTHS_IN", OracleDbType.Varchar2).Value = getDefault(selectedMonths, "");
                _with2.Parameters.Add("VESSEL_VOY_FK_IN", OracleDbType.Int32).Value = getDefault(VslVoyPk, "");
                _with2.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2).Value = getDefault(FlightNo, "");
                _with2.Parameters.Add("SHIPPER_PK_IN", OracleDbType.Int32).Value = getDefault(ShipperFK, "");
                _with2.Parameters.Add("CONSIGNEE_PK_IN", OracleDbType.Int32).Value = getDefault(ConsigneeFK, "");
                _with2.Parameters.Add("POL_PK_IN", OracleDbType.Int32).Value = getDefault(POLFK, "");
                _with2.Parameters.Add("POD_PK_IN", OracleDbType.Int32).Value = getDefault(PODFK, "");
                _with2.Parameters.Add("FROM_DATE_IN", OracleDbType.Varchar2).Value = getDefault(FromDate, "");
                _with2.Parameters.Add("TO_DATE_IN", OracleDbType.Varchar2).Value = getDefault(ToDate, "");
                _with2.Parameters.Add("BIZTYPE_IN", OracleDbType.Int32).Value = BizType;
                _with2.Parameters.Add("PROCESS_IN", OracleDbType.Int32).Value = Process;
                _with2.Parameters.Add("CARGOTYPE_IN", OracleDbType.Int32).Value = CargoType;
                _with2.Parameters.Add("STATUS_IN", OracleDbType.Int32).Value = Status;
                _with2.Parameters.Add("JOBTYPE_IN", OracleDbType.Int32).Value = JobType;
                _with2.Parameters.Add("COMMODITY_GROUP_PK_IN", OracleDbType.Int32).Value = getDefault(CommGrpFK, "");
                _with2.Parameters.Add("EMPLOYEE_MST_PK_IN", OracleDbType.Int32).Value = getDefault(ExecutiveFK, "");
                _with2.Parameters.Add("CURRENCY_MST_PK_IN", OracleDbType.Int32).Value = CurPK;
                _with2.Parameters.Add("LOGGED_LOC_MST_PK_IN", OracleDbType.Int32).Value = HttpContext.Current.Session["LOGED_IN_LOC_FK"];
                _with2.Parameters.Add("IS_ADMIN_IN", OracleDbType.Int32).Value = (isAdmin == 1 ? 1 : 0);
                _with2.Parameters.Add("LOAD_FLAG_IN", LoadFlg).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with2.Parameters.Add("TOTAL_PAGE_IN", OracleDbType.Int32).Direction = ParameterDirection.Output;
                _with2.Parameters.Add("YEAR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Parameters.Add("MONTH_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with2.Parameters.Add("PL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (FORM_FLG == "PPL")
                {
                    ds = objWF.GetDataSet("FETCH_PROFIT_AND_LOSS_PKG", "FETCH_PL_BL_GRP");
                }
                else
                {
                    ds = objWF.GetDataSet("FETCH_PROFIT_AND_LOSS_PKG", "FETCH_ACT_PL_BL_GRP");
                }
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENT_PAGE_IN"].Value);
                DataRelation Rel_Year = new DataRelation("YEAR", new DataColumn[] { ds.Tables[0].Columns["YEAR"] }, new DataColumn[] { ds.Tables[1].Columns["YEAR"] });

                Rel_Year.Nested = true;
                ds.Relations.Add(Rel_Year);

                DataRelation Rel_Month = new DataRelation("MONTH", new DataColumn[] {
                    ds.Tables[1].Columns["YEAR"],
                    ds.Tables[1].Columns["MONTH"]
                }, new DataColumn[] {
                    ds.Tables[2].Columns["YEAR"],
                    ds.Tables[2].Columns["MONTH"]
                });

                Rel_Month.Nested = true;
                ds.Relations.Add(Rel_Month);
                return ds;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }

        #endregion " FetchPLBL "

        #region " FetchPLQR "

        /// <summary>
        /// Fetches the PLQR.
        /// </summary>
        /// <param name="BL_FK">The b l_ fk.</param>
        /// <param name="JobCardFK">The job card fk.</param>
        /// <param name="LocationFK">The location fk.</param>
        /// <param name="selectedMonths">The selected months.</param>
        /// <param name="VslVoyPk">The VSL voy pk.</param>
        /// <param name="FlightNo">The flight no.</param>
        /// <param name="ShipperFK">The shipper fk.</param>
        /// <param name="ConsigneeFK">The consignee fk.</param>
        /// <param name="POLFK">The polfk.</param>
        /// <param name="PODFK">The podfk.</param>
        /// <param name="FromDate">From date.</param>
        /// <param name="ToDate">To date.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="Status">The status.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <param name="CommGrpFK">The comm GRP fk.</param>
        /// <param name="ExecutiveFK">The executive fk.</param>
        /// <param name="CurPK">The current pk.</param>
        /// <param name="isAdmin">The is admin.</param>
        /// <param name="LoadFlg">The load FLG.</param>
        /// <param name="txtBLID">The text blid.</param>
        /// <param name="FORM_FLG">For m_ FLG.</param>
        /// <returns></returns>
        public DataSet FetchPLQR(long BL_FK, long JobCardFK, long LocationFK, string selectedMonths, long VslVoyPk, string FlightNo, long ShipperFK, long ConsigneeFK, long POLFK, long PODFK,
        string FromDate, string ToDate, Int32 BizType, Int32 Process, Int32 CargoType, long Status, Int32 JobType, long CommGrpFK, long ExecutiveFK, Int32 CurPK,
        Int32 isAdmin, short LoadFlg, string txtBLID, string FORM_FLG = "PPL")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with3 = objWF.MyDataAdapter;
                _with3.SelectCommand = new OracleCommand();
                _with3.SelectCommand.Connection = objWF.MyConnection;
                if (FORM_FLG == "PPL")
                {
                    _with3.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_PROFIT_AND_LOSS_PKG.FETCH_PL_QR";
                }
                else
                {
                    _with3.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_PROFIT_AND_LOSS_PKG.FETCH_ACT_PL_QR";
                }
                _with3.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with3.SelectCommand.Parameters.Add("BOOKING_BL_PK_IN", OracleDbType.Int32).Value = (BL_FK <= 0 ? 0 : BL_FK);
                _with3.SelectCommand.Parameters.Add("JOB_CARD_PK_IN", OracleDbType.Int32).Value = (JobCardFK <= 0 ? 0 : JobCardFK);
                _with3.SelectCommand.Parameters.Add("BL_NO_IN", OracleDbType.Varchar2).Value = getDefault(txtBLID, "");
                _with3.SelectCommand.Parameters.Add("LOCATION_MST_PK_IN", OracleDbType.Int32).Value = (LocationFK <= 0 ? 0 : LocationFK);
                _with3.SelectCommand.Parameters.Add("SELECTED_MONTHS_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(selectedMonths) ? "" : selectedMonths);
                _with3.SelectCommand.Parameters.Add("VESSEL_VOY_FK_IN", OracleDbType.Int32).Value = (VslVoyPk <= 0 ? 0 : VslVoyPk);
                _with3.SelectCommand.Parameters.Add("FLIGHT_NO_IN", OracleDbType.Varchar2).Value = getDefault(FlightNo, "");
                _with3.SelectCommand.Parameters.Add("SHIPPER_PK_IN", OracleDbType.Int32).Value = (ShipperFK <= 0 ? 0 : ShipperFK);
                _with3.SelectCommand.Parameters.Add("CONSIGNEE_PK_IN", OracleDbType.Int32).Value = (ConsigneeFK <= 0 ? 0 : ConsigneeFK);
                _with3.SelectCommand.Parameters.Add("POL_PK_IN", OracleDbType.Int32).Value = (POLFK <= 0 ? 0 : POLFK);
                _with3.SelectCommand.Parameters.Add("POD_PK_IN", OracleDbType.Int32).Value = (PODFK <= 0 ? 0 : PODFK);
                _with3.SelectCommand.Parameters.Add("FROM_DATE_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(FromDate) ? "" : FromDate);
                _with3.SelectCommand.Parameters.Add("TO_DATE_IN", OracleDbType.Varchar2).Value = (string.IsNullOrEmpty(ToDate) ? "" : ToDate);
                _with3.SelectCommand.Parameters.Add("BIZTYPE_IN", OracleDbType.Int32).Value = BizType;
                _with3.SelectCommand.Parameters.Add("PROCESS_IN", OracleDbType.Int32).Value = Process;
                _with3.SelectCommand.Parameters.Add("CARGOTYPE_IN", OracleDbType.Int32).Value = CargoType;
                _with3.SelectCommand.Parameters.Add("STATUS_IN", OracleDbType.Int32).Value = Status;
                _with3.SelectCommand.Parameters.Add("JOBTYPE_IN", OracleDbType.Int32).Value = JobType;
                _with3.SelectCommand.Parameters.Add("COMMODITY_GROUP_PK_IN", OracleDbType.Int32).Value = (CommGrpFK <= 0 ? 0 : CommGrpFK);
                _with3.SelectCommand.Parameters.Add("EMPLOYEE_MST_PK_IN", OracleDbType.Int32).Value = (ExecutiveFK <= 0 ? 0 : ExecutiveFK);
                _with3.SelectCommand.Parameters.Add("CURRENCY_MST_PK_IN", OracleDbType.Int32).Value = CurPK;
                _with3.SelectCommand.Parameters.Add("LOGGED_LOC_MST_PK_IN", OracleDbType.Int32).Value = HttpContext.Current.Session["LOGED_IN_LOC_FK"];
                _with3.SelectCommand.Parameters.Add("IS_ADMIN_IN", OracleDbType.Int32).Value = (isAdmin == 1 ? 1 : 0);
                _with3.SelectCommand.Parameters.Add("LOAD_FLAG_IN", LoadFlg).Direction = ParameterDirection.Input;
                _with3.SelectCommand.Parameters.Add("PL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with3.Fill(ds);

                return ds;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }

        #endregion " FetchPLQR "

        #region "Fecth Container Details"

        /// <summary>
        /// Fetches the jc container data.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchJCContainerData(int JobPK, string MJCPK = "", int BizType = 0, int Process = 0, int CargoType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();

            try
            {
                var _with4 = objWF.MyCommand.Parameters;
                _with4.Add("JOBCARDPK_IN", JobPK).Direction = ParameterDirection.Input;
                _with4.Add("MJCPK_IN", MJCPK).Direction = ParameterDirection.Input;
                _with4.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with4.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with4.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with4.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_PROFIT_AND_LOSS_PKG", "FETCH_CONTAINER_DATA");
                return DS;
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return new DataSet();
        }

        /// <summary>
        /// Fetches the tp container data.
        /// </summary>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public DataSet FetchTPContainerData(int JobPK, int BizType = 0, int Process = 0, int CargoType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet DS = new DataSet();

            try
            {
                var _with5 = objWF.MyCommand.Parameters;
                _with5.Add("TPNOTE_FK_IN", JobPK).Direction = ParameterDirection.Input;
                _with5.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with5.Add("PROCESS_TYPE_IN", Process).Direction = ParameterDirection.Input;
                _with5.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with5.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                DS = objWF.GetDataSet("FETCH_PROFIT_AND_LOSS_PKG", "FETCH_TPCONT_DETAILS");
                return DS;
            }
            catch (Exception sqlExp)
            {
                ErrorMessage = sqlExp.Message;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return new DataSet();
        }

        #endregion "Fecth Container Details"

        #region " FetchJobCardExpFreight "

        /// <summary>
        /// Fetches the job card freight.
        /// </summary>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="CurPK">The current pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchJobCardFreight(Int32 JobCardPK, Int32 CurPK, int BizType = 0, int Process = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with6 = objWF.MyDataAdapter;
                _with6.SelectCommand = new OracleCommand();
                _with6.SelectCommand.Connection = objWF.MyConnection;
                _with6.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_PROFIT_AND_LOSS_PKG.FECTH_FREIGHT_DETAILS";
                _with6.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with6.SelectCommand.Parameters.Add("JOBCARDPK_IN", JobCardPK).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("JOBPROFIT_IN", 0).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("CURRENCY_FK_IN", CurPK).Direction = ParameterDirection.Input;
                _with6.SelectCommand.Parameters.Add("PL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with6.Fill(ds);
                return ds;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return new DataSet();
        }

        #endregion " FetchJobCardExpFreight "

        #region " FetchJobCard Cost Details "

        /// <summary>
        /// Fetches the job card est cost.
        /// </summary>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="CurPK">The current pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchJobCardEstCost(Int32 JobCardPK, Int32 CurPK, int BizType = 0, int Process = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with7 = objWF.MyDataAdapter;
                _with7.SelectCommand = new OracleCommand();
                _with7.SelectCommand.Connection = objWF.MyConnection;
                _with7.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_PROFIT_AND_LOSS_PKG.FETCH_COST_DETAILS";
                _with7.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with7.SelectCommand.Parameters.Add("JOBCARDPK_IN", JobCardPK).Direction = ParameterDirection.Input;
                _with7.SelectCommand.Parameters.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with7.SelectCommand.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with7.SelectCommand.Parameters.Add("BASECURRENCY_IN", CurPK).Direction = ParameterDirection.Input;
                _with7.SelectCommand.Parameters.Add("PL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with7.Fill(ds);
                return ds;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return new DataSet();
        }

        #endregion " FetchJobCard Cost Details "

        #region " FetchJobCard OthFreight "

        /// <summary>
        /// Fetches the job card oth freight.
        /// </summary>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="CurPK">The current pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public object FetchJobCardOthFreight(Int32 JobCardPK, Int32 CurPK, int BizType = 0, int Process = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with8 = objWF.MyDataAdapter;
                _with8.SelectCommand = new OracleCommand();
                _with8.SelectCommand.Connection = objWF.MyConnection;
                _with8.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_PROFIT_AND_LOSS_PKG.FETCH_OTHER_DETAILS";
                _with8.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with8.SelectCommand.Parameters.Add("JOBCARDPK_IN", JobCardPK).Direction = ParameterDirection.Input;
                _with8.SelectCommand.Parameters.Add("BIZTYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with8.SelectCommand.Parameters.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with8.SelectCommand.Parameters.Add("BASECURRENCY_IN", CurPK).Direction = ParameterDirection.Input;
                _with8.SelectCommand.Parameters.Add("PL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with8.Fill(ds);

                return ds;
            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }

        #endregion " FetchJobCard OthFreight "

        #region "Save Function"

        /// <summary>
        /// Saves the specified ds freight details.
        /// </summary>
        /// <param name="dsFreightDetails">The ds freight details.</param>
        /// <param name="dsCostDetails">The ds cost details.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsOtherCharges">The ds other charges.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <param name="Cargotype">The cargotype.</param>
        /// <returns></returns>
        public ArrayList Save(DataSet dsFreightDetails, DataSet dsCostDetails, long JobCardPK, DataSet dsOtherCharges, DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null, int Cargotype = 0)
        {
            Int32 nRowCnt = default(Int32);

            Int32 RecAfct = default(Int32);

            OracleCommand insFreightDetails = new OracleCommand();
            OracleCommand updFreightDetails = new OracleCommand();
            OracleCommand delFreightDetails = new OracleCommand();

            OracleCommand insPurchaseInvDetails = new OracleCommand();
            OracleCommand updPurchaseInvDetails = new OracleCommand();
            OracleCommand delPurchaseInvDetails = new OracleCommand();

            OracleCommand insCostDetails = new OracleCommand();
            OracleCommand updCostDetails = new OracleCommand();
            OracleCommand delCostDetails = new OracleCommand();

            OracleCommand insOtherChargesDetails = new OracleCommand();
            OracleCommand updOtherChargesDetails = new OracleCommand();
            OracleCommand delOtherChargesDetails = new OracleCommand();

            OracleCommand insIncomeChargeDetails = new OracleCommand();
            OracleCommand updIncomeChargeDetails = new OracleCommand();
            OracleCommand delIncomeChargeDetails = new OracleCommand();

            OracleCommand insExpenseChargeDetails = new OracleCommand();
            OracleCommand updExpenseChargeDetails = new OracleCommand();
            OracleCommand delExpenseChargeDetails = new OracleCommand();

            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            try
            {
                //--------------------------Freight Details-----------------------------
                if (Cargotype == 4)
                {
                    var _with9 = insFreightDetails;
                    _with9.Connection = objWK.MyConnection;
                    _with9.CommandType = CommandType.StoredProcedure;
                    _with9.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_INS";

                    _with9.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with9.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", "").Direction = ParameterDirection.Input;

                    _with9.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    _with9.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                    _with9.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                    _with9.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                    _with9.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                    _with9.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURR_FK").Direction = ParameterDirection.Input;
                    _with9.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                    _with9.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("surcharge_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                    _with9.Parameters["surcharge_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                    _with9.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                    _with9.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                    _with9.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("JOB_TRN_SEA_EXP_CONT_FK_IN", OracleDbType.Int32, 1, "JOB_TRN_SEA_EXP_CONT_PK").Direction = ParameterDirection.Input;
                    _with9.Parameters["JOB_TRN_SEA_EXP_CONT_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "RATEPERBASIS").Direction = ParameterDirection.Input;
                    _with9.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_FD_PK").Direction = ParameterDirection.Output;
                    _with9.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with10 = updFreightDetails;
                    _with10.Connection = objWK.MyConnection;
                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_UPD";

                    _with10.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_fd_pk").Direction = ParameterDirection.Input;
                    _with10.Parameters["JOB_TRN_SEA_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with10.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    _with10.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                    _with10.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                    _with10.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                    _with10.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "RATEPERBASIS").Direction = ParameterDirection.Input;
                    _with10.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                    _with10.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                    _with10.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURR_FK").Direction = ParameterDirection.Input;
                    _with10.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                    _with10.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                    _with10.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                    _with10.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                    _with10.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with10.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with11 = delFreightDetails;
                    _with11.Connection = objWK.MyConnection;
                    _with11.CommandType = CommandType.StoredProcedure;
                    _with11.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_DEL";

                    _with11.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_fd_pk").Direction = ParameterDirection.Input;
                    _with11.Parameters["JOB_TRN_SEA_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with11.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with11.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with12 = objWK.MyDataAdapter;

                    _with12.InsertCommand = insFreightDetails;
                    _with12.InsertCommand.Transaction = TRAN;

                    _with12.UpdateCommand = updFreightDetails;
                    _with12.UpdateCommand.Transaction = TRAN;

                    _with12.DeleteCommand = delFreightDetails;
                    _with12.DeleteCommand.Transaction = TRAN;

                    RecAfct = _with12.Update(dsFreightDetails);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                else
                {
                    var _with13 = insFreightDetails;
                    _with13.Connection = objWK.MyConnection;
                    _with13.CommandType = CommandType.StoredProcedure;
                    _with13.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_INS";
                    var _with14 = _with13.Parameters;

                    insFreightDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    insFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "Rateperbasis").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("surcharge_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["surcharge_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("job_trn_sea_exp_cont_fk_in", "").Direction = ParameterDirection.Input;

                    insFreightDetails.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_FD_PK").Direction = ParameterDirection.Output;
                    insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with15 = updFreightDetails;
                    _with15.Connection = objWK.MyConnection;
                    _with15.CommandType = CommandType.StoredProcedure;
                    _with15.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_UPD";
                    var _with16 = _with15.Parameters;

                    updFreightDetails.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_fd_pk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["JOB_TRN_SEA_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    updFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "Rateperbasis").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with17 = delFreightDetails;
                    _with17.Connection = objWK.MyConnection;
                    _with17.CommandType = CommandType.StoredProcedure;
                    _with17.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_DEL";

                    delFreightDetails.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_fd_pk").Direction = ParameterDirection.Input;
                    delFreightDetails.Parameters["JOB_TRN_SEA_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                    delFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    delFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with18 = objWK.MyDataAdapter;

                    _with18.InsertCommand = insFreightDetails;
                    _with18.InsertCommand.Transaction = TRAN;

                    _with18.UpdateCommand = updFreightDetails;
                    _with18.UpdateCommand.Transaction = TRAN;

                    _with18.DeleteCommand = delFreightDetails;
                    _with18.DeleteCommand.Transaction = TRAN;

                    RecAfct = _with18.Update(dsFreightDetails);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                if (!SaveSecondaryServices(objWK, TRAN, Convert.ToInt32(JobCardPK), dsIncomeChargeDetails, dsExpenseChargeDetails))
                {
                    arrMessage.Add("Error while saving secondary service details");
                    return arrMessage;
                }

                var _with19 = insCostDetails;
                _with19.Connection = objWK.MyConnection;
                _with19.CommandType = CommandType.StoredProcedure;
                _with19.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_INS";
                var _with20 = _with19.Parameters;
                insCostDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Output;
                insCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with21 = updCostDetails;
                _with21.Connection = objWK.MyConnection;
                _with21.CommandType = CommandType.StoredProcedure;
                _with21.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_UPD";
                var _with22 = _with21.Parameters;

                updCostDetails.Parameters.Add("JOB_TRN_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["JOB_TRN_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 50, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with23 = delCostDetails;
                _with23.Connection = objWK.MyConnection;
                _with23.CommandType = CommandType.StoredProcedure;
                _with23.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_DEL";

                delCostDetails.Parameters.Add("JOB_TRN_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_COST_PK").Direction = ParameterDirection.Input;
                delCostDetails.Parameters["JOB_TRN_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                delCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with24 = objWK.MyDataAdapter;

                _with24.InsertCommand = insCostDetails;
                _with24.InsertCommand.Transaction = TRAN;

                _with24.UpdateCommand = updCostDetails;
                _with24.UpdateCommand.Transaction = TRAN;

                _with24.DeleteCommand = delCostDetails;
                _with24.DeleteCommand.Transaction = TRAN;

                RecAfct = _with24.Update(dsCostDetails.Tables[0]);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }

                foreach (DataRow _rOth in dsOtherCharges.Tables[0].Rows)
                {
                    if (_rOth.RowState != DataRowState.Deleted)
                    {
                        string PayType = _rOth["Payment_Type"].ToString();
                        if (PayType.ToUpper() == "PREPAID")
                        {
                            _rOth["Payment_Type"] = "1";
                        }
                        else if (PayType.ToUpper() == "COLLECT")
                        {
                            _rOth["Payment_Type"] = "2";
                        }
                    }
                }
                var _with25 = insOtherChargesDetails;
                _with25.Connection = objWK.MyConnection;
                _with25.CommandType = CommandType.StoredProcedure;
                _with25.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_OTH_CHRG_INS";

                _with25.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                _with25.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                _with25.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with25.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "Payment_Type").Direction = ParameterDirection.Input;
                _with25.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                _with25.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                _with25.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with25.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                _with25.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with25.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                _with25.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with25.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                _with25.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                _with25.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                _with25.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                _with25.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                _with25.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                _with25.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_EXP_OTH_PK").Direction = ParameterDirection.Output;
                _with25.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with26 = updOtherChargesDetails;
                _with26.Connection = objWK.MyConnection;
                _with26.CommandType = CommandType.StoredProcedure;
                _with26.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_OTH_CHRG_UPD";

                _with26.Parameters.Add("JOB_TRN_SEA_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_oth_pk").Direction = ParameterDirection.Input;
                _with26.Parameters["JOB_TRN_SEA_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with26.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                _with26.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                _with26.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with26.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "Payment_Type").Direction = ParameterDirection.Input;
                _with26.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                _with26.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                _with26.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with26.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                _with26.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with26.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                _with26.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                _with26.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                _with26.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                _with26.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                _with26.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                _with26.Parameters.Add("PRINT_ON_MBL_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                _with26.Parameters["PRINT_ON_MBL_IN"].SourceVersion = DataRowVersion.Current;

                _with26.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with26.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with27 = delOtherChargesDetails;
                _with27.Connection = objWK.MyConnection;
                _with27.CommandType = CommandType.StoredProcedure;
                _with27.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_OTH_CHRG_DEL";

                _with27.Parameters.Add("JOB_TRN_SEA_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_exp_oth_pk").Direction = ParameterDirection.Input;
                _with27.Parameters["JOB_TRN_SEA_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with27.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with27.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with28 = objWK.MyDataAdapter;

                _with28.InsertCommand = insOtherChargesDetails;
                _with28.InsertCommand.Transaction = TRAN;

                _with28.UpdateCommand = updOtherChargesDetails;
                _with28.UpdateCommand.Transaction = TRAN;

                _with28.DeleteCommand = delOtherChargesDetails;
                _with28.DeleteCommand.Transaction = TRAN;

                RecAfct = _with28.Update(dsOtherCharges);

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

        /// <summary>
        /// Saves the secondary services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool SaveSecondaryServices(WorkFlow objWK, OracleTransaction TRAN, int JobCardPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            if ((dsIncomeChargeDetails != null))
            {
                //----------------------------------Income Charge Details----------------------------------
                Quantum_QFOR.cls_SeaBookingEntry objVesselVoyage = new Quantum_QFOR.cls_SeaBookingEntry();
                objVesselVoyage.ConfigurationPK = M_Configuration_PK;
                objVesselVoyage.CREATED_BY = M_CREATED_BY_FK;
                foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                {
                    int Frt_Pk = 0;
                    try
                    {
                        Frt_Pk = Convert.ToInt32(ri["JOB_TRN_SEA_EXP_FD_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Frt_Pk = 0;
                    }
                    var _with29 = objWK.MyCommand;
                    _with29.Parameters.Clear();
                    _with29.Transaction = TRAN;
                    _with29.CommandType = CommandType.StoredProcedure;
                    if (Frt_Pk > 0)
                    {
                        _with29.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_UPD";
                        _with29.Parameters.Add("JOB_TRN_SEA_EXP_FD_PK_IN", ri["JOB_TRN_SEA_EXP_FD_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with29.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_FD_INS";
                        _with29.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                        _with29.Parameters.Add("JOB_TRN_SEA_EXP_CONT_FK_IN", "").Direction = ParameterDirection.Input;
                    }
                    _with29.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("BASIS_IN", getDefault(ri["BASIS"], "")).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("PRINT_ON_MBL_IN", 1).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("BASIS_FK_IN", getDefault(ri["BASIS_PK"], "")).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("EXCHANGE_RATE_IN", getDefault(ri["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("RATE_PERBASIS_IN", getDefault(ri["RATEPERBASIS"], "")).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("SURCHARGE_IN", "").Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("QUANTITY_IN", getDefault(ri["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("SERVICE_MST_FK_IN", ri["SERVICE_MST_PK"]).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with29.ExecuteNonQuery();
                        if (Frt_Pk == 0)
                        {
                            _with29.Parameters.Clear();
                            _with29.CommandType = CommandType.StoredProcedure;
                            _with29.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_FRT_SEQ_CURRVAL";
                            _with29.Parameters.Add("BIZ_IN", 2).Direction = ParameterDirection.Input;
                            _with29.Parameters.Add("PROCESS_IN", 1).Direction = ParameterDirection.Input;
                            _with29.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with29.ExecuteNonQuery();
                            Frt_Pk = Convert.ToInt32(_with29.Parameters["RETURN_VALUE"].Value);
                            ri["JOB_TRN_SEA_EXP_FD_PK"] = Frt_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            //----------------------------------Expense Charge Details----------------------------------
            if ((dsExpenseChargeDetails != null))
            {
                foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                {
                    int Cost_Pk = 0;
                    try
                    {
                        Cost_Pk = Convert.ToInt32(re["JOB_TRN_SEA_EXP_COST_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Cost_Pk = 0;
                    }
                    var _with30 = objWK.MyCommand;
                    _with30.Parameters.Clear();
                    _with30.Transaction = TRAN;
                    _with30.CommandType = CommandType.StoredProcedure;
                    if (Cost_Pk > 0)
                    {
                        _with30.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_UPD";
                        _with30.Parameters.Add("JOB_TRN_EST_PK_IN", re["JOB_TRN_SEA_EXP_COST_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with30.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_PKG.JOB_TRN_SEA_EXP_COST_INS";
                        _with30.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }

                    _with30.Parameters.Add("JOB_CARD_SEA_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("VENDOR_MST_FK_IN", re["SUPPLIER_MST_PK"]).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("COST_ELEMENT_FK_IN", re["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("LOCATION_FK_IN", re["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("VENDOR_KEY_IN", re["SUPPLIER_MST_ID"]).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("PTMT_TYPE_IN", re["PTMT_TYPE"]).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("CURRENCY_MST_FK_IN", re["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("ESTIMATED_COST_IN", re["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("TOTAL_COST_IN", re["TOTAL_COST"]).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("BASIS_FK_IN", re["DD_VALUE"]).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("RATEPERBASIS_IN", re["RATEPERBASIS"]).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("QUANTITY_IN", getDefault(re["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("EXCHANGE_RATE_IN", getDefault(re["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("EXT_INT_FLAG_IN", getDefault(re["EXT_INT_FLAG"], 2)).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("SERVICE_MST_FK_IN", re["SERVICE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with30.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with30.ExecuteNonQuery();
                        if (Cost_Pk == 0)
                        {
                            _with30.Parameters.Clear();
                            _with30.CommandType = CommandType.StoredProcedure;
                            _with30.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_COST_SEQ_CURRVAL";
                            _with30.Parameters.Add("BIZ_IN", 2).Direction = ParameterDirection.Input;
                            _with30.Parameters.Add("PROCESS_IN", 1).Direction = ParameterDirection.Input;
                            _with30.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with30.ExecuteNonQuery();
                            Cost_Pk = Convert.ToInt32(_with30.Parameters["RETURN_VALUE"].Value);
                            re["JOB_TRN_SEA_EXP_COST_PK"] = Cost_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            ClearRemovedServices(objWK, TRAN, JobCardPK, dsIncomeChargeDetails, dsExpenseChargeDetails);
            return true;
        }

        /// <summary>
        /// Clears the removed services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool ClearRemovedServices(WorkFlow objWK, OracleTransaction TRAN, int JobCardPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            string SelectedFrtPks = "";
            string SelectedCostPks = "";
            if (JobCardPK > 0)
            {
                try
                {
                    foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedFrtPks))
                        {
                            SelectedFrtPks = getDefault(ri["JOB_TRN_SEA_EXP_FD_PK"], 0).ToString();
                        }
                        else
                        {
                            SelectedFrtPks += "," + getDefault(ri["JOB_TRN_SEA_EXP_FD_PK"], 0);
                        }
                    }
                    foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedCostPks))
                        {
                            SelectedCostPks = getDefault(re["JOB_TRN_SEA_EXP_COST_PK"], 0).ToString();
                        }
                        else
                        {
                            SelectedCostPks += "," + getDefault(re["JOB_TRN_SEA_EXP_COST_PK"], 0);
                        }
                    }

                    var _with31 = objWK.MyCommand;
                    _with31.Transaction = TRAN;
                    _with31.CommandType = CommandType.StoredProcedure;
                    _with31.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.DELETE_SEA_EXP_SEC_CHG_EXCEPT";
                    _with31.Parameters.Clear();
                    _with31.Parameters.Add("JOB_CARD_SEA_EXP_PK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("JOB_TRN_SEA_EXP_FD_PKS", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("JOB_TRN_SEA_EXP_COST_PKS", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
                    _with31.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    _with31.ExecuteNonQuery();
                    //TRAN.Commit()
                }
                catch (OracleException oraexp)
                {
                    TRAN.Rollback();
                    //Throw oraexp
                }
                catch (Exception ex)
                {
                    TRAN.Rollback();
                    //Throw ex
                }
                finally
                {
                    //objwk.CloseConnection()
                }
            }
            return false;
        }

        #endregion "Save Function"

        #region "Save SEA IMP Function"

        /// <summary>
        /// Saves the sea imp.
        /// </summary>
        /// <param name="dsFreightDetails">The ds freight details.</param>
        /// <param name="dsCostDetails">The ds cost details.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsOtherCharges">The ds other charges.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <returns></returns>
        public ArrayList SaveSeaImp(DataSet dsFreightDetails, DataSet dsCostDetails, long JobCardPK, DataSet dsOtherCharges, DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null, int CargoType = 0)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            Int32 RecAfct = default(Int32);

            OracleCommand insFreightDetails = new OracleCommand();
            OracleCommand updFreightDetails = new OracleCommand();
            OracleCommand delFreightDetails = new OracleCommand();

            OracleCommand insCostDetails = new OracleCommand();
            OracleCommand updCostDetails = new OracleCommand();
            OracleCommand delCostDetails = new OracleCommand();

            OracleCommand insOtherChargesDetails = new OracleCommand();
            OracleCommand updOtherChargesDetails = new OracleCommand();
            OracleCommand delOtherChargesDetails = new OracleCommand();

            try
            {
                if (CargoType == 4)
                {
                    if ((dsFreightDetails != null))
                    {
                        var _with32 = insFreightDetails;
                        _with32.Connection = objWK.MyConnection;
                        _with32.CommandType = CommandType.StoredProcedure;
                        _with32.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_INS";
                        var _with33 = _with32.Parameters;

                        insFreightDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                        insFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", "").Direction = ParameterDirection.Input;

                        insFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "Frtpayer_mst_fk").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "RATEPERBASIS").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 50, "SURCHARGE").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURR_FK").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                        //'changed

                        insFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("JOB_CARD_SEA_IMP_CONT_PK_IN", OracleDbType.Int32, 10, "JOB_CARD_SEA_IMP_CONT_PK").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["JOB_CARD_SEA_IMP_CONT_PK_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_IMP_FD_PK").Direction = ParameterDirection.Output;
                        insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                        var _with34 = updFreightDetails;
                        _with34.Connection = objWK.MyConnection;
                        _with34.CommandType = CommandType.StoredProcedure;
                        _with34.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_UPD";
                        var _with35 = _with34.Parameters;

                        updFreightDetails.Parameters.Add("JOB_TRN_SEA_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_fd_pk").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["JOB_TRN_SEA_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                        updFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", "").Direction = ParameterDirection.Input;

                        updFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "Frtpayer_mst_fk").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "RATEPERBASIS").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURR_FK").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                        //'curency

                        updFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 50, "SURCHARGE").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                        var _with36 = delFreightDetails;
                        _with36.Connection = objWK.MyConnection;
                        _with36.CommandType = CommandType.StoredProcedure;
                        _with36.CommandText = objWK.MyUserName + ".BB_JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_DEL";

                        delFreightDetails.Parameters.Add("JOB_TRN_SEA_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_fd_pk").Direction = ParameterDirection.Input;
                        delFreightDetails.Parameters["JOB_TRN_SEA_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                        delFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        delFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                        var _with37 = objWK.MyDataAdapter;

                        _with37.InsertCommand = insFreightDetails;
                        _with37.InsertCommand.Transaction = TRAN;

                        _with37.UpdateCommand = updFreightDetails;
                        _with37.UpdateCommand.Transaction = TRAN;

                        _with37.DeleteCommand = delFreightDetails;
                        _with37.DeleteCommand.Transaction = TRAN;

                        RecAfct = _with37.Update(dsFreightDetails.Tables[0]);

                        if (arrMessage.Count > 0)
                        {
                            TRAN.Rollback();
                            return arrMessage;
                        }
                    }
                }
                else
                {
                    if ((dsFreightDetails != null))
                    {
                        var _with38 = insFreightDetails;
                        _with38.Connection = objWK.MyConnection;
                        _with38.CommandType = CommandType.StoredProcedure;
                        _with38.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_INS";
                        var _with39 = _with38.Parameters;

                        insFreightDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                        insFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "Frtpayer_mst_fk").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 100, "roe").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 50, "SURCHARGE").Direction = ParameterDirection.Input;
                        insFreightDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                        insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_IMP_FD_PK").Direction = ParameterDirection.Output;
                        insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                        var _with40 = updFreightDetails;
                        _with40.Connection = objWK.MyConnection;
                        _with40.CommandType = CommandType.StoredProcedure;
                        _with40.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_UPD";
                        var _with41 = _with40.Parameters;

                        updFreightDetails.Parameters.Add("JOB_TRN_SEA_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_fd_pk").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["JOB_TRN_SEA_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                        updFreightDetails.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", OracleDbType.Int32, 10, "container_type_mst_fk").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "Frtpayer_mst_fk").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                        updFreightDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                        updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                        var _with42 = delFreightDetails;
                        _with42.Connection = objWK.MyConnection;
                        _with42.CommandType = CommandType.StoredProcedure;
                        _with42.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_DEL";

                        delFreightDetails.Parameters.Add("JOB_TRN_SEA_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_fd_pk").Direction = ParameterDirection.Input;
                        delFreightDetails.Parameters["JOB_TRN_SEA_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                        delFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        delFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                        var _with43 = objWK.MyDataAdapter;

                        _with43.InsertCommand = insFreightDetails;
                        _with43.InsertCommand.Transaction = TRAN;

                        _with43.UpdateCommand = updFreightDetails;
                        _with43.UpdateCommand.Transaction = TRAN;

                        _with43.DeleteCommand = delFreightDetails;
                        _with43.DeleteCommand.Transaction = TRAN;

                        RecAfct = _with43.Update(dsFreightDetails.Tables[0]);

                        if (arrMessage.Count > 0)
                        {
                            TRAN.Rollback();
                            return arrMessage;
                        }
                    }
                }
                if ((dsCostDetails != null))
                {
                    var _with44 = insCostDetails;
                    _with44.Connection = objWK.MyConnection;
                    _with44.CommandType = CommandType.StoredProcedure;
                    _with44.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_INS";
                    var _with45 = _with44.Parameters;
                    insCostDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    insCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                    insCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                    insCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                    insCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                    insCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                    insCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                    insCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    insCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                    insCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                    insCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                    insCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                    insCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                    //'surcharge
                    insCostDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                    insCostDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                    insCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_SEA_IMP_COST_PK").Direction = ParameterDirection.Output;
                    insCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with46 = updCostDetails;
                    _with46.Connection = objWK.MyConnection;
                    _with46.CommandType = CommandType.StoredProcedure;
                    _with46.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_UPD";
                    var _with47 = _with46.Parameters;

                    updCostDetails.Parameters.Add("JOB_TRN_IMP_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_SEA_IMP_COST_PK").Direction = ParameterDirection.Input;
                    updCostDetails.Parameters["JOB_TRN_IMP_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updCostDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    updCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                    updCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 50, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                    updCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                    updCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                    updCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                    updCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                    updCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    updCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                    updCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                    updCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                    updCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                    updCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                    //'surcharge
                    updCostDetails.Parameters.Add("SURCHARGE_IN", OracleDbType.Varchar2, 100, "SURCHARGE").Direction = ParameterDirection.Input;
                    updCostDetails.Parameters["SURCHARGE_IN"].SourceVersion = DataRowVersion.Current;

                    updCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "CLng(RETURN_VALUE)").Direction = ParameterDirection.Output;
                    updCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with48 = delCostDetails;
                    _with48.Connection = objWK.MyConnection;
                    _with48.CommandType = CommandType.StoredProcedure;
                    _with48.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_DEL";

                    delCostDetails.Parameters.Add("JOB_TRN_IMP_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_SEA_IMP_COST_PK").Direction = ParameterDirection.Input;
                    delCostDetails.Parameters["JOB_TRN_IMP_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    delCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "CLng(RETURN_VALUE)").Direction = ParameterDirection.Output;
                    delCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with49 = objWK.MyDataAdapter;

                    _with49.InsertCommand = insCostDetails;
                    _with49.InsertCommand.Transaction = TRAN;

                    _with49.UpdateCommand = updCostDetails;
                    _with49.UpdateCommand.Transaction = TRAN;

                    _with49.DeleteCommand = delCostDetails;
                    _with49.DeleteCommand.Transaction = TRAN;

                    RecAfct = _with49.Update(dsCostDetails.Tables[0]);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                //'End Koteshwari
                if ((dsIncomeChargeDetails != null) & (dsExpenseChargeDetails != null))
                {
                    if (!SaveSeaImpSecondaryServices(objWK, TRAN, Convert.ToInt32(JobCardPK), dsIncomeChargeDetails, dsExpenseChargeDetails))
                    {
                        arrMessage.Add("Error while saving secondary service details");
                        return arrMessage;
                    }
                }

                if ((dsOtherCharges != null))
                {
                    var _with50 = insOtherChargesDetails;
                    _with50.Connection = objWK.MyConnection;
                    _with50.CommandType = CommandType.StoredProcedure;
                    _with50.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_OTH_CHRG_INS";
                    var _with51 = _with50.Parameters;

                    insOtherChargesDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    insOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    insOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                    insOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    //latha

                    insOtherChargesDetails.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "PaymentType").Direction = ParameterDirection.Input;
                    insOtherChargesDetails.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    insOtherChargesDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                    insOtherChargesDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insOtherChargesDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "Frtpayer_mst_fk").Direction = ParameterDirection.Input;
                    insOtherChargesDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    //end

                    insOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                    insOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                    insOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                    insOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    insOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "job_trn_sea_imp_oth_pk").Direction = ParameterDirection.Output;
                    insOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with52 = updOtherChargesDetails;
                    _with52.Connection = objWK.MyConnection;
                    _with52.CommandType = CommandType.StoredProcedure;
                    _with52.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_OTH_CHRG_UPD";
                    var _with53 = _with52.Parameters;

                    updOtherChargesDetails.Parameters.Add("JOB_TRN_SEA_IMP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_oth_pk").Direction = ParameterDirection.Input;
                    updOtherChargesDetails.Parameters["JOB_TRN_SEA_IMP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updOtherChargesDetails.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    updOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    updOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                    updOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    //latha

                    updOtherChargesDetails.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "PaymentType").Direction = ParameterDirection.Input;
                    updOtherChargesDetails.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    updOtherChargesDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                    updOtherChargesDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updOtherChargesDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_mst_fk").Direction = ParameterDirection.Input;
                    updOtherChargesDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    //end

                    updOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                    updOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                    updOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                    updOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    updOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with54 = delOtherChargesDetails;
                    _with54.Connection = objWK.MyConnection;
                    _with54.CommandType = CommandType.StoredProcedure;
                    _with54.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_OTH_CHRG_DEL";

                    delOtherChargesDetails.Parameters.Add("JOB_TRN_SEA_IMP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_sea_imp_oth_pk").Direction = ParameterDirection.Input;
                    delOtherChargesDetails.Parameters["JOB_TRN_SEA_IMP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                    delOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    delOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with55 = objWK.MyDataAdapter;

                    _with55.InsertCommand = insOtherChargesDetails;
                    _with55.InsertCommand.Transaction = TRAN;

                    _with55.UpdateCommand = updOtherChargesDetails;
                    _with55.UpdateCommand.Transaction = TRAN;

                    _with55.DeleteCommand = delOtherChargesDetails;
                    _with55.DeleteCommand.Transaction = TRAN;

                    RecAfct = _with55.Update(dsOtherCharges);
                }
                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    TRAN.Commit();
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
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

        /// <summary>
        /// Saves the sea imp secondary services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool SaveSeaImpSecondaryServices(WorkFlow objWK, OracleTransaction TRAN, int JobCardPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            if ((dsIncomeChargeDetails != null))
            {
                //----------------------------------Income Charge Details----------------------------------
                foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                {
                    int Frt_Pk = 0;
                    try
                    {
                        Frt_Pk = Convert.ToInt32(ri["JOB_TRN_SEA_IMP_FD_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Frt_Pk = 0;
                    }
                    var _with56 = objWK.MyCommand;
                    _with56.Parameters.Clear();
                    _with56.Transaction = TRAN;
                    _with56.CommandType = CommandType.StoredProcedure;
                    if (Frt_Pk > 0)
                    {
                        _with56.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_UPD";
                        _with56.Parameters.Add("JOB_TRN_SEA_IMP_FD_PK_IN", ri["JOB_TRN_SEA_IMP_FD_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with56.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_FD_INS";
                        _with56.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }
                    _with56.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("BASIS_IN", getDefault(ri["BASIS"], "")).Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("BASIS_FK_IN", getDefault(ri["BASIS_PK"], "")).Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("EXCHANGE_RATE_IN", getDefault(ri["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("RATE_PERBASIS_IN", getDefault(ri["RATEPERBASIS"], "")).Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("SURCHARGE_IN", "").Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("QUANTITY_IN", getDefault(ri["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("SERVICE_MST_FK_IN", ri["SERVICE_MST_PK"]).Direction = ParameterDirection.Input;
                    _with56.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with56.ExecuteNonQuery();
                        if (Frt_Pk == 0)
                        {
                            _with56.Parameters.Clear();
                            _with56.CommandType = CommandType.StoredProcedure;
                            _with56.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_FRT_SEQ_CURRVAL";
                            _with56.Parameters.Add("BIZ_IN", 2).Direction = ParameterDirection.Input;
                            _with56.Parameters.Add("PROCESS_IN", 2).Direction = ParameterDirection.Input;
                            _with56.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with56.ExecuteNonQuery();
                            Frt_Pk = Convert.ToInt32(_with56.Parameters["RETURN_VALUE"].Value);
                            ri["JOB_TRN_SEA_IMP_FD_PK"] = Frt_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            //----------------------------------Expense Charge Details----------------------------------
            if ((dsExpenseChargeDetails != null))
            {
                foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                {
                    int Cost_Pk = 0;
                    try
                    {
                        Cost_Pk = Convert.ToInt32(re["JOB_TRN_SEA_IMP_COST_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Cost_Pk = 0;
                    }
                    var _with57 = objWK.MyCommand;
                    _with57.Parameters.Clear();
                    _with57.Transaction = TRAN;
                    _with57.CommandType = CommandType.StoredProcedure;
                    if (Cost_Pk > 0)
                    {
                        _with57.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_UPD";
                        _with57.Parameters.Add("JOB_TRN_IMP_EST_PK_IN", re["JOB_TRN_SEA_IMP_COST_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with57.CommandText = objWK.MyUserName + ".JOB_CARD_SEA_IMP_TBL_PKG.JOB_TRN_SEA_IMP_COST_INS";
                        _with57.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }
                    _with57.Parameters.Add("JOB_CARD_SEA_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("VENDOR_MST_FK_IN", re["SUPPLIER_MST_PK"]).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("COST_ELEMENT_FK_IN", re["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("LOCATION_FK_IN", re["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("VENDOR_KEY_IN", re["SUPPLIER_MST_ID"]).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("PTMT_TYPE_IN", re["PTMT_TYPE"]).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("CURRENCY_MST_FK_IN", re["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("ESTIMATED_COST_IN", re["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("TOTAL_COST_IN", re["TOTAL_COST"]).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("BASIS_FK_IN", re["DD_VALUE"]).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("RATEPERBASIS_IN", re["RATEPERBASIS"]).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("QUANTITY_IN", getDefault(re["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("EXCHANGE_RATE_IN", getDefault(re["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("EXT_INT_FLAG_IN", getDefault(re["EXT_INT_FLAG"], 2)).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("SERVICE_MST_FK_IN", re["SERVICE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with57.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with57.ExecuteNonQuery();
                        if (Cost_Pk == 0)
                        {
                            _with57.Parameters.Clear();
                            _with57.CommandType = CommandType.StoredProcedure;
                            _with57.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_COST_SEQ_CURRVAL";
                            _with57.Parameters.Add("BIZ_IN", 2).Direction = ParameterDirection.Input;
                            _with57.Parameters.Add("PROCESS_IN", 2).Direction = ParameterDirection.Input;
                            _with57.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with57.ExecuteNonQuery();
                            Cost_Pk = Convert.ToInt32(_with57.Parameters["RETURN_VALUE"].Value);
                            re["JOB_TRN_SEA_IMP_COST_PK"] = Cost_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            ClearSeaImpRemovedServices(objWK, TRAN, JobCardPK, dsIncomeChargeDetails, dsExpenseChargeDetails);
            return true;
        }

        /// <summary>
        /// Clears the sea imp removed services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool ClearSeaImpRemovedServices(WorkFlow objWK, OracleTransaction TRAN, int JobCardPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            string SelectedFrtPks = "";
            string SelectedCostPks = "";
            if (JobCardPK > 0)
            {
                try
                {
                    if ((dsIncomeChargeDetails != null))
                    {
                        foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                        {
                            if (string.IsNullOrEmpty(SelectedFrtPks))
                            {
                                SelectedFrtPks = Convert.ToString(ri["JOB_TRN_SEA_IMP_FD_PK"]);
                            }
                            else
                            {
                                SelectedFrtPks += "," + ri["JOB_TRN_SEA_IMP_FD_PK"];
                            }
                        }
                        foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                        {
                            if (string.IsNullOrEmpty(SelectedCostPks))
                            {
                                SelectedCostPks = Convert.ToString(re["JOB_TRN_SEA_IMP_COST_PK"]);
                            }
                            else
                            {
                                SelectedCostPks += "," + re["JOB_TRN_SEA_IMP_COST_PK"];
                            }
                        }

                        var _with58 = objWK.MyCommand;
                        _with58.Transaction = TRAN;
                        _with58.CommandType = CommandType.StoredProcedure;
                        _with58.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.DELETE_SEA_IMP_SEC_CHG_EXCEPT";
                        _with58.Parameters.Clear();
                        _with58.Parameters.Add("JOB_CARD_SEA_IMP_PK_IN", JobCardPK).Direction = ParameterDirection.Input;
                        _with58.Parameters.Add("JOB_TRN_SEA_IMP_FD_PKS", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
                        _with58.Parameters.Add("JOB_TRN_SEA_IMP_COST_PKS", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
                        _with58.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                        _with58.ExecuteNonQuery();
                    }
                }
                catch (OracleException oraexp)
                {
                    TRAN.Rollback();
                }
                catch (Exception ex)
                {
                    TRAN.Rollback();
                }
                finally
                {
                }
            }
            return false;
        }

        #endregion "Save SEA IMP Function"

        #region "Save Air Exp Function"

        /// <summary>
        /// Saves the air exp.
        /// </summary>
        /// <param name="dsFreightDetails">The ds freight details.</param>
        /// <param name="dsCostDetails">The ds cost details.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsOtherCharges">The ds other charges.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public ArrayList SaveAirExp(DataSet dsFreightDetails, DataSet dsCostDetails, long JobCardPK, DataSet dsOtherCharges, DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            Int32 RecAfct = default(Int32);

            OracleCommand insFreightDetails = new OracleCommand();
            OracleCommand updFreightDetails = new OracleCommand();
            OracleCommand delFreightDetails = new OracleCommand();

            OracleCommand insCostDetails = new OracleCommand();
            OracleCommand updCostDetails = new OracleCommand();
            OracleCommand delCostDetails = new OracleCommand();

            OracleCommand insOtherChargesDetails = new OracleCommand();
            OracleCommand updOtherChargesDetails = new OracleCommand();
            OracleCommand delOtherChargesDetails = new OracleCommand();

            try
            {
                var _with59 = insFreightDetails;
                _with59.Connection = objWK.MyConnection;
                _with59.CommandType = CommandType.StoredProcedure;
                _with59.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_FD_INS";
                var _with60 = _with59.Parameters;

                insFreightDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "Rateperbasis").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("PRINT_ON_MAWB_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                insFreightDetails.Parameters["PRINT_ON_MAWB_IN"].SourceVersion = DataRowVersion.Current;

                insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_EXP_FD_PK").Direction = ParameterDirection.Output;
                insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with61 = updFreightDetails;
                _with61.Connection = objWK.MyConnection;
                _with61.CommandType = CommandType.StoredProcedure;
                _with61.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_FD_UPD";
                var _with62 = _with61.Parameters;

                updFreightDetails.Parameters.Add("JOB_TRN_AIR_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_fd_pk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["JOB_TRN_AIR_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("RATE_PERBASIS_IN", OracleDbType.Int32, 10, "Rateperbasis").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["RATE_PERBASIS_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("PRINT_ON_MAWB_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                updFreightDetails.Parameters["PRINT_ON_MAWB_IN"].SourceVersion = DataRowVersion.Current;

                updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with63 = delFreightDetails;
                _with63.Connection = objWK.MyConnection;
                _with63.CommandType = CommandType.StoredProcedure;
                _with63.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_FD_DEL";

                delFreightDetails.Parameters.Add("JOB_TRN_AIR_EXP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_fd_pk").Direction = ParameterDirection.Input;
                delFreightDetails.Parameters["JOB_TRN_AIR_EXP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                delFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with64 = objWK.MyDataAdapter;

                _with64.InsertCommand = insFreightDetails;
                _with64.InsertCommand.Transaction = TRAN;

                _with64.UpdateCommand = updFreightDetails;
                _with64.UpdateCommand.Transaction = TRAN;

                _with64.DeleteCommand = delFreightDetails;
                _with64.DeleteCommand.Transaction = TRAN;

                RecAfct = _with64.Update(dsFreightDetails);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }

                if (!SaveAirExpSecondaryServices(objWK, TRAN, Convert.ToInt32(JobCardPK), dsIncomeChargeDetails, dsExpenseChargeDetails))
                {
                    arrMessage.Add("Error while saving secondary service details");
                    return arrMessage;
                }

                var _with65 = insCostDetails;
                _with65.Connection = objWK.MyConnection;
                _with65.CommandType = CommandType.StoredProcedure;
                _with65.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_COST_INS";
                var _with66 = _with65.Parameters;
                insCostDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                insCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                insCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_EXP_COST_PK").Direction = ParameterDirection.Output;
                insCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with67 = updCostDetails;
                _with67.Connection = objWK.MyConnection;
                _with67.CommandType = CommandType.StoredProcedure;
                _with67.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_COST_UPD";
                var _with68 = _with67.Parameters;

                updCostDetails.Parameters.Add("JOB_TRN_AIR_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_AIR_EXP_COST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["JOB_TRN_AIR_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updCostDetails.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 50, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                updCostDetails.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                updCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with69 = delCostDetails;
                _with69.Connection = objWK.MyConnection;
                _with69.CommandType = CommandType.StoredProcedure;
                _with69.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_COST_DEL";

                delCostDetails.Parameters.Add("JOB_TRN_AIR_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_AIR_EXP_COST_PK").Direction = ParameterDirection.Input;
                delCostDetails.Parameters["JOB_TRN_AIR_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                delCostDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "CLng(RETURN_VALUE)").Direction = ParameterDirection.Output;
                delCostDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with70 = objWK.MyDataAdapter;

                _with70.InsertCommand = insCostDetails;
                _with70.InsertCommand.Transaction = TRAN;

                _with70.UpdateCommand = updCostDetails;
                _with70.UpdateCommand.Transaction = TRAN;

                _with70.DeleteCommand = delCostDetails;
                _with70.DeleteCommand.Transaction = TRAN;

                RecAfct = _with70.Update(dsCostDetails.Tables[0]);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    return arrMessage;
                }

                var _with71 = insOtherChargesDetails;
                _with71.Connection = objWK.MyConnection;
                _with71.CommandType = CommandType.StoredProcedure;
                _with71.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_OTH_CHRG_INS";
                var _with72 = _with71.Parameters;

                insOtherChargesDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                insOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "Payment_Type").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("PRINT_ON_MAWB_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                insOtherChargesDetails.Parameters["PRINT_ON_MAWB_IN"].SourceVersion = DataRowVersion.Current;

                insOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "job_trn_air_exp_oth_pk").Direction = ParameterDirection.Output;
                insOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with73 = updOtherChargesDetails;
                _with73.Connection = objWK.MyConnection;
                _with73.CommandType = CommandType.StoredProcedure;
                _with73.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_OTH_CHRG_UPD";
                var _with74 = _with73.Parameters;

                updOtherChargesDetails.Parameters.Add("JOB_TRN_AIR_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_oth_pk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["JOB_TRN_AIR_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                updOtherChargesDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "Payment_Type").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_mst_fk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_cust_mst_fk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("PRINT_ON_MAWB_IN", OracleDbType.Int32, 1, "Print").Direction = ParameterDirection.Input;
                updOtherChargesDetails.Parameters["PRINT_ON_MAWB_IN"].SourceVersion = DataRowVersion.Current;

                updOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with75 = delOtherChargesDetails;
                _with75.Connection = objWK.MyConnection;
                _with75.CommandType = CommandType.StoredProcedure;
                _with75.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_OTH_CHRG_DEL";

                delOtherChargesDetails.Parameters.Add("JOB_TRN_AIR_EXP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_air_exp_oth_pk").Direction = ParameterDirection.Input;
                delOtherChargesDetails.Parameters["JOB_TRN_AIR_EXP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                delOtherChargesDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                delOtherChargesDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with76 = objWK.MyDataAdapter;

                _with76.InsertCommand = insOtherChargesDetails;
                _with76.InsertCommand.Transaction = TRAN;

                _with76.UpdateCommand = updOtherChargesDetails;
                _with76.UpdateCommand.Transaction = TRAN;

                _with76.DeleteCommand = delOtherChargesDetails;
                _with76.DeleteCommand.Transaction = TRAN;

                RecAfct = _with76.Update(dsOtherCharges);

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
                throw oraexp;
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

        /// <summary>
        /// Saves the air exp secondary services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool SaveAirExpSecondaryServices(WorkFlow objWK, OracleTransaction TRAN, int JobCardPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            if ((dsIncomeChargeDetails != null))
            {
                //----------------------------------Income Charge Details----------------------------------
                foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                {
                    int Frt_Pk = 0;
                    try
                    {
                        Frt_Pk = Convert.ToInt32(ri["JOB_TRN_AIR_EXP_FD_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Frt_Pk = 0;
                    }
                    var _with77 = objWK.MyCommand;
                    _with77.Parameters.Clear();
                    _with77.Transaction = TRAN;
                    _with77.CommandType = CommandType.StoredProcedure;
                    if (Frt_Pk > 0)
                    {
                        _with77.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_FD_UPD";
                        _with77.Parameters.Add("JOB_TRN_AIR_EXP_FD_PK_IN", ri["JOB_TRN_AIR_EXP_FD_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with77.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_FD_INS";
                        _with77.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }
                    _with77.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("BASIS_IN", getDefault(ri["BASIS"], "")).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("PRINT_ON_MAWB_IN", 1).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("BASIS_FK_IN", getDefault(ri["BASIS_PK"], "")).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("EXCHANGE_RATE_IN", getDefault(ri["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("RATE_PERBASIS_IN", getDefault(ri["RATEPERBASIS"], "")).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("QUANTITY_IN", getDefault(ri["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("SERVICE_MST_FK_IN", ri["SERVICE_MST_PK"]).Direction = ParameterDirection.Input;
                    _with77.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;

                    try
                    {
                        _with77.ExecuteNonQuery();
                        if (Frt_Pk == 0)
                        {
                            _with77.Parameters.Clear();
                            _with77.CommandType = CommandType.StoredProcedure;
                            _with77.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_FRT_SEQ_CURRVAL";
                            _with77.Parameters.Add("BIZ_IN", 1).Direction = ParameterDirection.Input;
                            _with77.Parameters.Add("PROCESS_IN", 1).Direction = ParameterDirection.Input;
                            _with77.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with77.ExecuteNonQuery();
                            Frt_Pk = Convert.ToInt32(_with77.Parameters["RETURN_VALUE"].Value);
                            ri["JOB_TRN_AIR_EXP_FD_PK"] = Frt_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            //----------------------------------Expense Charge Details----------------------------------
            if ((dsExpenseChargeDetails != null))
            {
                foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                {
                    int Cost_Pk = 0;
                    try
                    {
                        Cost_Pk = Convert.ToInt32(re["JOB_TRN_AIR_EXP_COST_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Cost_Pk = 0;
                    }
                    var _with78 = objWK.MyCommand;
                    _with78.Parameters.Clear();
                    _with78.Transaction = TRAN;
                    _with78.CommandType = CommandType.StoredProcedure;
                    if (Cost_Pk > 0)
                    {
                        _with78.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_COST_UPD";
                        _with78.Parameters.Add("JOB_TRN_AIR_EST_PK_IN", re["JOB_TRN_AIR_EXP_COST_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with78.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_EXP_TBL_PKG.JOB_TRN_AIR_EXP_COST_INS";
                        _with78.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }

                    _with78.Parameters.Add("JOB_CARD_AIR_EXP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("VENDOR_MST_FK_IN", re["SUPPLIER_MST_PK"]).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("COST_ELEMENT_FK_IN", re["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("LOCATION_FK_IN", re["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("VENDOR_KEY_IN", re["SUPPLIER_MST_ID"]).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("PTMT_TYPE_IN", re["PTMT_TYPE"]).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("CURRENCY_MST_FK_IN", re["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("ESTIMATED_COST_IN", re["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("TOTAL_COST_IN", re["TOTAL_COST"]).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("BASIS_FK_IN", re["DD_VALUE"]).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("RATEPERBASIS_IN", re["RATEPERBASIS"]).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("QUANTITY_IN", getDefault(re["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("EXCHANGE_RATE_IN", getDefault(re["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("EXT_INT_FLAG_IN", getDefault(re["EXT_INT_FLAG"], 2)).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("SERVICE_MST_FK_IN", re["SERVICE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with78.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with78.ExecuteNonQuery();
                        if (Cost_Pk == 0)
                        {
                            _with78.Parameters.Clear();
                            _with78.CommandType = CommandType.StoredProcedure;
                            _with78.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_COST_SEQ_CURRVAL";
                            _with78.Parameters.Add("BIZ_IN", 1).Direction = ParameterDirection.Input;
                            _with78.Parameters.Add("PROCESS_IN", 1).Direction = ParameterDirection.Input;
                            _with78.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with78.ExecuteNonQuery();
                            Cost_Pk = Convert.ToInt32(_with78.Parameters["RETURN_VALUE"].Value);
                            re["JOB_TRN_AIR_EXP_COST_PK"] = Cost_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            ClearAirExpRemovedServices(objWK, TRAN, JobCardPK, dsIncomeChargeDetails, dsExpenseChargeDetails);
            return true;
        }

        /// <summary>
        /// Clears the air exp removed services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool ClearAirExpRemovedServices(WorkFlow objWK, OracleTransaction TRAN, int JobCardPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            string SelectedFrtPks = "";
            string SelectedCostPks = "";
            if (JobCardPK > 0)
            {
                try
                {
                    foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedFrtPks))
                        {
                            SelectedFrtPks = Convert.ToString(ri["JOB_TRN_AIR_EXP_FD_PK"]);
                        }
                        else
                        {
                            SelectedFrtPks += "," + ri["JOB_TRN_AIR_EXP_FD_PK"];
                        }
                    }
                    foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedCostPks))
                        {
                            SelectedCostPks = Convert.ToString(re["JOB_TRN_AIR_EXP_COST_PK"]);
                        }
                        else
                        {
                            SelectedCostPks += "," + re["JOB_TRN_AIR_EXP_COST_PK"];
                        }
                    }

                    var _with79 = objWK.MyCommand;
                    _with79.Transaction = TRAN;
                    _with79.CommandType = CommandType.StoredProcedure;
                    _with79.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.DELETE_AIR_EXP_SEC_CHG_EXCEPT";
                    _with79.Parameters.Clear();
                    _with79.Parameters.Add("JOB_CARD_AIR_EXP_PK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with79.Parameters.Add("JOB_TRN_AIR_EXP_FD_PKS", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
                    _with79.Parameters.Add("JOB_TRN_AIR_EXP_COST_PKS", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
                    _with79.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    _with79.ExecuteNonQuery();
                }
                catch (OracleException oraexp)
                {
                    TRAN.Rollback();
                }
                catch (Exception ex)
                {
                    TRAN.Rollback();
                }
                finally
                {
                }
            }
            return false;
        }

        #endregion "Save Air Exp Function"

        #region "Save Air Imp Function"

        /// <summary>
        /// Saves the air imp.
        /// </summary>
        /// <param name="dsFreightDetails">The ds freight details.</param>
        /// <param name="dsCostDetails">The ds cost details.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsOtherCharges">The ds other charges.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public ArrayList SaveAirImp(DataSet dsFreightDetails, DataSet dsCostDetails, long JobCardPK, DataSet dsOtherCharges, DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;

            Int32 RecAfct = default(Int32);
            OracleCommand insFreightDetails = new OracleCommand();
            OracleCommand updFreightDetails = new OracleCommand();
            OracleCommand delFreightDetails = new OracleCommand();

            OracleCommand insDropDetails = new OracleCommand();
            OracleCommand updDropDetails = new OracleCommand();
            OracleCommand insCostDetails = new OracleCommand();
            OracleCommand updCostDetails = new OracleCommand();
            OracleCommand delCostDetails = new OracleCommand();

            OracleCommand insOtherChargesDetails = new OracleCommand();
            OracleCommand updOtherChargesDetails = new OracleCommand();
            OracleCommand delOtherChargesDetails = new OracleCommand();

            try
            {
                if ((dsFreightDetails != null))
                {
                    var _with80 = insFreightDetails;
                    _with80.Connection = objWK.MyConnection;
                    _with80.CommandType = CommandType.StoredProcedure;
                    _with80.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_FD_INS";
                    var _with81 = _with80.Parameters;

                    insFreightDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    insFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "Frtpayer_mst_fk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                    insFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    insFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_IMP_FD_PK").Direction = ParameterDirection.Output;
                    insFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with82 = updFreightDetails;
                    _with82.Connection = objWK.MyConnection;
                    _with82.CommandType = CommandType.StoredProcedure;
                    _with82.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_FD_UPD";
                    var _with83 = _with82.Parameters;

                    updFreightDetails.Parameters.Add("JOB_TRN_AIR_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_fd_pk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["JOB_TRN_AIR_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    updFreightDetails.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("FREIGHT_TYPE_IN", OracleDbType.Int32, 1, "freight_type").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                    //latha
                    updFreightDetails.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "Frtpayer_mst_fk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("FREIGHT_AMT_IN", OracleDbType.Int32, 10, "freight_amt").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["FREIGHT_AMT_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_fk").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("BASIS_IN", OracleDbType.Int32, 10, "basis").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("QUANTITY_IN", OracleDbType.Int32, 10, "quantity").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "roe").Direction = ParameterDirection.Input;
                    updFreightDetails.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    updFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    updFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with84 = delFreightDetails;
                    _with84.Connection = objWK.MyConnection;
                    _with84.CommandType = CommandType.StoredProcedure;
                    _with84.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_FD_DEL";

                    delFreightDetails.Parameters.Add("JOB_TRN_AIR_IMP_FD_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_fd_pk").Direction = ParameterDirection.Input;
                    delFreightDetails.Parameters["JOB_TRN_AIR_IMP_FD_PK_IN"].SourceVersion = DataRowVersion.Current;

                    delFreightDetails.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    delFreightDetails.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with85 = objWK.MyDataAdapter;

                    _with85.InsertCommand = insFreightDetails;
                    _with85.InsertCommand.Transaction = TRAN;

                    _with85.UpdateCommand = updFreightDetails;
                    _with85.UpdateCommand.Transaction = TRAN;

                    _with85.DeleteCommand = delFreightDetails;
                    _with85.DeleteCommand.Transaction = TRAN;

                    RecAfct = _with85.Update(dsFreightDetails);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }

                if ((dsIncomeChargeDetails != null) & (dsExpenseChargeDetails != null))
                {
                    if (!SaveAirImpSecondaryServices(objWK, TRAN, Convert.ToInt32(JobCardPK), dsIncomeChargeDetails, dsExpenseChargeDetails))
                    {
                        arrMessage.Add("Error while saving secondary service details");
                        return arrMessage;
                    }
                }

                if ((dsCostDetails != null))
                {
                    var _with86 = insCostDetails;
                    _with86.Connection = objWK.MyConnection;
                    _with86.CommandType = CommandType.StoredProcedure;
                    _with86.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_COST_INS";
                    insCostDetails.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with86.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                    _with86.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with86.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                    _with86.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with86.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                    _with86.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with86.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                    _with86.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                    _with86.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                    _with86.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with86.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                    _with86.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with86.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                    _with86.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                    _with86.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                    _with86.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                    _with86.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "JOB_TRN_AIR_IMP_COST_PK").Direction = ParameterDirection.Output;
                    _with86.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with87 = updCostDetails;
                    _with87.Connection = objWK.MyConnection;
                    _with87.CommandType = CommandType.StoredProcedure;
                    _with87.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_COST_UPD";

                    _with87.Parameters.Add("JOB_TRN_AIR_IMP_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_AIR_IMP_COST_PK").Direction = ParameterDirection.Input;
                    _with87.Parameters["JOB_TRN_AIR_IMP_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with87.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with87.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_PK").Direction = ParameterDirection.Input;
                    _with87.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with87.Parameters.Add("COST_ELEMENT_FK_IN", OracleDbType.Int32, 50, "COST_ELEMENT_MST_PK").Direction = ParameterDirection.Input;
                    _with87.Parameters["COST_ELEMENT_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with87.Parameters.Add("LOCATION_FK_IN", OracleDbType.Int32, 10, "LOCATION_MST_FK").Direction = ParameterDirection.Input;
                    _with87.Parameters["LOCATION_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with87.Parameters.Add("VENDOR_KEY_IN", OracleDbType.Varchar2, 50, "VENDOR_KEY").Direction = ParameterDirection.Input;
                    _with87.Parameters["VENDOR_KEY_IN"].SourceVersion = DataRowVersion.Current;

                    _with87.Parameters.Add("PTMT_TYPE_IN", OracleDbType.Int32, 1, "PTMT_TYPE").Direction = ParameterDirection.Input;
                    _with87.Parameters["PTMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with87.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                    _with87.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with87.Parameters.Add("ESTIMATED_COST_IN", OracleDbType.Int32, 20, "ESTIMATED_COST").Direction = ParameterDirection.Input;
                    _with87.Parameters["ESTIMATED_COST_IN"].SourceVersion = DataRowVersion.Current;

                    _with87.Parameters.Add("TOTAL_COST_IN", OracleDbType.Int32, 20, "TOTAL_COST").Direction = ParameterDirection.Input;
                    _with87.Parameters["TOTAL_COST_IN"].SourceVersion = DataRowVersion.Current;

                    _with87.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with87.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with88 = delCostDetails;
                    _with88.Connection = objWK.MyConnection;
                    _with88.CommandType = CommandType.StoredProcedure;
                    _with88.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_COST_DEL";

                    _with88.Parameters.Add("JOB_TRN_AIR_IMP_EST_PK_IN", OracleDbType.Int32, 10, "JOB_TRN_AIR_IMP_COST_PK").Direction = ParameterDirection.Input;
                    _with88.Parameters["JOB_TRN_AIR_IMP_EST_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with88.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "Clng(RETURN_VALUE)").Direction = ParameterDirection.Output;
                    _with88.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with89 = objWK.MyDataAdapter;

                    _with89.InsertCommand = insCostDetails;
                    _with89.InsertCommand.Transaction = TRAN;

                    _with89.UpdateCommand = updCostDetails;
                    _with89.UpdateCommand.Transaction = TRAN;

                    _with89.DeleteCommand = delCostDetails;
                    _with89.DeleteCommand.Transaction = TRAN;

                    RecAfct = _with89.Update(dsCostDetails.Tables[0]);

                    if (arrMessage.Count > 0)
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }

                if ((dsOtherCharges != null))
                {
                    var _with90 = insOtherChargesDetails;
                    _with90.Connection = objWK.MyConnection;
                    _with90.CommandType = CommandType.StoredProcedure;
                    _with90.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_OTH_CHRG_INS";

                    _with90.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with90.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    _with90.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with90.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                    _with90.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    //latha

                    _with90.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "PaymentType").Direction = ParameterDirection.Input;
                    _with90.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with90.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                    _with90.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with90.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "Frtpayer_mst_fk").Direction = ParameterDirection.Input;
                    _with90.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    //end
                    _with90.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                    _with90.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                    _with90.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                    _with90.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with90.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "job_trn_air_IMP_oth_pk").Direction = ParameterDirection.Output;
                    _with90.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with91 = updOtherChargesDetails;
                    _with91.Connection = objWK.MyConnection;
                    _with91.CommandType = CommandType.StoredProcedure;
                    _with91.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_OTH_CHRG_UPD";

                    _with91.Parameters.Add("JOB_TRN_AIR_IMP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_oth_pk").Direction = ParameterDirection.Input;
                    _with91.Parameters["JOB_TRN_AIR_IMP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with91.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;

                    _with91.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "freight_element_mst_pk").Direction = ParameterDirection.Input;
                    _with91.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with91.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "currency_mst_pk").Direction = ParameterDirection.Input;
                    _with91.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with91.Parameters.Add("PAYMENT_TYPE_IN", OracleDbType.Int32, 1, "PaymentType").Direction = ParameterDirection.Input;
                    _with91.Parameters["PAYMENT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                    _with91.Parameters.Add("LOCATION_MST_FK_IN", OracleDbType.Int32, 10, "location_fk").Direction = ParameterDirection.Input;
                    _with91.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with91.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", OracleDbType.Int32, 10, "frtpayer_mst_fk").Direction = ParameterDirection.Input;
                    _with91.Parameters["FRTPAYER_CUST_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with91.Parameters.Add("AMOUNT_IN", OracleDbType.Int32, 10, "amount").Direction = ParameterDirection.Input;
                    _with91.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;

                    _with91.Parameters.Add("EXCHANGE_RATE_IN", OracleDbType.Int32, 10, "ROE").Direction = ParameterDirection.Input;
                    _with91.Parameters["EXCHANGE_RATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with91.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with91.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with92 = delOtherChargesDetails;
                    _with92.Connection = objWK.MyConnection;
                    _with92.CommandType = CommandType.StoredProcedure;
                    _with92.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_OTH_CHRG_DEL";

                    _with92.Parameters.Add("JOB_TRN_AIR_IMP_OTH_PK_IN", OracleDbType.Int32, 10, "job_trn_air_IMP_oth_pk").Direction = ParameterDirection.Input;
                    _with92.Parameters["JOB_TRN_AIR_IMP_OTH_PK_IN"].SourceVersion = DataRowVersion.Current;

                    _with92.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with92.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                    var _with93 = objWK.MyDataAdapter;

                    _with93.InsertCommand = insOtherChargesDetails;
                    _with93.InsertCommand.Transaction = TRAN;

                    _with93.UpdateCommand = updOtherChargesDetails;
                    _with93.UpdateCommand.Transaction = TRAN;

                    _with93.DeleteCommand = delOtherChargesDetails;
                    _with93.DeleteCommand.Transaction = TRAN;

                    RecAfct = _with93.Update(dsOtherCharges);
                }
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
                throw oraexp;
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

        /// <summary>
        /// Saves the air imp secondary services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool SaveAirImpSecondaryServices(WorkFlow objWK, OracleTransaction TRAN, int JobCardPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            if ((dsIncomeChargeDetails != null))
            {
                foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                {
                    int Frt_Pk = 0;
                    try
                    {
                        Frt_Pk = Convert.ToInt32(ri["JOB_TRN_AIR_IMP_FD_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Frt_Pk = 0;
                    }
                    var _with94 = objWK.MyCommand;
                    _with94.Parameters.Clear();
                    _with94.Transaction = TRAN;
                    _with94.CommandType = CommandType.StoredProcedure;
                    if (Frt_Pk > 0)
                    {
                        _with94.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_FD_UPD";
                        _with94.Parameters.Add("JOB_TRN_AIR_IMP_FD_PK_IN", ri["JOB_TRN_AIR_IMP_FD_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with94.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_FD_INS";
                        _with94.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }
                    _with94.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with94.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
                    _with94.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
                    _with94.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with94.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                    _with94.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
                    _with94.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with94.Parameters.Add("BASIS_IN", getDefault(ri["BASIS"], "")).Direction = ParameterDirection.Input;
                    _with94.Parameters.Add("BASIS_FK_IN", getDefault(ri["BASIS_PK"], "")).Direction = ParameterDirection.Input;
                    _with94.Parameters.Add("EXCHANGE_RATE_IN", getDefault(ri["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with94.Parameters.Add("RATE_PERBASIS_IN", getDefault(ri["RATEPERBASIS"], "")).Direction = ParameterDirection.Input;
                    _with94.Parameters.Add("QUANTITY_IN", getDefault(ri["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with94.Parameters.Add("SERVICE_MST_FK_IN", ri["SERVICE_MST_PK"]).Direction = ParameterDirection.Input;
                    _with94.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with94.ExecuteNonQuery();
                        if (Frt_Pk == 0)
                        {
                            _with94.Parameters.Clear();
                            _with94.CommandType = CommandType.StoredProcedure;
                            _with94.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_FRT_SEQ_CURRVAL";
                            _with94.Parameters.Add("BIZ_IN", 1).Direction = ParameterDirection.Input;
                            _with94.Parameters.Add("PROCESS_IN", 2).Direction = ParameterDirection.Input;
                            _with94.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with94.ExecuteNonQuery();
                            Frt_Pk = Convert.ToInt32(_with94.Parameters["RETURN_VALUE"].Value);
                            ri["JOB_TRN_AIR_IMP_FD_PK"] = Frt_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            //----------------------------------Expense Charge Details----------------------------------
            if ((dsExpenseChargeDetails != null))
            {
                foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                {
                    int Cost_Pk = 0;
                    try
                    {
                        Cost_Pk = Convert.ToInt32(re["JOB_TRN_AIR_IMP_COST_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Cost_Pk = 0;
                    }
                    var _with95 = objWK.MyCommand;
                    _with95.Parameters.Clear();
                    _with95.Transaction = TRAN;
                    _with95.CommandType = CommandType.StoredProcedure;
                    if (Cost_Pk > 0)
                    {
                        _with95.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_COST_UPD";
                        _with95.Parameters.Add("JOB_TRN_AIR_IMP_EST_PK_IN", re["JOB_TRN_AIR_IMP_COST_PK"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with95.CommandText = objWK.MyUserName + ".JOB_CARD_AIR_IMP_TBL_PKG.JOB_TRN_AIR_IMP_COST_INS";
                        _with95.Parameters.Add("SERVICE_TYPE_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }
                    _with95.Parameters.Add("JOB_CARD_AIR_IMP_FK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("VENDOR_MST_FK_IN", re["SUPPLIER_MST_PK"]).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("COST_ELEMENT_FK_IN", re["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("LOCATION_FK_IN", re["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("VENDOR_KEY_IN", re["SUPPLIER_MST_ID"]).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("PTMT_TYPE_IN", re["PTMT_TYPE"]).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("CURRENCY_MST_FK_IN", re["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("ESTIMATED_COST_IN", re["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("TOTAL_COST_IN", re["TOTAL_COST"]).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("BASIS_FK_IN", re["DD_VALUE"]).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("RATEPERBASIS_IN", re["RATEPERBASIS"]).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("QUANTITY_IN", getDefault(re["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("EXCHANGE_RATE_IN", getDefault(re["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("EXT_INT_FLAG_IN", getDefault(re["EXT_INT_FLAG"], 2)).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("SERVICE_MST_FK_IN", re["SERVICE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with95.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;

                    try
                    {
                        _with95.ExecuteNonQuery();
                        if (Cost_Pk == 0)
                        {
                            _with95.Parameters.Clear();
                            _with95.CommandType = CommandType.StoredProcedure;
                            _with95.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.FETCH_COST_SEQ_CURRVAL";
                            _with95.Parameters.Add("BIZ_IN", 1).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("PROCESS_IN", 2).Direction = ParameterDirection.Input;
                            _with95.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                            _with95.ExecuteNonQuery();
                            Cost_Pk = Convert.ToInt32(_with95.Parameters["RETURN_VALUE"].Value);
                            re["JOB_TRN_AIR_IMP_COST_PK"] = Cost_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            ClearAirImpRemovedServices(objWK, TRAN, JobCardPK, dsIncomeChargeDetails, dsExpenseChargeDetails);
            return true;
        }

        /// <summary>
        /// Clears the air imp removed services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="JobCardPK">The job card pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool ClearAirImpRemovedServices(WorkFlow objWK, OracleTransaction TRAN, int JobCardPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            string SelectedFrtPks = "";
            string SelectedCostPks = "";
            if (JobCardPK > 0)
            {
                try
                {
                    foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedFrtPks))
                        {
                            SelectedFrtPks = Convert.ToString(ri["JOB_TRN_AIR_IMP_FD_PK"]);
                        }
                        else
                        {
                            SelectedFrtPks += "," + ri["JOB_TRN_AIR_IMP_FD_PK"];
                        }
                    }
                    foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedCostPks))
                        {
                            SelectedCostPks = Convert.ToString(re["JOB_TRN_AIR_IMP_COST_PK"]);
                        }
                        else
                        {
                            SelectedCostPks += "," + re["JOB_TRN_AIR_IMP_COST_PK"];
                        }
                    }

                    var _with96 = objWK.MyCommand;
                    _with96.Transaction = TRAN;
                    _with96.CommandType = CommandType.StoredProcedure;
                    _with96.CommandText = objWK.MyUserName + ".JOBCARD_SEC_SERVICE_PKG.DELETE_AIR_IMP_SEC_CHG_EXCEPT";
                    _with96.Parameters.Clear();
                    _with96.Parameters.Add("JOB_CARD_AIR_IMP_PK_IN", JobCardPK).Direction = ParameterDirection.Input;
                    _with96.Parameters.Add("JOB_TRN_AIR_IMP_FD_PKS", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
                    _with96.Parameters.Add("JOB_TRN_AIR_IMP_COST_PKS", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
                    _with96.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    _with96.ExecuteNonQuery();
                }
                catch (OracleException oraexp)
                {
                    TRAN.Rollback();
                }
                catch (Exception ex)
                {
                    TRAN.Rollback();
                }
                finally
                {
                }
            }
            return false;
        }

        #endregion "Save Air Imp Function"

        #region "Save CBJC Function"

        /// <summary>
        /// Saves the CBJC.
        /// </summary>
        /// <param name="CBJCPK">The CBJCPK.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public ArrayList SaveCBJC(long CBJCPK, DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);

            try
            {
                if (!SaveCBJCSecondaryServices(objWK, TRAN, Convert.ToInt32(CBJCPK), dsIncomeChargeDetails, dsExpenseChargeDetails))
                {
                    arrMessage.Add("Error while saving secondary service details");
                    return arrMessage;
                }
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

        #region "Secondary Services"

        /// <summary>
        /// Saves the CBJC secondary services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="CBJCPK">The CBJCPK.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool SaveCBJCSecondaryServices(WorkFlow objWK, OracleTransaction TRAN, int CBJCPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            if ((dsIncomeChargeDetails != null))
            {
                //----------------------------------Income Charge Details----------------------------------
                foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                {
                    int Frt_Pk = 0;
                    try
                    {
                        Frt_Pk = Convert.ToInt32(ri["CBJC_TRN_FD_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Frt_Pk = 0;
                    }
                    var _with97 = objWK.MyCommand;
                    _with97.Parameters.Clear();
                    _with97.Transaction = TRAN;
                    _with97.CommandType = CommandType.StoredProcedure;
                    if (Frt_Pk > 0)
                    {
                        _with97.CommandText = objWK.MyUserName + ".CBJC_TRN_FD_PKG.CBJC_TRN_FD_UPD";
                        _with97.Parameters.Add("CBJC_TRN_FD_PK_IN", ri["CBJC_TRN_FD_PK"]).Direction = ParameterDirection.Input;
                        _with97.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                        _with97.Parameters.Add("VERSION_NO_IN", ri["VERSION_NO"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with97.CommandText = objWK.MyUserName + ".CBJC_TRN_FD_PKG.CBJC_TRN_FD_INS";
                        _with97.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                    }
                    _with97.Parameters.Add("CBJC_FK_IN", CBJCPK).Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("BASIS_FK_IN", getDefault(ri["BASIS_PK"], "")).Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("EXCHANGE_RATE_IN", getDefault(ri["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("RATEPERBASIS_IN", getDefault(ri["RATEPERBASIS"], "")).Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("QUANTITY_IN", getDefault(ri["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("SERVICE_MST_FK_IN", ri["SERVICE_MST_PK"]).Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("INV_AGENT_TRN_FK_IN", "").Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("CONSOL_INVOICE_TRN_FK_IN", "").Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                    _with97.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;

                    try
                    {
                        _with97.ExecuteNonQuery();
                        if (Frt_Pk == 0)
                        {
                            Frt_Pk = Convert.ToInt32(_with97.Parameters["RETURN_VALUE"].Value);
                            ri["CBJC_TRN_FD_PK"] = Frt_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            //----------------------------------Expense Charge Details----------------------------------
            if ((dsExpenseChargeDetails != null))
            {
                foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                {
                    int Cost_Pk = 0;
                    try
                    {
                        Cost_Pk = Convert.ToInt32(re["CBJC_TRN_COST_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Cost_Pk = 0;
                    }
                    var _with98 = objWK.MyCommand;
                    _with98.Parameters.Clear();
                    _with98.Transaction = TRAN;
                    _with98.CommandType = CommandType.StoredProcedure;
                    if (Cost_Pk > 0)
                    {
                        _with98.CommandText = objWK.MyUserName + ".CBJC_TRN_COST_PKG.CBJC_TRN_COST_UPD";
                        _with98.Parameters.Add("CBJC_TRN_COST_PK_IN", re["CBJC_TRN_COST_PK"]).Direction = ParameterDirection.Input;
                        _with98.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                        _with98.Parameters.Add("VERSION_NO_IN", re["VERSION_NO"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with98.CommandText = objWK.MyUserName + ".CBJC_TRN_COST_PKG.CBJC_TRN_COST_INS";
                        _with98.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                    }

                    _with98.Parameters.Add("CBJC_FK_IN", CBJCPK).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("VENDOR_MST_FK_IN", re["SUPPLIER_MST_PK"]).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("COST_ELEMENT_MST_FK_IN", re["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("LOCATION_MST_FK_IN", re["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("FREIGHT_TYPE_IN", re["PTMT_TYPE"]).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("CURRENCY_MST_FK_IN", re["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("ESTIMATED_COST_IN", re["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("TOTAL_COST_IN", re["TOTAL_COST"]).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("BASIS_FK_IN", re["DD_VALUE"]).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("RATEPERBASIS_IN", re["RATEPERBASIS"]).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("QUANTITY_IN", getDefault(re["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("EXCHANGE_RATE_IN", getDefault(re["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("EXT_INT_FLAG_IN", getDefault(re["EXT_INT_FLAG"], 2)).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("SERVICE_MST_FK_IN", re["SERVICE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("INV_SUPPLIER_FK_IN", "").Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                    _with98.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with98.ExecuteNonQuery();
                        if (Cost_Pk == 0)
                        {
                            Cost_Pk = Convert.ToInt32(_with98.Parameters["RETURN_VALUE"].Value);
                            re["CBJC_TRN_COST_PK"] = Cost_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            CBJCClearRemovedServices(objWK, TRAN, CBJCPK, dsIncomeChargeDetails, dsExpenseChargeDetails);
            return true;
        }

        /// <summary>
        /// CBJCs the clear removed services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="CBJCPK">The CBJCPK.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool CBJCClearRemovedServices(WorkFlow objWK, OracleTransaction TRAN, int CBJCPK, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            string SelectedFrtPks = "";
            string SelectedCostPks = "";
            if (CBJCPK > 0)
            {
                try
                {
                    foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedFrtPks))
                        {
                            SelectedFrtPks = Convert.ToString(getDefault(ri["CBJC_TRN_FD_PK"], 0));
                        }
                        else
                        {
                            SelectedFrtPks += "," + getDefault(ri["CBJC_TRN_FD_PK"], 0);
                        }
                    }
                    foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedCostPks))
                        {
                            SelectedCostPks = Convert.ToString(getDefault(re["CBJC_TRN_COST_PK"], 0));
                        }
                        else
                        {
                            SelectedCostPks += "," + getDefault(re["CBJC_TRN_COST_PK"], 0);
                        }
                    }

                    var _with99 = objWK.MyCommand;
                    _with99.Transaction = TRAN;
                    _with99.CommandType = CommandType.StoredProcedure;
                    _with99.CommandText = objWK.MyUserName + ".CBJC_TRN_FD_PKG.DELETE_SEC_CHG_EXCEPT";
                    _with99.Parameters.Clear();
                    _with99.Parameters.Add("CBJC_PK_IN", CBJCPK).Direction = ParameterDirection.Input;
                    _with99.Parameters.Add("CBJC_TRN_FD_FKS_IN", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
                    _with99.Parameters.Add("CBJC_TRN_COST_FKS_IN", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
                    _with99.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    _with99.ExecuteNonQuery();
                }
                catch (OracleException oraexp)
                {
                    TRAN.Rollback();
                }
                catch (Exception ex)
                {
                    TRAN.Rollback();
                }
                finally
                {
                }
            }
            return false;
        }

        #endregion "Secondary Services"

        #endregion "Save CBJC Function"

        #region "SAVE"

        /// <summary>
        /// Saves the tp note.
        /// </summary>
        /// <param name="TPNotePK">The tp note pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public ArrayList SaveTpNote(long TPNotePK, DataSet dsIncomeChargeDetails = null, DataSet dsExpenseChargeDetails = null)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            Int32 RecAfct = default(Int32);

            try
            {
                if (!SaveTpNoteSecondaryServices(objWK, TRAN, Convert.ToInt32(TPNotePK), dsIncomeChargeDetails, dsExpenseChargeDetails))
                {
                    arrMessage.Add("Error while saving secondary service details");
                    return arrMessage;
                }
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

        #region "Secondary Services"

        /// <summary>
        /// Saves the tp note secondary services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="TpNote_Pk">The tp note_ pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool SaveTpNoteSecondaryServices(WorkFlow objWK, OracleTransaction TRAN, int TpNote_Pk, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            if ((dsIncomeChargeDetails != null))
            {
                //----------------------------------Income Charge Details----------------------------------
                foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                {
                    int Frt_Pk = 0;
                    try
                    {
                        Frt_Pk = Convert.ToInt32(ri["TRANSPORT_TRN_FD_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Frt_Pk = 0;
                    }
                    var _with100 = objWK.MyCommand;
                    _with100.Parameters.Clear();
                    _with100.Transaction = TRAN;
                    _with100.CommandType = CommandType.StoredProcedure;
                    if (Frt_Pk > 0)
                    {
                        _with100.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_FD_PKG.TRANSPORT_TRN_FD_UPD";
                        _with100.Parameters.Add("TRANSPORT_TRN_FD_PK_IN", ri["TRANSPORT_TRN_FD_PK"]).Direction = ParameterDirection.Input;
                        _with100.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                        _with100.Parameters.Add("VERSION_NO_IN", ri["VERSION_NO"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with100.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_FD_PKG.TRANSPORT_TRN_FD_INS";
                        _with100.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                    }
                    _with100.Parameters.Add("TRANSPORT_INST_FK_IN", TpNote_Pk).Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", ri["CHARGE_PK"]).Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("FREIGHT_TYPE_IN", ri["FREIGHT_TYPE"]).Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("LOCATION_MST_FK_IN", ri["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("FRTPAYER_CUST_MST_FK_IN", ri["FRTPAYER_CUST_MST_FK"]).Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("FREIGHT_AMT_IN", ri["FREIGHT_AMT"]).Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("CURRENCY_MST_FK_IN", ri["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("BASIS_FK_IN", getDefault(ri["BASIS_PK"], "")).Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("EXCHANGE_RATE_IN", getDefault(ri["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("RATEPERBASIS_IN", getDefault(ri["RATEPERBASIS"], "")).Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("QUANTITY_IN", getDefault(ri["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("SERVICE_MST_FK_IN", ri["SERVICE_MST_PK"]).Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("INV_AGENT_TRN_FK_IN", "").Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("CONSOL_INVOICE_TRN_FK_IN", "").Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                    _with100.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;

                    try
                    {
                        _with100.ExecuteNonQuery();
                        if (Frt_Pk == 0)
                        {
                            Frt_Pk = Convert.ToInt32(_with100.Parameters["RETURN_VALUE"].Value);
                            ri["TRANSPORT_TRN_FD_PK"] = Frt_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            //----------------------------------Expense Charge Details----------------------------------
            if ((dsExpenseChargeDetails != null))
            {
                foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                {
                    int Cost_Pk = 0;
                    try
                    {
                        Cost_Pk = Convert.ToInt32(re["TRANSPORT_TRN_COST_PK"]);
                    }
                    catch (Exception ex)
                    {
                        Cost_Pk = 0;
                    }
                    var _with101 = objWK.MyCommand;
                    _with101.Parameters.Clear();
                    _with101.Transaction = TRAN;
                    _with101.CommandType = CommandType.StoredProcedure;
                    if (Cost_Pk > 0)
                    {
                        _with101.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_COST_PKG.TRANSPORT_TRN_COST_UPD";
                        _with101.Parameters.Add("TRANSPORT_TRN_COST_PK_IN", re["TRANSPORT_TRN_COST_PK"]).Direction = ParameterDirection.Input;
                        _with101.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                        _with101.Parameters.Add("VERSION_NO_IN", re["VERSION_NO"]).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with101.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_COST_PKG.TRANSPORT_TRN_COST_INS";
                        _with101.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(M_CREATED_BY_FK)).Direction = ParameterDirection.Input;
                    }

                    _with101.Parameters.Add("TRANSPORT_INST_FK_IN", TpNote_Pk).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("VENDOR_MST_FK_IN", re["SUPPLIER_MST_PK"]).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("COST_ELEMENT_MST_FK_IN", re["COST_ELEMENT_MST_PK"]).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("LOCATION_MST_FK_IN", re["LOCATION_MST_FK"]).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("FREIGHT_TYPE_IN", re["PTMT_TYPE"]).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("CURRENCY_MST_FK_IN", re["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("ESTIMATED_COST_IN", re["ESTIMATED_COST"]).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("TOTAL_COST_IN", re["TOTAL_COST"]).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("BASIS_FK_IN", re["DD_VALUE"]).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("RATEPERBASIS_IN", re["RATEPERBASIS"]).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("QUANTITY_IN", getDefault(re["VOLUME"], "")).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("EXCHANGE_RATE_IN", getDefault(re["ROE"], 1)).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("EXT_INT_FLAG_IN", getDefault(re["EXT_INT_FLAG"], 2)).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("SERVICE_MST_FK_IN", re["SERVICE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("INV_SUPPLIER_FK_IN", "").Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("CONFIG_MST_FK_IN", Convert.ToInt64(M_Configuration_PK)).Direction = ParameterDirection.Input;
                    _with101.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50).Direction = ParameterDirection.Output;
                    try
                    {
                        _with101.ExecuteNonQuery();
                        if (Cost_Pk == 0)
                        {
                            Cost_Pk = Convert.ToInt32(_with101.Parameters["RETURN_VALUE"].Value);
                            re["TRANSPORT_TRN_COST_PK"] = Cost_Pk;
                        }
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }
            }
            TpNoteClearRemovedServices(objWK, TRAN, TpNote_Pk, dsIncomeChargeDetails, dsExpenseChargeDetails);
            return true;
        }

        /// <summary>
        /// Tps the note clear removed services.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="TpNote_Pk">The tp note_ pk.</param>
        /// <param name="dsIncomeChargeDetails">The ds income charge details.</param>
        /// <param name="dsExpenseChargeDetails">The ds expense charge details.</param>
        /// <returns></returns>
        public bool TpNoteClearRemovedServices(WorkFlow objWK, OracleTransaction TRAN, int TpNote_Pk, DataSet dsIncomeChargeDetails, DataSet dsExpenseChargeDetails)
        {
            string SelectedFrtPks = "";
            string SelectedCostPks = "";
            if (TpNote_Pk > 0)
            {
                try
                {
                    foreach (DataRow ri in dsIncomeChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedFrtPks))
                        {
                            SelectedFrtPks = Convert.ToString(getDefault(ri["TRANSPORT_TRN_FD_PK"], 0));
                        }
                        else
                        {
                            SelectedFrtPks += "," + getDefault(ri["TRANSPORT_TRN_FD_PK"], 0);
                        }
                    }
                    foreach (DataRow re in dsExpenseChargeDetails.Tables[1].Rows)
                    {
                        if (string.IsNullOrEmpty(SelectedCostPks))
                        {
                            SelectedCostPks = Convert.ToString(getDefault(re["TRANSPORT_TRN_COST_PK"], 0));
                        }
                        else
                        {
                            SelectedCostPks += "," + getDefault(re["TRANSPORT_TRN_COST_PK"], 0);
                        }
                    }

                    var _with102 = objWK.MyCommand;
                    _with102.Transaction = TRAN;
                    _with102.CommandType = CommandType.StoredProcedure;
                    _with102.CommandText = objWK.MyUserName + ".TRANSPORT_TRN_FD_PKG.DELETE_SEC_CHG_EXCEPT";
                    _with102.Parameters.Clear();
                    _with102.Parameters.Add("TRANSPORT_INST_SEA_PK_IN", TpNote_Pk).Direction = ParameterDirection.Input;
                    _with102.Parameters.Add("TRANSPORT_TRN_FD_FKS_IN", (string.IsNullOrEmpty(SelectedFrtPks) ? "" : SelectedFrtPks)).Direction = ParameterDirection.Input;
                    _with102.Parameters.Add("TRANSPORT_TRN_COST_FKS_IN", (string.IsNullOrEmpty(SelectedCostPks) ? "" : SelectedCostPks)).Direction = ParameterDirection.Input;
                    _with102.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                    _with102.ExecuteNonQuery();
                }
                catch (OracleException oraexp)
                {
                    TRAN.Rollback();
                }
                catch (Exception ex)
                {
                    TRAN.Rollback();
                }
                finally
                {
                }
            }
            return false;
        }

        #endregion "Secondary Services"

        #endregion "SAVE"

        #region " FetchPLQR "

        /// <summary>
        /// Fetches the shipment header.
        /// </summary>
        /// <param name="JobCardFK">The job card fk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <param name="FormFlg">The form FLG.</param>
        /// <returns></returns>
        public DataSet FetchShipmentHeader(long JobCardFK, Int32 BizType, Int32 Process, Int32 CargoType, Int32 JobType, string FormFlg)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with103 = objWF.MyDataAdapter;
                _with103.SelectCommand = new OracleCommand();
                _with103.SelectCommand.Connection = objWF.MyConnection;
                _with103.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_PROFIT_AND_LOSS_PKG.FECTH_SHIPMENT_HEADER";
                _with103.SelectCommand.CommandType = CommandType.StoredProcedure;

                _with103.SelectCommand.Parameters.Add("JOBCARDPK_IN", OracleDbType.Int32).Value = (JobCardFK <= 0 ? 0 : JobCardFK);
                _with103.SelectCommand.Parameters.Add("BIZTYPE_IN", OracleDbType.Int32).Value = BizType;
                _with103.SelectCommand.Parameters.Add("PROCESS_IN", OracleDbType.Int32).Value = Process;
                _with103.SelectCommand.Parameters.Add("CARGOTYPE_IN", OracleDbType.Int32).Value = CargoType;
                _with103.SelectCommand.Parameters.Add("JOBTYPE_IN", OracleDbType.Int32).Value = JobType;
                _with103.SelectCommand.Parameters.Add("FORM_FLG_IN", FormFlg).Direction = ParameterDirection.Input;
                _with103.SelectCommand.Parameters.Add("GET_DS", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                _with103.Fill(ds);

                return ds;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }

        #endregion " FetchPLQR "

        #region "Fetch Agent"

        /// <summary>
        /// Fetches the pol location agent.
        /// </summary>
        /// <param name="LocPK">The loc pk.</param>
        /// <returns></returns>
        public DataSet FetchPOLLocationAgent(Int64 LocPK)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                sb.Append(" SELECT AMT.AGENT_MST_PK POL_AGENT_FK, AMT.AGENT_ID POL_AGENT_ID, AMT.AGENT_NAME POL_AGENT_NAME ");
                sb.Append(" FROM AGENT_MST_TBL AMT WHERE AMT.LOCATION_MST_FK = " + LocPK + " AND ROWNUM = 1 ");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 23/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Agent"
    }
}